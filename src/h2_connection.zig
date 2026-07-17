const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;

const c = @import("root.zig").c;

const log = std.log.scoped(.h2);

pub const H2Connection = struct {
    stream: std.net.Stream,
    session: *c.nghttp2_session,
    settings_received: bool,
    settings_ack_received: bool,

    pub fn connect(gpa: Allocator, host: []const u8, port: u16) !H2Connection {
        const tcp_stream = try std.net.tcpConnectToHost(gpa, host, port);
        errdefer tcp_stream.close();

        var callbacks: ?*c.nghttp2_session_callbacks = null;
        if (c.nghttp2_session_callbacks_new(&callbacks) != 0)
            return error.Nghttp2CallbacksNewFailed;
        defer c.nghttp2_session_callbacks_del(callbacks);

        c.nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks, onFrameRecv);
        c.nghttp2_session_callbacks_set_on_header_callback(callbacks, onHeader);
        c.nghttp2_session_callbacks_set_on_data_chunk_recv_callback(callbacks, onDataChunkRecv);
        c.nghttp2_session_callbacks_set_on_stream_close_callback(callbacks, onStreamClose);

        var conn: H2Connection = .{
            .stream = tcp_stream,
            .session = undefined,
            .settings_received = false,
            .settings_ack_received = false,
        };

        var session: ?*c.nghttp2_session = null;
        if (c.nghttp2_session_client_new(&session, callbacks, null) != 0) {
            return error.Nghttp2SessionNewFailed;
        }
        conn.session = session.?;

        return conn;
    }

    pub fn performHandshake(self: *H2Connection) !void {
        c.nghttp2_session_set_user_data(self.session, @ptrCast(self));

        if (c.nghttp2_submit_settings(self.session, c.NGHTTP2_FLAG_NONE, null, 0) != 0) {
            return error.Nghttp2SubmitSettingsFailed;
        }

        try self.sendAll();
        log.debug("sent client connection preface", .{});

        var buf: [16384]u8 = undefined;
        while (!self.settings_received or !self.settings_ack_received) {
            const n = try self.stream.read(&buf);
            if (n == 0) return error.ConnectionClosed;

            const consumed = c.nghttp2_session_mem_recv2(self.session, &buf, n);
            if (consumed < 0) return error.Nghttp2MemRecvFailed;

            try self.sendAll();
        }

        log.debug("handshake complete", .{});
    }

    /// Read from socket and feed to nghttp2 until the given condition is met.
    pub fn recvUntil(
        self: *H2Connection,
        done: *const fn (?*anyopaque) bool,
        ctx: ?*anyopaque,
    ) !void {
        var buf: [16384]u8 = undefined;
        while (!done(ctx)) {
            const n = try self.stream.read(&buf);
            if (n == 0) return error.ConnectionClosed;

            const consumed = c.nghttp2_session_mem_recv2(self.session, &buf, n);
            if (consumed < 0) return error.Nghttp2MemRecvFailed;

            try self.sendAll();
        }
    }

    pub fn sendAll(self: *H2Connection) !void {
        while (true) {
            var data_ptr: ?[*]const u8 = null;
            const len = c.nghttp2_session_mem_send2(self.session, &data_ptr);
            if (len < 0) return error.Nghttp2MemSendFailed;
            if (len == 0) break;

            try self.stream.writeAll(data_ptr.?[0..@intCast(len)]);
        }
    }

    pub fn deinit(self: *H2Connection) void {
        c.nghttp2_session_del(self.session);
        self.stream.close();
        self.* = undefined;
    }

    // -- nghttp2 callbacks --

    fn onFrameRecv(
        _: ?*c.nghttp2_session,
        frame: ?*const c.nghttp2_frame,
        user_data: ?*anyopaque,
    ) callconv(.c) c_int {
        const self: *H2Connection = @ptrCast(@alignCast(user_data orelse return 0));
        const hd = (frame orelse return 0).hd;

        if (hd.type == @as(u8, @intCast(c.NGHTTP2_SETTINGS))) {
            if (hd.flags & @as(u8, @intCast(c.NGHTTP2_FLAG_ACK)) != 0) {
                log.debug("received SETTINGS ACK", .{});
                self.settings_ack_received = true;
            } else {
                log.debug("received SETTINGS", .{});
                self.settings_received = true;
            }
        }

        return 0;
    }

    fn onHeader(
        session: ?*c.nghttp2_session,
        frame: ?*const c.nghttp2_frame,
        name: ?[*]const u8,
        namelen: usize,
        value: ?[*]const u8,
        valuelen: usize,
        _: u8,
        _: ?*anyopaque,
    ) callconv(.c) c_int {
        const stream_id = (frame orelse return 0).hd.stream_id;
        const n = name orelse return 0;
        const v = value orelse return 0;
        const stream_ud = c.nghttp2_session_get_stream_user_data(session, stream_id);
        if (stream_ud) |ud| {
            const state: *StreamState = @ptrCast(@alignCast(ud));
            state.handleHeader(n[0..namelen], v[0..valuelen]);
        }
        return 0;
    }

    fn onDataChunkRecv(
        session: ?*c.nghttp2_session,
        _: u8,
        stream_id: i32,
        data: ?[*]const u8,
        len: usize,
        _: ?*anyopaque,
    ) callconv(.c) c_int {
        const d = data orelse return 0;
        const stream_ud = c.nghttp2_session_get_stream_user_data(session, stream_id);
        if (stream_ud) |ud| {
            const state: *StreamState = @ptrCast(@alignCast(ud));
            state.handleData(d[0..len]);
        }
        return 0;
    }

    fn onStreamClose(
        session: ?*c.nghttp2_session,
        stream_id: i32,
        _: u32,
        _: ?*anyopaque,
    ) callconv(.c) c_int {
        const stream_ud = c.nghttp2_session_get_stream_user_data(session, stream_id);
        if (stream_ud) |ud| {
            const state: *StreamState = @ptrCast(@alignCast(ud));
            state.closed = true;
        }
        return 0;
    }
};

/// Per-stream state used by gRPC to accumulate response data.
pub const StreamState = struct {
    allocator: mem.Allocator,
    http_status: ?u32 = null,
    grpc_status: ?u32 = null,
    grpc_message: ?[]const u8 = null,
    data: std.ArrayList(u8),
    closed: bool = false,

    pub fn init(allocator: mem.Allocator) StreamState {
        return .{
            .allocator = allocator,
            .data = .empty,
        };
    }

    pub fn deinit(self: *StreamState) void {
        if (self.grpc_message) |message| self.allocator.free(message);
        self.data.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn isDone(ctx: ?*anyopaque) bool {
        const self: *StreamState = @ptrCast(@alignCast(ctx));
        return self.closed;
    }

    pub fn handleHeader(self: *StreamState, name: []const u8, value: []const u8) void {
        if (mem.eql(u8, name, ":status")) {
            self.http_status = std.fmt.parseInt(u32, value, 10) catch null;
        } else if (mem.eql(u8, name, "grpc-status")) {
            self.grpc_status = std.fmt.parseInt(u32, value, 10) catch null;
        } else if (mem.eql(u8, name, "grpc-message")) {
            // Dupe: `value` points into nghttp2-owned memory that is invalid
            // once the header callback returns.
            if (self.grpc_message) |old| self.allocator.free(old);
            self.grpc_message = self.allocator.dupe(u8, value) catch null;
        }
    }

    pub fn handleData(self: *StreamState, chunk: []const u8) void {
        self.data.appendSlice(self.allocator, chunk) catch |err| {
            log.err("appendSlice failed: {s}", .{@errorName(err)});
        };
    }
};
