const std = @import("std");
const Allocator = std.mem.Allocator;

const c = @import("root.zig").c;
const H2Connection = @import("h2_connection.zig").H2Connection;
const StreamState = @import("h2_connection.zig").StreamState;

const log = std.log.scoped(.grpc);

pub const GrpcResponse = struct {
    http_status: u32,
    grpc_status: u32,
    grpc_message: ?[]const u8,
    /// Raw protobuf response bytes (gRPC length-prefix stripped). Owned by caller.
    data: []const u8,

    pub fn deinit(self: GrpcResponse, gpa: Allocator) void {
        gpa.free(self.data);
    }
};

/// Context for the nghttp2 data provider read callback.
/// Serves the gRPC length-prefixed message: [1 byte flags][4 byte big-endian length][payload].
const DataProviderCtx = struct {
    payload: []const u8,
    offset: usize = 0,

    fn totalLen(self: *const DataProviderCtx) usize {
        return 5 + self.payload.len;
    }

    fn readCallback(
        _: ?*c.nghttp2_session,
        _: i32,
        buf: [*c]u8,
        length: usize,
        data_flags: [*c]u32,
        source: [*c]c.nghttp2_data_source,
        _: ?*anyopaque,
    ) callconv(.c) c.nghttp2_ssize {
        const ctx: *DataProviderCtx = @ptrCast(@alignCast(source.*.ptr));
        const total = ctx.totalLen();
        const remaining = total - ctx.offset;
        const to_copy = @min(remaining, length);

        // Build the full framed message conceptually:
        //   [0]: compressed flag (0 = not compressed)
        //   [1..5]: big-endian u32 message length
        //   [5..]: payload bytes
        const dst = buf[0..to_copy];
        for (dst, 0..) |*b, i| {
            const pos = ctx.offset + i;
            if (pos == 0) {
                b.* = 0; // not compressed
            } else if (pos < 5) {
                // Big-endian length bytes at positions 1-4.
                const payload_len: u32 = @intCast(ctx.payload.len);
                const shift: u5 = @intCast((4 - pos) * 8);
                b.* = @truncate(payload_len >> shift);
            } else {
                b.* = ctx.payload[pos - 5];
            }
        }

        ctx.offset += to_copy;
        if (ctx.offset >= total) {
            data_flags.* |= @as(u32, @intCast(c.NGHTTP2_DATA_FLAG_EOF));
        }
        return @intCast(to_copy);
    }
};

/// Make a gRPC unary call over an established h2c connection.
pub fn unaryCall(
    gpa: Allocator,
    conn: *H2Connection,
    path: []const u8,
    request_bytes: []const u8,
) !GrpcResponse {
    log.debug("calling {s}", .{path});

    // Build gRPC request headers.
    var nva = [_]c.nghttp2_nv{
        makeNv(":method", "POST"),
        makeNv(":scheme", "http"),
        makeNv(":authority", "localhost"),
        makeNv(":path", path),
        makeNv("content-type", "application/grpc+proto"),
        makeNv("te", "trailers"),
    };

    // Set up the data provider for the request body.
    var provider_ctx: DataProviderCtx = .{ .payload = request_bytes };
    var data_provider: c.nghttp2_data_provider2 = .{
        .source = .{ .ptr = @ptrCast(&provider_ctx) },
        .read_callback = DataProviderCtx.readCallback,
    };

    // Per-stream state to accumulate the response.
    var stream_state: StreamState = .init(gpa);
    defer stream_state.deinit();

    const stream_id = c.nghttp2_submit_request2(
        conn.session,
        null, // priority (ignored)
        &nva,
        nva.len,
        &data_provider,
        @ptrCast(&stream_state), // stream_user_data
    );
    if (stream_id < 0) return error.Nghttp2SubmitRequestFailed;

    // Flush the request.
    try conn.sendAll();

    // Receive until the stream closes.
    try conn.recvUntil(StreamState.isDone, @ptrCast(&stream_state));

    const http_status = stream_state.http_status orelse
        return error.MissingHttpStatus;
    const grpc_status = stream_state.grpc_status orelse
        return error.MissingGrpcStatus;

    log.debug("received grpc-status {d}", .{grpc_status});

    // Strip the 5-byte gRPC length prefix from the response data and dupe into caller-owned memory.
    const raw = stream_state.data.items;
    const payload_src = if (raw.len > 5) raw[5..] else raw[0..0];
    const payload = try gpa.dupe(u8, payload_src);

    return .{
        .http_status = http_status,
        .grpc_status = grpc_status,
        .grpc_message = stream_state.grpc_message,
        .data = payload,
    };
}

/// Iterator over gRPC-framed messages on a server-streaming response.
/// Each call to `next()` blocks until a complete message is available.
pub const GrpcStreamReader = struct {
    conn: *H2Connection,
    stream_state: StreamState,
    stream_id: i32 = -1,
    consumed: usize = 0,

    /// Must be called after the struct has reached its final memory location
    /// (i.e. after return-by-value from openServerStream) to fix the
    /// nghttp2 stream_user_data pointer.
    pub fn activate(self: *GrpcStreamReader) void {
        if (self.stream_id >= 0) {
            _ = c.nghttp2_session_set_stream_user_data(
                self.conn.session,
                self.stream_id,
                @ptrCast(&self.stream_state),
            );
        }
    }

    pub fn next(self: *GrpcStreamReader, allocator: Allocator) !?[]const u8 {
        while (true) {
            const available = self.stream_state.data.items[self.consumed..];

            if (available.len >= 5) {
                const msg_len: usize = @intCast(std.mem.readInt(u32, available[1..5], .big));
                const frame_len = 5 + msg_len;

                if (available.len >= frame_len) {
                    const payload = try allocator.dupe(u8, available[5..frame_len]);
                    self.consumed += frame_len;
                    self.compact();
                    return payload;
                }
            }

            if (self.stream_state.closed) return null;

            try self.readOnce();
        }
    }

    fn readOnce(self: *GrpcStreamReader) !void {
        var buf: [16384]u8 = undefined;
        const n = try self.conn.stream.read(&buf);
        if (n == 0) return error.ConnectionClosed;
        const consumed = c.nghttp2_session_mem_recv2(self.conn.session, &buf, n);
        if (consumed < 0) return error.Nghttp2MemRecvFailed;
        try self.conn.sendAll();
    }

    fn compact(self: *GrpcStreamReader) void {
        if (self.consumed == 0) return;
        const items = self.stream_state.data.items;
        const remaining = items.len - self.consumed;
        if (remaining > 0) {
            @memmove(items[0..remaining], items[self.consumed..][0..remaining]);
        }
        self.stream_state.data.items.len = remaining;
        self.consumed = 0;
    }

    pub fn deinit(self: *GrpcStreamReader) void {
        self.stream_state.deinit();
        self.* = undefined;
    }
};

/// Open a server-streaming gRPC call. Sends the request and returns
/// a `GrpcStreamReader` for iterating over response messages.
pub fn openServerStream(
    allocator: Allocator,
    conn: *H2Connection,
    path: []const u8,
    request_bytes: []const u8,
) !GrpcStreamReader {
    log.debug("opening stream {s}", .{path});

    var reader: GrpcStreamReader = .{
        .conn = conn,
        .stream_state = .init(allocator),
    };
    errdefer reader.deinit();

    var nva = [_]c.nghttp2_nv{
        makeNv(":method", "POST"),
        makeNv(":scheme", "http"),
        makeNv(":authority", "localhost"),
        makeNv(":path", path),
        makeNv("content-type", "application/grpc+proto"),
        makeNv("te", "trailers"),
    };

    var provider_ctx: DataProviderCtx = .{ .payload = request_bytes };
    var data_provider: c.nghttp2_data_provider2 = .{
        .source = .{ .ptr = @ptrCast(&provider_ctx) },
        .read_callback = DataProviderCtx.readCallback,
    };

    const stream_id = c.nghttp2_submit_request2(
        conn.session,
        null,
        &nva,
        nva.len,
        &data_provider,
        null, // set properly via activate() after return-by-value
    );
    if (stream_id < 0) return error.Nghttp2SubmitRequestFailed;
    reader.stream_id = stream_id;

    try conn.sendAll();
    return reader;
}

fn makeNv(name: []const u8, value: []const u8) c.nghttp2_nv {
    return .{
        .name = @constCast(name.ptr),
        .value = @constCast(value.ptr),
        .namelen = name.len,
        .valuelen = value.len,
        .flags = @intCast(c.NGHTTP2_NV_FLAG_NONE),
    };
}
