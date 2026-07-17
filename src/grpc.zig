const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;

const c = @import("root.zig").c;
const H2Connection = @import("h2_connection.zig").H2Connection;
const StreamState = @import("h2_connection.zig").StreamState;

const log = std.log.scoped(.grpc);

/// One extra request header to attach to a gRPC call. Names must already be
/// lowercase (an HTTP/2 requirement, enforced when the request is built).
/// `sensitive` headers are flagged NGHTTP2_NV_FLAG_NO_INDEX so secret values
/// (e.g. `authorization`) never enter the HPACK dynamic table; nghttp2's
/// normal copy behavior stays enabled, so the caller may wipe the value
/// buffer once the request has been submitted and flushed.
pub const RequestHeader = struct {
    name: []const u8,
    value: []const u8,
    sensitive: bool = false,
};

/// Per-request options shared by unary and server-streaming calls.
pub const RequestOptions = struct {
    headers: []const RequestHeader = &.{},
};

/// Fixed upper bound on extra headers per request. Headers live in a stack
/// array so request construction never allocates; excess headers are
/// rejected rather than grown.
pub const max_extra_headers = 4;

const base_header_count = 6;

pub const HeaderError = error{ TooManyRequestHeaders, InvalidHeaderName };

/// The six fixed gRPC headers plus bounded extras. The nv entries point into
/// caller-owned memory; nghttp2 copies names and values at submit time (no
/// NO_COPY flags), so the pointed-to bytes only need to outlive
/// nghttp2_submit_request2.
const Headers = struct {
    nva: [base_header_count + max_extra_headers]c.nghttp2_nv,
    len: usize,
};

/// Single construction site for request headers: both the unary and the
/// server-streaming builders go through here, so extra-header semantics
/// (bounds, name validation, NO_INDEX) cannot diverge between them.
fn buildHeaders(path: []const u8, options: RequestOptions) HeaderError!Headers {
    if (options.headers.len > max_extra_headers) return error.TooManyRequestHeaders;

    var headers: Headers = .{
        .nva = undefined,
        .len = base_header_count + options.headers.len,
    };
    headers.nva[0..base_header_count].* = .{
        makeNv(":method", "POST", .none),
        makeNv(":scheme", "http", .none),
        makeNv(":authority", "localhost", .none),
        makeNv(":path", path, .none),
        makeNv("content-type", "application/grpc+proto", .none),
        makeNv("te", "trailers", .none),
    };
    for (options.headers, base_header_count..) |header, i| {
        if (!validHeaderName(header.name)) return error.InvalidHeaderName;
        headers.nva[i] = makeNv(
            header.name,
            header.value,
            if (header.sensitive) .no_index else .none,
        );
    }
    return headers;
}

/// HTTP/2 requires lowercase field names; uppercase would poison the whole
/// connection with a protocol error rather than just this request.
fn validHeaderName(name: []const u8) bool {
    if (name.len == 0) return false;
    for (name) |b| {
        if (std.ascii.isUpper(b)) return false;
    }
    return true;
}

pub const GrpcResponse = struct {
    http_status: u32,
    grpc_status: u32,
    /// grpc-message trailer, if any. Owned by caller.
    grpc_message: ?[]const u8,
    /// Raw protobuf response bytes (gRPC length-prefix stripped). Owned by caller.
    data: []const u8,

    pub fn deinit(self: GrpcResponse, gpa: Allocator) void {
        if (self.grpc_message) |message| gpa.free(message);
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
        buf: ?[*]u8,
        length: usize,
        data_flags: ?*u32,
        source: ?*c.nghttp2_data_source,
        _: ?*anyopaque,
    ) callconv(.c) c.nghttp2_ssize {
        const src = source orelse return -1;
        const ctx: *DataProviderCtx = @ptrCast(@alignCast(src.ptr));
        const total = ctx.totalLen();
        const remaining = total - ctx.offset;
        const to_copy = @min(remaining, length);

        // Build the full framed message conceptually:
        //   [0]: compressed flag (0 = not compressed)
        //   [1..5]: big-endian u32 message length
        //   [5..]: payload bytes
        const dst = (buf orelse return -1)[0..to_copy];
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
            (data_flags orelse return -1).* |= @as(u32, @intCast(c.NGHTTP2_DATA_FLAG_EOF));
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
    return unaryCallWithOptions(gpa, conn, path, request_bytes, .{});
}

/// `unaryCall` with extra request headers.
pub fn unaryCallWithOptions(
    gpa: Allocator,
    conn: *H2Connection,
    path: []const u8,
    request_bytes: []const u8,
    options: RequestOptions,
) !GrpcResponse {
    log.debug("calling {s}", .{path});

    var headers = try buildHeaders(path, options);

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
        &headers.nva,
        headers.len,
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
    errdefer gpa.free(payload);

    // The stream state (and its trailer copy) dies with this call; the
    // response owns its own copy.
    const message = if (stream_state.grpc_message) |m| try gpa.dupe(u8, m) else null;

    return .{
        .http_status = http_status,
        .grpc_status = grpc_status,
        .grpc_message = message,
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
    return openServerStreamWithOptions(allocator, conn, path, request_bytes, .{});
}

/// `openServerStream` with extra request headers.
pub fn openServerStreamWithOptions(
    allocator: Allocator,
    conn: *H2Connection,
    path: []const u8,
    request_bytes: []const u8,
    options: RequestOptions,
) !GrpcStreamReader {
    log.debug("opening stream {s}", .{path});

    var reader: GrpcStreamReader = .{
        .conn = conn,
        .stream_state = .init(allocator),
    };
    errdefer reader.deinit();

    var headers = try buildHeaders(path, options);

    var provider_ctx: DataProviderCtx = .{ .payload = request_bytes };
    var data_provider: c.nghttp2_data_provider2 = .{
        .source = .{ .ptr = @ptrCast(&provider_ctx) },
        .read_callback = DataProviderCtx.readCallback,
    };

    const stream_id = c.nghttp2_submit_request2(
        conn.session,
        null,
        &headers.nva,
        headers.len,
        &data_provider,
        null, // set properly via activate() after return-by-value
    );
    if (stream_id < 0) return error.Nghttp2SubmitRequestFailed;
    reader.stream_id = stream_id;

    try conn.sendAll();
    return reader;
}

const NvFlag = enum { none, no_index };

fn makeNv(name: []const u8, value: []const u8, flag: NvFlag) c.nghttp2_nv {
    return .{
        .name = @constCast(name.ptr),
        .value = @constCast(value.ptr),
        .namelen = name.len,
        .valuelen = value.len,
        .flags = @intCast(switch (flag) {
            .none => c.NGHTTP2_NV_FLAG_NONE,
            .no_index => c.NGHTTP2_NV_FLAG_NO_INDEX,
        }),
    };
}

// -- Tests -----------------------------------------------------------------------------

fn expectNv(
    nv: c.nghttp2_nv,
    name: []const u8,
    value: []const u8,
    no_index: bool,
) !void {
    try testing.expectEqualStrings(name, nv.name[0..nv.namelen]);
    try testing.expectEqualStrings(value, nv.value[0..nv.valuelen]);
    const no_index_flag: u8 = @intCast(c.NGHTTP2_NV_FLAG_NO_INDEX);
    try testing.expectEqual(no_index, nv.flags & no_index_flag != 0);
    // Copy behavior stays enabled: with NO_COPY unset, nghttp2 duplicates the
    // name/value at submit time, so callers may wipe secret buffers after the
    // request is flushed.
    const no_copy: u8 = @intCast(c.NGHTTP2_NV_FLAG_NO_COPY_NAME | c.NGHTTP2_NV_FLAG_NO_COPY_VALUE);
    try testing.expectEqual(0, nv.flags & no_copy);
}

test "StreamState owns its grpc-message copy" {
    // The header callback hands us a slice into nghttp2-owned memory that is
    // invalid after the callback returns; StreamState must keep its own copy
    // (auth classification reads it after the transport has moved on).
    var state: StreamState = .init(testing.allocator);
    defer state.deinit();

    var transient = "etcdserver: invalid auth token".*;
    state.handleHeader("grpc-message", &transient);
    @memset(&transient, 'x');
    try testing.expectEqualStrings("etcdserver: invalid auth token", state.grpc_message.?);

    // A repeated trailer replaces the copy without leaking (leak-checked by
    // testing.allocator).
    var second = "etcdserver: user name is empty".*;
    state.handleHeader("grpc-message", &second);
    try testing.expectEqualStrings("etcdserver: user name is empty", state.grpc_message.?);
}

test "buildHeaders: empty options preserve the existing header set" {
    const headers = try buildHeaders("/etcdserverpb.KV/Range", .{});
    try testing.expectEqual(base_header_count, headers.len);
    try expectNv(headers.nva[0], ":method", "POST", false);
    try expectNv(headers.nva[1], ":scheme", "http", false);
    try expectNv(headers.nva[2], ":authority", "localhost", false);
    try expectNv(headers.nva[3], ":path", "/etcdserverpb.KV/Range", false);
    try expectNv(headers.nva[4], "content-type", "application/grpc+proto", false);
    try expectNv(headers.nva[5], "te", "trailers", false);
}

test "buildHeaders: extra header appears with exact name and value" {
    const headers = try buildHeaders("/p", .{ .headers = &.{
        .{ .name = "x-custom", .value = "v1" },
    } });
    try testing.expectEqual(base_header_count + 1, headers.len);
    try expectNv(headers.nva[base_header_count], "x-custom", "v1", false);
}

test "buildHeaders: sensitive authorization header carries NO_INDEX" {
    const headers = try buildHeaders("/p", .{ .headers = &.{
        .{ .name = "authorization", .value = "Basic YWxpY2U6Zmlyc3Q=", .sensitive = true },
    } });
    try expectNv(
        headers.nva[base_header_count],
        "authorization",
        "Basic YWxpY2U6Zmlyc3Q=",
        true,
    );
}

test "buildHeaders: header-count guard rejects overflow" {
    const extra: RequestHeader = .{ .name = "x-h", .value = "v" };
    const too_many = [_]RequestHeader{extra} ** (max_extra_headers + 1);
    try testing.expectError(
        error.TooManyRequestHeaders,
        buildHeaders("/p", .{ .headers = &too_many }),
    );
    // Exactly max_extra_headers is still accepted.
    const headers = try buildHeaders("/p", .{ .headers = too_many[0..max_extra_headers] });
    try testing.expectEqual(base_header_count + max_extra_headers, headers.len);
}

test "buildHeaders: rejects non-lowercase and empty header names" {
    try testing.expectError(error.InvalidHeaderName, buildHeaders("/p", .{ .headers = &.{
        .{ .name = "Authorization", .value = "v" },
    } }));
    try testing.expectError(error.InvalidHeaderName, buildHeaders("/p", .{ .headers = &.{
        .{ .name = "", .value = "v" },
    } }));
}

test "unary and streaming entry points share the options path" {
    // Compile-time signature guard: both *WithOptions entry points take the
    // same RequestOptions, and the legacy wrappers remain source-compatible.
    // buildHeaders is the only header-construction site, so pinning the
    // signatures pins both request builders to one options path.
    const unary: *const fn (
        Allocator,
        *H2Connection,
        []const u8,
        []const u8,
        RequestOptions,
    ) anyerror!GrpcResponse = &unaryCallWithOptions;
    _ = unary;
    const streaming: *const fn (
        Allocator,
        *H2Connection,
        []const u8,
        []const u8,
        RequestOptions,
    ) anyerror!GrpcStreamReader = &openServerStreamWithOptions;
    _ = streaming;
    const legacy_unary: *const fn (
        Allocator,
        *H2Connection,
        []const u8,
        []const u8,
    ) anyerror!GrpcResponse = &unaryCall;
    _ = legacy_unary;
    const legacy_streaming: *const fn (
        Allocator,
        *H2Connection,
        []const u8,
        []const u8,
    ) anyerror!GrpcStreamReader = &openServerStream;
    _ = legacy_streaming;
}
