//! A typed etcd client over `baskingspot/grpc`.
//!
//! The client owns one HTTP/2 connection and a token. A call encodes a plain
//! struct, sends one gRPC message, and decodes the reply into an arena the
//! caller frees with `Reply.deinit`. Streaming calls use `open` and `stream`;
//! `Watcher` builds a reconnecting watch on top of those.

const std = @import("std");
const Io = std.Io;
const mem = std.mem;
const crypto = std.crypto;
const testing = std.testing;
const Allocator = mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const grpc = @import("grpc");
/// The metadata field type grpc accepts; it is http.Header behind the alias.
pub const Header = grpc.Header;
const pb = @import("protobuf");
const time = @import("stdx").time;

const auth = @import("auth.zig");
const endpoint_mod = @import("endpoint.zig");
const Endpoint = endpoint_mod.Endpoint;
const etcdserverpb = @import("etcdserverpb.zig");

pub const KeepAlive = grpc.KeepAlive;
pub const TcpKeepAlive = endpoint_mod.KeepAlive;

const log = std.log.scoped(.etcd);

pub const kv_service = "etcdserverpb.KV";
pub const watch_service = "etcdserverpb.Watch";
pub const lease_service = "etcdserverpb.Lease";
pub const auth_service = "etcdserverpb.Auth";
pub const maintenance_service = "etcdserverpb.Maintenance";
pub const cluster_service = "etcdserverpb.Cluster";

/// The most extra metadata fields one call may carry, not counting the token.
pub const max_headers = 8;

pub const CallOptions = struct {
    /// The deadline for a unary call. Five seconds keeps a dead server from
    /// hanging the caller; set `.none` for an unbounded wait.
    timeout: time.OptionalDuration = .fromSecondsComptime(5),
    /// Regular gRPC metadata. Names must be lowercase.
    headers: []const Header = &.{},
};

pub const OpenOptions = struct {
    /// A stream usually has no deadline. The default is `.none`.
    timeout: time.OptionalDuration = .none,
    headers: []const Header = &.{},
};

pub const Options = struct {
    /// The space one unary response may use, for the message and the status
    /// text. Four MiB matches gRPC's default receive limit.
    reply_capacity: usize = 4 * 1024 * 1024,
    /// TCP keepalive policy applied when dialing the endpoint. Null disables it.
    tcp_keepalive: ?TcpKeepAlive = .default,
    /// HTTP/2 PING keepalive policy. Disabled by default on unary clients;
    /// `Watcher` enables it by default.
    keepalive: KeepAlive = .{},
};

/// A unary reply: the gRPC status, the decoded message when one arrived, and
/// the arena that owns both. `deinit` frees the arena.
pub fn Reply(comptime T: type) type {
    return struct {
        const Self = @This();

        status: grpc.status.Status,
        value: ?T = null,
        arena: ArenaAllocator,

        pub fn deinit(self: *Self) void {
            self.arena.deinit();
            self.* = undefined;
        }

        pub fn ok(self: *const Self) bool {
            return self.status.code == .ok;
        }

        /// The decoded message. Its slices borrow this reply's arena, so keep
        /// the reply alive while the value is in use.
        pub fn tryValue(self: *const Self) !T {
            if (self.status.code != .ok) return error.GrpcCallFailed;
            return self.value orelse error.MissingResponse;
        }
    };
}

pub const AuthResult = union(enum) {
    /// The client now holds a token.
    authenticated,
    /// The server has authentication disabled. The client holds no token.
    disabled,
};

pub const Client = struct {
    gpa: Allocator,
    endpoint: []u8,
    transport: grpc.Client,
    auth_token: ?[]u8 = null,
    request_buffer: std.ArrayList(u8) = .empty,
    reply_buffer: []u8 = &.{},

    /// On success the client owns the socket. On failure the caller keeps
    /// nothing and must not call `deinit`. The client wires internal pointers
    /// to its own address: initialize it where it lives and do not move it.
    pub fn init(
        self: *Client,
        gpa: Allocator,
        // ziglint-ignore: Z023 -- the allocator stays first to mirror grpc.Client.init.
        io: Io,
        endpoint: []const u8,
        options: Options,
    ) !void {
        const parsed = try Endpoint.parse(endpoint);

        self.gpa = gpa;
        self.endpoint = &.{};
        self.auth_token = null;
        self.request_buffer = .empty;
        self.reply_buffer = &.{};

        const endpoint_copy = try gpa.dupe(u8, endpoint);
        errdefer gpa.free(endpoint_copy);
        const reply_buffer = try gpa.alloc(u8, options.reply_capacity);
        errdefer gpa.free(reply_buffer);

        const socket = try parsed.connectWithKeepAlive(io, options.tcp_keepalive);
        self.transport.init(gpa, io, socket) catch |err| {
            socket.close(io);
            return err;
        };
        errdefer self.transport.deinit();
        if (options.keepalive.idle_time.get() != null) {
            try self.transport.setKeepAlive(options.keepalive);
        }

        self.endpoint = endpoint_copy;
        self.reply_buffer = reply_buffer;
    }

    pub fn deinit(self: *Client) void {
        self.clearToken();
        self.transport.deinit();
        self.gpa.free(self.endpoint);
        self.gpa.free(self.reply_buffer);
        self.request_buffer.deinit(self.gpa);
        self.* = undefined;
    }

    /// The current token, or null. The slice is a secret and lives until the
    /// next token change.
    pub fn token(self: *const Client) ?[]const u8 {
        return self.auth_token;
    }

    /// Replace the token. The client owns its copy. Wipe the old one first.
    pub fn setToken(self: *Client, value: []const u8) !void {
        const copy = try self.gpa.dupe(u8, value);
        self.replaceToken(copy);
    }

    pub fn clearToken(self: *Client) void {
        const current = self.auth_token orelse return;
        crypto.secureZero(u8, current);
        self.gpa.free(current);
        self.auth_token = null;
    }

    /// Exchange credentials for a token and store it. An auth-disabled server
    /// clears any token and returns `.disabled`.
    pub fn authenticate(
        self: *Client,
        credentials: auth.Credentials,
        options: CallOptions,
    ) !AuthResult {
        const request: etcdserverpb.AuthenticateRequest = .{
            .name = credentials.username,
            .password = credentials.password,
        };
        const body = try self.encodeRequest(etcdserverpb.AuthenticateRequest, request);
        defer self.wipeRequestBody(body);

        // Authentication carries credentials, never a token. User metadata is
        // still useful for tracing.
        var storage: [max_headers + 1]Header = undefined;
        const metadata = try fillMetadata(null, options.headers, &storage);
        const raw = try self.perform(body, auth_service, "Authenticate", metadata, options.timeout);
        defer crypto.secureZero(u8, @constCast(raw.message));

        if (raw.status.code != .ok) {
            if (auth.isAuthNotEnabled(raw.status.message)) {
                self.clearToken();
                return .disabled;
            }
            log.debug("authenticate rejected: {s}", .{raw.status.message});
            return error.AuthenticateFailed;
        }

        var reader: Io.Reader = .fixed(raw.message);
        var parsed = try pb.decode(etcdserverpb.AuthenticateResponse, self.gpa, &reader, .{});
        defer {
            crypto.secureZero(u8, @constCast(parsed.value.token));
            parsed.deinit();
        }
        if (parsed.value.token.len == 0) return error.AuthenticateFailed;

        const copy = try self.gpa.dupe(u8, parsed.value.token);
        self.replaceToken(copy);
        return .authenticated;
    }

    /// One typed unary call. The token, when present, is added to the metadata.
    pub fn unary(
        self: *Client,
        comptime Request: type,
        comptime Response: type,
        service: []const u8,
        method: []const u8,
        request: Request,
        options: CallOptions,
    ) !Reply(Response) {
        const body = try self.encodeRequest(Request, request);
        var storage: [max_headers + 1]Header = undefined;
        const metadata = try fillMetadata(self.auth_token, options.headers, &storage);
        const raw = try self.perform(body, service, method, metadata, options.timeout);
        return decodeReply(Response, self.gpa, raw);
    }

    pub fn put(
        self: *Client,
        request: etcdserverpb.PutRequest,
        options: CallOptions,
    ) !Reply(etcdserverpb.PutResponse) {
        return self.unary(
            etcdserverpb.PutRequest,
            etcdserverpb.PutResponse,
            kv_service,
            "Put",
            request,
            options,
        );
    }

    pub fn range(
        self: *Client,
        request: etcdserverpb.RangeRequest,
        options: CallOptions,
    ) !Reply(etcdserverpb.RangeResponse) {
        return self.unary(
            etcdserverpb.RangeRequest,
            etcdserverpb.RangeResponse,
            kv_service,
            "Range",
            request,
            options,
        );
    }

    pub fn deleteRange(
        self: *Client,
        request: etcdserverpb.DeleteRangeRequest,
        options: CallOptions,
    ) !Reply(etcdserverpb.DeleteRangeResponse) {
        return self.unary(
            etcdserverpb.DeleteRangeRequest,
            etcdserverpb.DeleteRangeResponse,
            kv_service,
            "DeleteRange",
            request,
            options,
        );
    }

    pub fn compact(
        self: *Client,
        request: etcdserverpb.CompactionRequest,
        options: CallOptions,
    ) !Reply(etcdserverpb.CompactionResponse) {
        return self.unary(
            etcdserverpb.CompactionRequest,
            etcdserverpb.CompactionResponse,
            kv_service,
            "Compact",
            request,
            options,
        );
    }

    pub fn leaseGrant(
        self: *Client,
        request: etcdserverpb.LeaseGrantRequest,
        options: CallOptions,
    ) !Reply(etcdserverpb.LeaseGrantResponse) {
        return self.unary(
            etcdserverpb.LeaseGrantRequest,
            etcdserverpb.LeaseGrantResponse,
            lease_service,
            "LeaseGrant",
            request,
            options,
        );
    }

    pub fn leaseRevoke(
        self: *Client,
        request: etcdserverpb.LeaseRevokeRequest,
        options: CallOptions,
    ) !Reply(etcdserverpb.LeaseRevokeResponse) {
        return self.unary(
            etcdserverpb.LeaseRevokeRequest,
            etcdserverpb.LeaseRevokeResponse,
            lease_service,
            "LeaseRevoke",
            request,
            options,
        );
    }

    pub fn leaseTimeToLive(
        self: *Client,
        request: etcdserverpb.LeaseTimeToLiveRequest,
        options: CallOptions,
    ) !Reply(etcdserverpb.LeaseTimeToLiveResponse) {
        return self.unary(
            etcdserverpb.LeaseTimeToLiveRequest,
            etcdserverpb.LeaseTimeToLiveResponse,
            lease_service,
            "LeaseTimeToLive",
            request,
            options,
        );
    }

    pub fn maintenanceStatus(
        self: *Client,
        options: CallOptions,
    ) !Reply(etcdserverpb.StatusResponse) {
        return self.unary(
            etcdserverpb.StatusRequest,
            etcdserverpb.StatusResponse,
            maintenance_service,
            "Status",
            .{},
            options,
        );
    }

    pub fn memberList(
        self: *Client,
        options: CallOptions,
    ) !Reply(etcdserverpb.MemberListResponse) {
        return self.unary(
            etcdserverpb.MemberListRequest,
            etcdserverpb.MemberListResponse,
            cluster_service,
            "MemberList",
            .{},
            options,
        );
    }

    /// Open a streaming call. The caller stores the returned `Call` in stable
    /// memory and passes it to `stream`; the call borrows this client.
    pub fn open(
        self: *Client,
        service: []const u8,
        method: []const u8,
        options: OpenOptions,
    ) !grpc.Call {
        var storage: [max_headers + 1]Header = undefined;
        const metadata = try fillMetadata(self.auth_token, options.headers, &storage);
        return self.transport.open(.{
            .authority = self.endpoint,
            .service = service,
            .method = method,
            .streaming = true,
            .timeout = options.timeout,
            .metadata = metadata,
        });
    }

    /// The byte-stream facade over an open call.
    pub fn stream(self: *Client, call: *grpc.Call, calls: []grpc.Call) grpc.Stream {
        return self.transport.stream(call, calls);
    }

    fn perform(
        self: *Client,
        request_bytes: []const u8,
        service: []const u8,
        method: []const u8,
        metadata: []const Header,
        timeout: time.OptionalDuration,
    ) !grpc.Response {
        return self.transport.unary(.{
            .authority = self.endpoint,
            .service = service,
            .method = method,
            .message = request_bytes,
            .timeout = timeout,
            .metadata = metadata,
        }, self.reply_buffer);
    }

    fn encodeRequest(self: *Client, comptime T: type, value: T) ![]const u8 {
        const size = pb.encodedSize(T, value, .{});
        self.request_buffer.clearRetainingCapacity();
        const buffer = try self.request_buffer.addManyAsSlice(self.gpa, size);

        var writer: Io.Writer = .fixed(buffer);
        try pb.encode(T, value, .{}, &writer);
        std.debug.assert(writer.buffered().len == size);
        return writer.buffered();
    }

    fn wipeRequestBody(self: *Client, body: []const u8) void {
        crypto.secureZero(u8, @constCast(body));
        self.request_buffer.clearRetainingCapacity();
    }

    fn replaceToken(self: *Client, value: []u8) void {
        self.clearToken();
        self.auth_token = value;
    }
};

fn fillMetadata(
    token: ?[]const u8,
    extra: []const Header,
    storage: *[max_headers + 1]Header,
) ![]const Header {
    if (extra.len > max_headers) return error.TooManyHeaders;

    var len: usize = 0;
    if (token) |value| {
        storage[0] = .{ .name = auth.token_header_name, .value = value };
        len = 1;
    }
    @memcpy(storage[len..][0..extra.len], extra);
    len += extra.len;
    return storage[0..len];
}

fn decodeReply(
    comptime T: type,
    gpa: Allocator,
    raw: grpc.Response,
) !Reply(T) {
    var reply: Reply(T) = .{
        .status = .{ .code = raw.status.code },
        .arena = .init(gpa),
    };
    errdefer reply.arena.deinit();

    if (raw.status.message.len > 0) {
        reply.status.message = try reply.arena.allocator().dupe(u8, raw.status.message);
    }
    if (raw.message.len > 0) {
        var reader: Io.Reader = .fixed(raw.message);
        reply.value = pb.decodeLeaky(T, reply.arena.allocator(), &reader, .{}) catch |err| blk: {
            if (raw.status.code == .ok) return err;
            break :blk null;
        };
    }
    return reply;
}

test "fillMetadata: no token passes the extra fields through" {
    var storage: [max_headers + 1]Header = undefined;
    const extra = [_]Header{.{ .name = "x-test", .value = "1" }};
    const metadata = try fillMetadata(null, &extra, &storage);

    try testing.expectEqual(@as(usize, 1), metadata.len);
    try testing.expectEqualStrings("x-test", metadata[0].name);
    try testing.expectEqualStrings("1", metadata[0].value);
}

test "fillMetadata: the token comes first" {
    var storage: [max_headers + 1]Header = undefined;
    const extra = [_]Header{.{ .name = "x-test", .value = "1" }};
    const metadata = try fillMetadata("a-token", &extra, &storage);

    try testing.expectEqual(@as(usize, 2), metadata.len);
    try testing.expectEqualStrings(auth.token_header_name, metadata[0].name);
    try testing.expectEqualStrings("a-token", metadata[0].value);
    try testing.expectEqualStrings("x-test", metadata[1].name);
}

test "fillMetadata: a full set is accepted and one more is not" {
    var storage: [max_headers + 1]Header = undefined;
    const full: [max_headers]Header = @splat(.{ .name = "x", .value = "v" });
    const metadata = try fillMetadata("t", &full, &storage);
    try testing.expectEqual(@as(usize, max_headers + 1), metadata.len);

    const over: [max_headers + 1]Header = @splat(.{ .name = "x", .value = "v" });
    try testing.expectError(
        error.TooManyHeaders,
        fillMetadata("t", &over, &storage),
    );
}

test "decodeReply: a status message is copied into the reply arena" {
    const gpa = testing.allocator;

    var encoded: Io.Writer.Allocating = .init(gpa);
    defer encoded.deinit();
    try pb.encode(
        etcdserverpb.PutResponse,
        .{ .header = .{ .revision = 42 } },
        .{},
        &encoded.writer,
    );

    var status_text_buf = "deadline exceeded".*;
    const raw: grpc.Response = .{
        .status = .{ .code = .ok, .message = &status_text_buf },
        .message = encoded.written(),
    };

    var reply = try decodeReply(etcdserverpb.PutResponse, gpa, raw);
    defer reply.deinit();
    @memset(&status_text_buf, 'x');

    try testing.expect(reply.ok());
    try testing.expectEqualStrings("deadline exceeded", reply.status.message);
    try testing.expectEqual(@as(i64, 42), reply.value.?.header.?.revision);
}

test "decodeReply: an error status keeps the status when the body does not decode" {
    const gpa = testing.allocator;
    var body = "not a protobuf message".*;

    var reply = try decodeReply(etcdserverpb.PutResponse, gpa, .{
        .status = .{ .code = .unavailable, .message = "try again" },
        .message = &body,
    });
    defer reply.deinit();

    try testing.expect(!reply.ok());
    try testing.expectEqual(grpc.status.Code.unavailable, reply.status.code);
    try testing.expect(reply.value == null);
}
