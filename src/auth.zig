//! etcd token authentication: the flow etcd actually supports.
//!
//! etcd's gRPC surface does not accept HTTP Basic auth: the server takes the
//! raw metadata value and looks it up as a token issued by the
//! Auth.Authenticate RPC (server/auth/store.go AuthInfoFromCtx, same in 3.5
//! and 3.6). Clients exchange credentials for a token, attach the token to
//! every request, and re-authenticate when the server rejects it.
//!
//! Port of the tokenAuthInterceptor from mattrobenolt/connect-etcd#4. This
//! module provides the mechanism (the Authenticate RPC and rejection
//! classification); callers own the policy (token caching, retry-once,
//! reset on stream failures).

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;
const Io = std.Io;
const heap = std.heap;
const crypto = std.crypto;
const testing = std.testing;

const grpc = @import("grpc.zig");
const H2Connection = @import("h2_connection.zig").H2Connection;
const etcdserverpb = @import("proto/etcdserverpb.pb.zig");

const log = std.log.scoped(.etcd_auth);

/// gRPC metadata key etcd inspects for the auth token
/// (rpctypes.TokenFieldNameGRPC). Note: `token`, not `authorization`.
pub const token_header_name = "token";

// etcd error strings, stable across versions
// (see go.etcd.io/etcd/api/v3rpc/rpctypes).
const err_invalid_auth_token = "etcdserver: invalid auth token";
const err_auth_old_revision = "etcdserver: revision of auth store is old";
const err_auth_not_enabled = "etcdserver: authentication is not enabled";
const err_user_empty = "etcdserver: user name is empty";

/// Whether a rejected response indicates the token (or lack of one) was the
/// problem and a fresh Authenticate call may fix it:
///
/// - "invalid auth token": simple tokens expire (5m default TTL), are
///   invalidated by any auth store change, and are member-local, so a
///   reconnect behind a load-balanced Service can land on a member that
///   never issued ours.
/// - "revision of auth store is old": the token embeds an auth store
///   revision that has since changed.
/// - "user name is empty": no token was sent because auth looked disabled,
///   but it has been enabled server-side since.
pub fn needsReauth(grpc_message: ?[]const u8) bool {
    const message = grpc_message orelse return false;
    return mem.indexOf(u8, message, err_invalid_auth_token) != null or
        mem.indexOf(u8, message, err_auth_old_revision) != null or
        mem.indexOf(u8, message, err_user_empty) != null;
}

/// Whether a rejected response means the server has authentication disabled
/// entirely, so requests should proceed without a token.
pub fn isAuthNotEnabled(grpc_message: ?[]const u8) bool {
    const message = grpc_message orelse return false;
    return mem.indexOf(u8, message, err_auth_not_enabled) != null;
}

pub const AuthenticateResult = union(enum) {
    /// The issued token. Owned by the caller; it is a credential — wipe
    /// before free (e.g. `crypto.secureZero`).
    token: []u8,
    /// The server has authentication disabled. Proceed without a token;
    /// recovery is automatic — once auth gets enabled, requests fail with
    /// "user name is empty", which `needsReauth` classifies for retry.
    auth_disabled,
};

/// Exchange credentials for a token via Auth.Authenticate on `conn`. This is
/// the one RPC that authenticates with credentials rather than a token, so
/// it sends no token header itself. The encoded request buffer (which
/// carries the password) and the raw response bytes (which carry the token)
/// are wiped before returning on every path.
pub fn authenticate(
    gpa: Allocator,
    conn: *H2Connection,
    name: []const u8,
    password: []const u8,
) !AuthenticateResult {
    var arena_state: heap.ArenaAllocator = .init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const request: etcdserverpb.AuthenticateRequest = .{
        .name = name,
        .password = password,
    };
    var out: Io.Writer.Allocating = .init(gpa);
    defer {
        crypto.secureZero(u8, out.written());
        out.deinit();
    }
    try request.encode(&out.writer, arena);

    const response = try grpc.unaryCallWithOptions(
        gpa,
        conn,
        "/etcdserverpb.Auth/Authenticate",
        out.written(),
        .{},
    );
    defer {
        crypto.secureZero(u8, @constCast(response.data));
        response.deinit(gpa);
    }

    if (response.http_status != 200) return error.AuthenticateFailed;
    if (response.grpc_status != 0) {
        if (isAuthNotEnabled(response.grpc_message)) return .auth_disabled;
        // Bad credentials, deleted user, or server trouble. The message is
        // safe to log (etcd never echoes the password); the caller decides
        // whether/how to retry.
        log.debug("authenticate rejected: grpc-status {d}", .{response.grpc_status});
        return error.AuthenticateFailed;
    }

    var reader: Io.Reader = .fixed(response.data);
    const decoded = try etcdserverpb.AuthenticateResponse.decode(&reader, arena);
    if (decoded.token.len == 0) return error.AuthenticateFailed;
    return .{ .token = try gpa.dupe(u8, decoded.token) };
}

// -- Tests -----------------------------------------------------------------------------

test "needsReauth classifies retryable auth rejections" {
    try testing.expect(needsReauth("etcdserver: invalid auth token"));
    try testing.expect(needsReauth("etcdserver: revision of auth store is old"));
    try testing.expect(needsReauth("etcdserver: user name is empty"));
    // Embedded in a longer message still matches.
    try testing.expect(needsReauth("rpc error: etcdserver: invalid auth token (retry)"));

    try testing.expect(!needsReauth(null));
    try testing.expect(!needsReauth(""));
    try testing.expect(!needsReauth("etcdserver: authentication is not enabled"));
    try testing.expect(!needsReauth("etcdserver: permission denied"));
    try testing.expect(!needsReauth("etcdserver: authentication failed, invalid user ID or password"));
}

test "isAuthNotEnabled" {
    try testing.expect(isAuthNotEnabled("etcdserver: authentication is not enabled"));
    try testing.expect(!isAuthNotEnabled(null));
    try testing.expect(!isAuthNotEnabled("etcdserver: invalid auth token"));
}

test "AuthenticateRequest/Response wire round-trip" {
    const gpa = testing.allocator;
    var arena_state: heap.ArenaAllocator = .init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // Request: field 1 = name, field 2 = password (etcd rpc.proto).
    const request: etcdserverpb.AuthenticateRequest = .{
        .name = "exo-a",
        .password = "s3cr=et",
    };
    var out: Io.Writer.Allocating = .init(arena);
    try request.encode(&out.writer, arena);
    var request_reader: Io.Reader = .fixed(out.written());
    const request_decoded = try etcdserverpb.AuthenticateRequest.decode(&request_reader, arena);
    try testing.expectEqualStrings("exo-a", request_decoded.name);
    try testing.expectEqualStrings("s3cr=et", request_decoded.password);

    // Response: field 2 = token.
    const response: etcdserverpb.AuthenticateResponse = .{ .token = "abcde.12345" };
    var response_out: Io.Writer.Allocating = .init(arena);
    try response.encode(&response_out.writer, arena);
    var response_reader: Io.Reader = .fixed(response_out.written());
    const response_decoded = try etcdserverpb.AuthenticateResponse.decode(&response_reader, arena);
    try testing.expectEqualStrings("abcde.12345", response_decoded.token);
}
