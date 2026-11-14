//! etcd token authentication: the flow etcd actually supports.
//!
//! etcd's gRPC surface does not accept HTTP Basic auth. A client exchanges
//! credentials for a token through Auth.Authenticate, sends the token as the
//! `token` metadata field on every call, and authenticates again when the
//! server rejects it.
//!
//! This module holds the pieces that do not need a connection: the metadata
//! name, the credentials, and the classification of an auth rejection.
//! `Client.authenticate` performs the exchange and stores the token.

const std = @import("std");
const mem = std.mem;
const crypto = std.crypto;
const Allocator = std.mem.Allocator;
const testing = std.testing;

/// The gRPC metadata field etcd inspects (rpctypes.TokenFieldNameGRPC).
/// The name is `token`, not `authorization`.
pub const token_header_name = "token";

const err_invalid_auth_token = "etcdserver: invalid auth token";
const err_auth_old_revision = "etcdserver: revision of auth store is old";
const err_auth_not_enabled = "etcdserver: authentication is not enabled";
const err_user_empty = "etcdserver: user name is empty";

pub const Credentials = struct {
    username: []const u8,
    password: []const u8,
};

/// Whether a rejection means a fresh Authenticate call may fix the request.
///
/// Simple tokens expire, an auth store change invalidates them, and a token is
/// member-local, so a reconnect behind a load-balanced service can land on a
/// member that never issued it.
pub fn needsReauth(message: ?[]const u8) bool {
    const text = message orelse return false;
    return mem.find(u8, text, err_invalid_auth_token) != null or
        mem.find(u8, text, err_auth_old_revision) != null or
        mem.find(u8, text, err_user_empty) != null;
}

/// Whether the server has authentication disabled, so requests should proceed
/// without a token.
pub fn isAuthNotEnabled(message: ?[]const u8) bool {
    const text = message orelse return false;
    return mem.find(u8, text, err_auth_not_enabled) != null;
}

/// Owned credentials. Both fields are secrets: `deinit` wipes them first.
pub const OwnedCredentials = struct {
    username: []u8,
    password: []u8,

    pub fn init(gpa: Allocator, credentials: Credentials) !OwnedCredentials {
        const username = try gpa.dupe(u8, credentials.username);
        errdefer gpa.free(username);
        return .{
            .username = username,
            .password = try gpa.dupe(u8, credentials.password),
        };
    }

    pub fn deinit(self: *OwnedCredentials, gpa: Allocator) void {
        crypto.secureZero(u8, self.username);
        gpa.free(self.username);
        crypto.secureZero(u8, self.password);
        gpa.free(self.password);
        self.* = undefined;
    }
};

test "needsReauth classifies retryable auth rejections" {
    try testing.expect(needsReauth("etcdserver: invalid auth token"));
    try testing.expect(needsReauth("etcdserver: revision of auth store is old"));
    try testing.expect(needsReauth("etcdserver: user name is empty"));
    try testing.expect(needsReauth("rpc error: etcdserver: invalid auth token (retry)"));

    try testing.expect(!needsReauth(null));
    try testing.expect(!needsReauth(""));
    try testing.expect(!needsReauth("etcdserver: authentication is not enabled"));
    try testing.expect(!needsReauth("etcdserver: permission denied"));
    try testing.expect(!needsReauth(
        "etcdserver: authentication failed, invalid user ID or password",
    ));
}

test "isAuthNotEnabled" {
    try testing.expect(isAuthNotEnabled("etcdserver: authentication is not enabled"));
    try testing.expect(!isAuthNotEnabled(null));
    try testing.expect(!isAuthNotEnabled("etcdserver: invalid auth token"));
}
