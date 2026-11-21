//! An etcd client for Zig 0.17, built on baskingspot/grpc and
//! baskingspot/protobuf.
//!
//! `Client` owns one connection and turns a request struct into one gRPC
//! call. `Watcher` owns a second connection for a watch that reconnects.
//! The etcd messages are plain structs in `etcdserverpb` and `mvccpb`; no
//! code generation is involved.

const std = @import("std");
const testing = std.testing;
const Io = std.Io;

const pb = @import("protobuf");

pub const auth = @import("auth.zig");
pub const client = @import("client.zig");
pub const Client = client.Client;
pub const Reply = client.Reply;
pub const CallOptions = client.CallOptions;
pub const OpenOptions = client.OpenOptions;
pub const ClientOptions = client.Options;
pub const KeepAlive = client.KeepAlive;
pub const TcpKeepAlive = client.TcpKeepAlive;
pub const AuthResult = client.AuthResult;
pub const Header = client.Header;
pub const endpoint = @import("endpoint.zig");
pub const etcdserverpb = @import("etcdserverpb.zig");
pub const mvccpb = @import("mvccpb.zig");
pub const range = @import("range.zig");
pub const KeyRange = range.KeyRange;
pub const Range = range.Range;
pub const prefixEnd = range.prefixEnd;
pub const watch = @import("watch.zig");
pub const Watcher = watch.Watcher;
pub const WatchOptions = watch.Options;

test {
    _ = auth;
    _ = client;
    _ = endpoint;
    _ = etcdserverpb;
    _ = mvccpb;
    _ = range;
    _ = watch;
}

test "PutRequest keeps etcd's field numbers" {
    const request: etcdserverpb.PutRequest = .{
        .key = "hello",
        .value = "world",
        .lease = 7,
        .prev_kv = true,
    };

    var buffer: [64]u8 = undefined;
    var writer: Io.Writer = .fixed(&buffer);
    try pb.encode(etcdserverpb.PutRequest, request, .{}, &writer);

    var reader: Io.Reader = .fixed(writer.buffered());
    var parsed = try pb.decode(etcdserverpb.PutRequest, testing.allocator, &reader, .{});
    defer parsed.deinit();

    try testing.expectEqualStrings("hello", parsed.value.key);
    try testing.expectEqualStrings("world", parsed.value.value);
    try testing.expectEqual(@as(i64, 7), parsed.value.lease);
    try testing.expect(parsed.value.prev_kv);
}

test "WatchResponse decodes repeated events" {
    const response: etcdserverpb.WatchResponse = .{
        .header = .{ .revision = 99 },
        .watch_id = 3,
        .events = &.{
            .{ .type = .put, .kv = .{ .key = "a", .value = "1", .mod_revision = 98 } },
            .{ .type = .delete, .kv = .{ .key = "b", .mod_revision = 99 } },
        },
    };

    var buffer: [256]u8 = undefined;
    var writer: Io.Writer = .fixed(&buffer);
    try pb.encode(etcdserverpb.WatchResponse, response, .{}, &writer);

    var reader: Io.Reader = .fixed(writer.buffered());
    var parsed = try pb.decode(
        etcdserverpb.WatchResponse,
        testing.allocator,
        &reader,
        .{},
    );
    defer parsed.deinit();

    try testing.expectEqual(@as(i64, 99), parsed.value.header.?.revision);
    try testing.expectEqual(@as(usize, 2), parsed.value.events.len);
    try testing.expectEqual(mvccpb.Event.Type.put, parsed.value.events[0].type);
    try testing.expectEqualStrings("a", parsed.value.events[0].kv.key);
    try testing.expectEqual(mvccpb.Event.Type.delete, parsed.value.events[1].type);
}
