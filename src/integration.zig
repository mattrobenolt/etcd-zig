//! Integration tests against a live etcd.
//!
//! `zig build test-integration -Detcd-endpoint=127.0.0.1:2379`. The justfile
//! starts a throwaway etcd and passes the endpoint. Every test makes its own
//! connection, so one test cannot leave the next one a poisoned session.

const std = @import("std");
const testing = std.testing;
const expect = testing.expect;
const expectEqual = testing.expectEqual;
const expectEqualStrings = testing.expectEqualStrings;

const config = @import("config");
const etcd = @import("etcd");

/// `Client.init` wires internal pointers to the client's own address, so the
/// caller must initialize it where it lives. A helper returns it by value at
/// its peril.
fn connect(client: *etcd.Client) !void {
    try client.init(testing.allocator, testing.io, config.endpoint, .{});
}

test "integration: put, range, and delete" {
    var client: etcd.Client = undefined;
    try connect(&client);
    defer client.deinit();

    var put = try client.put(.{ .key = "integration/basic", .value = "hello" }, .{});
    defer put.deinit();
    const put_value = try put.tryValue();
    try expect(put_value.header != null);

    var got = try client.range(.{ .key = "integration/basic" }, .{});
    defer got.deinit();
    const got_value = try got.tryValue();
    try expectEqual(@as(usize, 1), got_value.kvs.len);
    try expectEqualStrings("hello", got_value.kvs[0].value);

    var deleted = try client.deleteRange(.{ .key = "integration/basic" }, .{});
    defer deleted.deinit();
    const deleted_value = try deleted.tryValue();
    try expectEqual(@as(i64, 1), deleted_value.deleted);

    var empty = try client.range(.{ .key = "integration/basic" }, .{});
    defer empty.deinit();
    try expectEqual(@as(usize, 0), (try empty.tryValue()).kvs.len);
}

test "integration: lease grant, attach, and revoke" {
    var client: etcd.Client = undefined;
    try connect(&client);
    defer client.deinit();

    var grant = try client.leaseGrant(.{ .ttl = 30 }, .{});
    defer grant.deinit();
    const grant_value = try grant.tryValue();
    try expect(grant_value.id != 0);
    try expect(grant_value.ttl > 0);
    const lease_id = grant_value.id;

    var put = try client.put(.{
        .key = "integration/lease",
        .value = "attached",
        .lease = lease_id,
    }, .{});
    defer put.deinit();
    _ = try put.tryValue();

    var ttl = try client.leaseTimeToLive(.{ .id = lease_id, .keys = true }, .{});
    defer ttl.deinit();
    const ttl_value = try ttl.tryValue();
    try expect(ttl_value.ttl > 0);
    try expectEqual(@as(usize, 1), ttl_value.keys.len);
    try expectEqualStrings("integration/lease", ttl_value.keys[0]);

    var revoke = try client.leaseRevoke(.{ .id = lease_id }, .{});
    defer revoke.deinit();
    _ = try revoke.tryValue();

    var gone = try client.range(.{ .key = "integration/lease" }, .{});
    defer gone.deinit();
    try expectEqual(@as(usize, 0), (try gone.tryValue()).kvs.len);
}

test "integration: watch delivers put and delete events" {
    const gpa = testing.allocator;

    var client: etcd.Client = undefined;
    try connect(&client);
    defer client.deinit();

    var watcher = try etcd.Watcher.init(gpa, testing.io, config.endpoint, .{
        .target = .{ .prefix = "integration/watch/" },
    });
    defer watcher.deinit();

    const watch_id = blk: {
        var created = (try watcher.next()) orelse return error.WatchClosed;
        defer created.deinit();
        try expect(created.value.created);
        break :blk created.value.watch_id;
    };

    try watcher.requestProgress();
    {
        var progress = (try watcher.next()) orelse return error.WatchClosed;
        defer progress.deinit();
        try expect(!progress.value.created);
        try expect(progress.value.header != null);
    }

    var put = try client.put(.{ .key = "integration/watch/key", .value = "one" }, .{});
    defer put.deinit();
    _ = try put.tryValue();

    {
        var response = (try watcher.next()) orelse return error.WatchClosed;
        defer response.deinit();
        try expectEqual(@as(usize, 1), response.value.events.len);
        try expectEqual(etcd.mvccpb.Event.Type.put, response.value.events[0].type);
        try expectEqualStrings("integration/watch/key", response.value.events[0].kv.key);
    }

    var deleted = try client.deleteRange(.{ .key = "integration/watch/key" }, .{});
    defer deleted.deinit();
    _ = try deleted.tryValue();

    {
        var response = (try watcher.next()) orelse return error.WatchClosed;
        defer response.deinit();
        try expectEqual(@as(usize, 1), response.value.events.len);
        try expectEqual(etcd.mvccpb.Event.Type.delete, response.value.events[0].type);
    }

    try watcher.cancel(watch_id);
    try expect((try watcher.next()) == null);
}

test "integration: maintenance status and member list" {
    var client: etcd.Client = undefined;
    try connect(&client);
    defer client.deinit();

    var status = try client.maintenanceStatus(.{});
    defer status.deinit();
    const status_value = try status.tryValue();
    try expect(status_value.version.len > 0);
    try expect(status_value.leader != 0);

    var members = try client.memberList(.{});
    defer members.deinit();
    const members_value = try members.tryValue();
    try expect(members_value.members.len >= 1);
}

test "integration: authenticate against a disabled server" {
    var client: etcd.Client = undefined;
    try connect(&client);
    defer client.deinit();

    const result = try client.authenticate(.{
        .username = "nobody",
        .password = "unused",
    }, .{});
    try expect(result == .disabled);
    try expect(client.token() == null);
}

test "integration: authenticate and call with a token" {
    if (config.auth_endpoint.len == 0) return error.SkipZigTest;

    const gpa = testing.allocator;
    var client: etcd.Client = undefined;
    try client.init(gpa, testing.io, config.auth_endpoint, .{});
    defer client.deinit();

    {
        var rejected = try client.range(.{ .key = "integration/auth" }, .{});
        defer rejected.deinit();
        try expect(!rejected.ok());
        try expect(etcd.auth.needsReauth(rejected.status.message));
    }

    const result = try client.authenticate(.{
        .username = config.auth_user,
        .password = config.auth_password,
    }, .{});
    try expect(result == .authenticated);
    try expect(client.token() != null);

    {
        var put = try client.put(.{ .key = "integration/auth", .value = "ok" }, .{});
        defer put.deinit();
        _ = try put.tryValue();
    }

    {
        var got = try client.range(.{ .key = "integration/auth" }, .{});
        defer got.deinit();
        const value = try got.tryValue();
        try expectEqual(@as(usize, 1), value.kvs.len);
        try expectEqualStrings("ok", value.kvs[0].value);
    }

    try client.setToken("not-a-token");
    {
        var rejected = try client.range(.{ .key = "integration/auth" }, .{});
        defer rejected.deinit();
        try expect(!rejected.ok());
        try expect(etcd.auth.needsReauth(rejected.status.message));
    }
}

test "integration: stop unblocks an idle watch" {
    var watcher = try etcd.Watcher.init(testing.allocator, testing.io, config.endpoint, .{
        .target = .{ .prefix = "integration/stop/" },
    });
    defer watcher.deinit();

    const first = try watcher.nextEvent();
    switch (first) {
        .response => |response| {
            var parsed = response;
            parsed.deinit();
        },
        else => return error.WatchNotEstablished,
    }

    const Stopper = struct {
        fn run(w: *etcd.Watcher) void {
            var threaded: std.Io.Threaded = .init_single_threaded;
            defer threaded.deinit();
            const io = threaded.io();
            io.sleep(.fromNanoseconds(100 * std.time.ns_per_ms), .awake) catch {};
            w.stop(io);
        }
    };
    const stopper = try std.Thread.spawn(.{}, Stopper.run, .{&watcher});

    // A response may already be queued when stop() fires. Drain it and
    // require the next call to observe the stop.
    var stopped = false;
    var attempts: usize = 0;
    while (attempts < 16) : (attempts += 1) {
        switch (try watcher.nextEvent()) {
            .response => |response| {
                var parsed = response;
                parsed.deinit();
            },
            .reconnecting => |err| return err,
            .stopped => {
                stopped = true;
                break;
            },
        }
    }
    stopper.join();
    try expect(stopped);
}
