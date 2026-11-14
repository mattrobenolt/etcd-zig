//! A small demo: write, read, watch an event, delete, and cancel the watch.
//!
//! `zig build run -- 127.0.0.1:2379`. The endpoint defaults to the same.

const std = @import("std");
const print = std.debug.print;

const etcd = @import("etcd");

const default_endpoint = "127.0.0.1:2379";
const prefix = "etcd-zig-demo/";

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const arena = init.arena.allocator();
    const args = try init.minimal.args.toSlice(arena);
    const endpoint = if (args.len > 1) args[1] else default_endpoint;

    var client: etcd.Client = undefined;
    try client.init(init.gpa, io, endpoint, .{});
    defer client.deinit();

    var put = try client.put(.{ .key = prefix ++ "greeting", .value = "hello" }, .{});
    defer put.deinit();
    const put_value = try put.tryValue();
    print("put {s} at revision {d}\n", .{
        prefix ++ "greeting",
        if (put_value.header) |header| header.revision else 0,
    });

    var range_buffer: [64]u8 = undefined;
    const target: etcd.KeyRange = .{ .prefix = prefix };
    const bounds = try target.bounds(&range_buffer);
    var list = try client.range(.{ .key = bounds.key, .range_end = bounds.range_end }, .{});
    defer list.deinit();
    for ((try list.tryValue()).kvs) |kv| {
        print("  {s} = {s}\n", .{ kv.key, kv.value });
    }

    var watcher: etcd.Watcher = try .init(init.gpa, io, endpoint, .{
        .target = .{ .prefix = prefix },
    });
    defer watcher.deinit();

    const watch_id = blk: {
        var created = (try watcher.next()) orelse return error.WatchClosed;
        defer created.deinit();
        if (!created.value.created) return error.WatchNotCreated;
        print("watch {d} created at revision {d}\n", .{
            created.value.watch_id,
            created.value.header.?.revision,
        });
        break :blk created.value.watch_id;
    };

    var update = try client.put(.{ .key = prefix ++ "greeting", .value = "universe" }, .{});
    defer update.deinit();
    _ = try update.tryValue();

    {
        var event = (try watcher.next()) orelse return error.WatchClosed;
        defer event.deinit();
        for (event.value.events) |entry| {
            print("event {s} {s} = {s} at revision {d}\n", .{
                @tagName(entry.type),
                entry.kv.key,
                entry.kv.value,
                entry.kv.mod_revision,
            });
        }
    }

    var deleted = try client.deleteRange(.{
        .key = bounds.key,
        .range_end = bounds.range_end,
    }, .{});
    defer deleted.deinit();
    const deleted_value = try deleted.tryValue();
    print("deleted {d} keys\n", .{deleted_value.deleted});

    try watcher.cancel(watch_id);
    // The delete above and the cancel can both be waiting. Drain them, and
    // free each response, until the canceled response ends the iteration.
    while (try watcher.next()) |response| {
        var discarded = response;
        discarded.deinit();
    }
    print("watch canceled\n", .{});
}
