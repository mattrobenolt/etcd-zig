//! A small benchmark against a live etcd: sequential Put, Range, and the
//! time from a Put response to the matching watch event.
//!
//! `zig build bench -Doptimize=ReleaseFast -- 127.0.0.1:2379 all 5000`
//! or `just bench`. The key is overwritten in place, so the backend stays
//! small. Start etcd with `--unsafe-no-fsync` to take the disk out of the
//! Put path.

const std = @import("std");
const Io = std.Io;
const mem = std.mem;
const Allocator = std.mem.Allocator;

const etcd = @import("etcd");

const default_key = "bench/key";

pub fn main(init: std.process.Init) !void {
    // A fast allocator, so the numbers describe the client and not the
    // debug allocator's bookkeeping.
    const gpa: Allocator = std.heap.smp_allocator;
    const io = init.io;
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    if (args.len < 4) return error.Usage;

    const endpoint = args[1];
    const mode = args[2];
    const iterations = try std.fmt.parseInt(u32, args[3], 10);

    var client: etcd.Client = undefined;
    try client.init(gpa, io, endpoint, .{});
    defer client.deinit();

    if (mem.eql(u8, mode, "all") or mem.eql(u8, mode, "put")) {
        try benchPut(io, &client, iterations);
    }
    if (mem.eql(u8, mode, "all") or mem.eql(u8, mode, "range")) {
        try benchRange(io, &client, iterations);
    }
    if (mem.eql(u8, mode, "all") or mem.eql(u8, mode, "watch")) {
        try benchWatch(gpa, io, endpoint, &client, iterations);
    }
}

fn benchPut(io: Io, client: *etcd.Client, iterations: u32) !void {
    try put(client, default_key, "seed");
    try warmup(client, "put", iterations);

    const start = now(io);
    var i: u32 = 0;
    while (i < iterations) : (i += 1) {
        try put(client, default_key, "value");
    }
    report("put", iterations, now(io) - start);
}

fn benchRange(io: Io, client: *etcd.Client, iterations: u32) !void {
    try put(client, default_key, "seed");
    try warmup(client, "range", iterations);

    const start = now(io);
    var i: u32 = 0;
    while (i < iterations) : (i += 1) {
        var reply = try client.range(.{ .key = default_key, .limit = 1 }, .{});
        defer reply.deinit();
        _ = try reply.tryValue();
    }
    report("range", iterations, now(io) - start);
}

fn benchWatch(
    gpa: Allocator,
    io: Io,
    endpoint: []const u8,
    client: *etcd.Client,
    iterations: u32,
) !void {
    var watcher = try etcd.Watcher.init(gpa, io, endpoint, .{
        .target = .{ .single = default_key },
    });
    defer watcher.deinit();

    {
        var created = (try watcher.next()) orelse return error.WatchClosed;
        defer created.deinit();
        if (!created.value.created) return error.WatchNotCreated;
    }

    const warmup_count = @min(iterations / 10, 100);
    var i: u32 = 0;
    while (i < warmup_count) : (i += 1) {
        try put(client, default_key, "warm");
        var event = (try watcher.next()) orelse return error.WatchClosed;
        event.deinit();
    }

    // Measure after the Put response: the event may already be queued, so
    // this is the watch path, not the Put round trip.
    var elapsed: i96 = 0;
    i = 0;
    while (i < iterations) : (i += 1) {
        try put(client, default_key, "value");
        const after_put = now(io);
        var event = (try watcher.next()) orelse return error.WatchClosed;
        elapsed += now(io) - after_put;
        event.deinit();
    }
    report("watch", iterations, elapsed);
}

fn warmup(client: *etcd.Client, mode: []const u8, iterations: u32) !void {
    const count = @min(iterations / 10, 1000);
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        if (mem.eql(u8, mode, "put")) {
            try put(client, default_key, "warm");
        } else {
            var reply = try client.range(.{ .key = default_key, .limit = 1 }, .{});
            defer reply.deinit();
            _ = try reply.tryValue();
        }
    }
}

fn put(client: *etcd.Client, key: []const u8, value: []const u8) !void {
    var reply = try client.put(.{ .key = key, .value = value }, .{});
    defer reply.deinit();
    _ = try reply.tryValue();
}

fn now(io: Io) i96 {
    return Io.Clock.now(.awake, io).nanoseconds;
}

fn report(mode: []const u8, iterations: u32, elapsed_ns: i96) void {
    const count: f64 = @floatFromInt(iterations);
    const per_op: f64 = @as(f64, @floatFromInt(elapsed_ns)) / count;
    std.debug.print("{s} {d} ns_per_op={d:.1}\n", .{ mode, iterations, per_op });
}
