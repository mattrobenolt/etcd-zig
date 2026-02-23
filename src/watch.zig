const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const sleep = std.Thread.sleep;
const testing = std.testing;

const grpc = @import("grpc.zig");
const H2Connection = @import("h2_connection.zig").H2Connection;
const etcdserverpb = @import("proto/etcdserverpb.pb.zig");

const log = std.log.scoped(.watch);

/// The result of computing a prefix end bound.
const PrefixEnd = union(enum) {
    /// An allocated upper bound. Caller must free with the same allocator.
    owned: []u8,
    /// The prefix is all 0xFF — range covers everything from prefix onward.
    unbounded,

    fn deinit(self: PrefixEnd, gpa: Allocator) void {
        switch (self) {
            .owned => |s| gpa.free(s),
            .unbounded => {},
        }
    }

    /// Returns the wire representation for etcd range_end.
    /// `.unbounded` maps to "" (etcd convention for no upper bound).
    fn toSlice(self: PrefixEnd) []const u8 {
        return switch (self) {
            .owned => |s| s,
            .unbounded => "",
        };
    }
};

/// Computes the end of the key range that covers all keys with
/// the given prefix. For example, "foo" yields "fop".
/// If the prefix consists entirely of 0xFF bytes, returns
/// `.unbounded` (all keys from prefix to end of keyspace).
fn prefixEnd(gpa: Allocator, prefix: []const u8) Allocator.Error!PrefixEnd {
    // Find the last byte that is not 0xFF.
    var i: usize = prefix.len;
    while (i > 0) {
        i -= 1;
        if (prefix[i] < 0xFF) {
            const end = try gpa.dupe(u8, prefix[0 .. i + 1]);
            end[i] += 1;
            return .{ .owned = end };
        }
    }
    return .unbounded;
}

/// The key range to watch.
pub const KeyRange = union(enum) {
    /// Watch a single key.
    single: []const u8,
    /// Watch all keys with a given prefix.
    prefix: []const u8,
    /// Watch an explicit half-open range [start, end).
    range: struct { start: []const u8, end: []const u8 },
    /// Watch the entire keyspace.
    all,

    /// Returns the (key, range_end) pair for the etcd WatchCreateRequest.
    const WireRange = struct { key: []const u8, range_end: []const u8, prefix_end: ?PrefixEnd };

    fn toWireRange(self: KeyRange, gpa: Allocator) Allocator.Error!WireRange {
        return switch (self) {
            .single => |k| .{ .key = k, .range_end = "", .prefix_end = null },
            .prefix => |p| {
                const pe = try prefixEnd(gpa, p);
                return .{ .key = p, .range_end = pe.toSlice(), .prefix_end = pe };
            },
            .range => |r| .{ .key = r.start, .range_end = r.end, .prefix_end = null },
            .all => .{ .key = "\x00", .range_end = "\x00", .prefix_end = null },
        };
    }
};

/// Iterator that encapsulates the full watch lifecycle: connecting, handshaking,
/// opening a gRPC server stream for etcd Watch, decoding responses, and
/// transparently reconnecting on transient failures.
///
/// Usage:
///   var iter: WatchIterator = .init(gpa, "localhost", 2379, .{ .target = .all });
///   defer iter.deinit();
///   while (iter.next()) |resp| {
///       defer resp.deinit(gpa);
///       // process resp.events, etc.
///   }
///   // null means the watch was canceled by the server (non-recoverable).
pub const WatchIterator = struct {
    // Config (immutable after init)
    gpa: Allocator,
    host: []const u8,
    port: u16,
    target: KeyRange,
    prev_kv: bool,

    // Mutable state
    last_revision: i64,
    conn: ?H2Connection,
    stream: ?grpc.GrpcStreamReader,

    pub const Options = struct {
        target: KeyRange = .all,
        start_revision: i64 = 0,
        prev_kv: bool = false,
    };

    pub fn init(gpa: Allocator, host: []const u8, port: u16, opts: Options) WatchIterator {
        return .{
            .gpa = gpa,
            .host = host,
            .port = port,
            .target = opts.target,
            .prev_kv = opts.prev_kv,
            .last_revision = opts.start_revision,
            .conn = null,
            .stream = null,
        };
    }

    pub fn deinit(self: *WatchIterator) void {
        self.teardown();
        self.* = undefined;
    }

    /// Returns the next WatchResponse from the stream. Blocks until a response
    /// is available, reconnecting transparently on transient errors. Returns
    /// null only if the watch is canceled by the server (non-recoverable).
    /// The returned WatchResponse is owned by the caller; call
    /// `resp.deinit(allocator)` when done.
    pub fn next(self: *WatchIterator) ?etcdserverpb.WatchResponse {
        while (true) {
            // Ensure we have a live connection and stream.
            if (self.stream == null) {
                self.establish() catch |err| {
                    log.warn("establish failed: {s}, retrying in 1s...", .{@errorName(err)});
                    self.teardown();
                    sleep(1 * std.time.ns_per_s);
                    continue;
                };
            }

            // Read the next gRPC frame from the stream.
            const msg_bytes = self.stream.?.next(self.gpa) catch |err| {
                log.warn("stream read error: {s}, reconnecting...", .{@errorName(err)});
                self.teardown();
                sleep(1 * std.time.ns_per_s);
                continue;
            };

            if (msg_bytes) |bytes| {
                defer self.gpa.free(bytes);

                var reader: Io.Reader = .fixed(bytes);
                var resp = etcdserverpb.WatchResponse.decode(&reader, self.gpa) catch |err| {
                    log.warn("decode error: {s}, skipping frame", .{@errorName(err)});
                    continue;
                };

                // Track the latest revision for reconnect.
                if (resp.header) |hdr| {
                    if (hdr.revision > self.last_revision) {
                        self.last_revision = hdr.revision;
                    }
                }

                // A server-side cancel is non-recoverable; signal end of iteration.
                if (resp.canceled) {
                    log.info("canceled by server (compact_revision={d})", .{
                        resp.compact_revision,
                    });
                    resp.deinit(self.gpa);
                    return null;
                }

                return resp;
            } else {
                // stream.next() returned null => stream closed by server.
                log.warn("stream closed, reconnecting...", .{});
                self.teardown();
                sleep(1 * std.time.ns_per_s);
                continue;
            }
        }
    }

    // -- internal helpers --

    fn teardown(self: *WatchIterator) void {
        if (self.stream) |*s| {
            s.deinit();
            self.stream = null;
        }
        if (self.conn) |*cn| {
            cn.deinit();
            self.conn = null;
        }
    }

    fn establish(self: *WatchIterator) !void {
        assert(self.conn == null);
        assert(self.stream == null);

        // Connect and handshake.
        var conn: H2Connection = try .connect(self.gpa, self.host, self.port);
        errdefer conn.deinit();
        try conn.performHandshake();

        self.conn = conn;

        // Build the WatchCreateRequest starting from last_revision + 1 so
        // we resume without missing events.
        const start_rev = if (self.last_revision > 0) self.last_revision + 1 else 0;

        const wire = try self.target.toWireRange(self.gpa);
        defer if (wire.prefix_end) |pe| pe.deinit(self.gpa);

        const req_bytes = try encodePb(etcdserverpb.WatchRequest, self.gpa, .{
            .create_request = .{
                .key = wire.key,
                .range_end = wire.range_end,
                .start_revision = start_rev,
                .prev_kv = self.prev_kv,
            },
        });
        defer self.gpa.free(req_bytes);

        // openServerStream returns by value; we store it into self.stream,
        // then call activate() to fix the nghttp2 stream_user_data pointer.
        self.stream = try grpc.openServerStream(
            self.gpa,
            &self.conn.?,
            "/etcdserverpb.Watch/Watch",
            req_bytes,
        );
        self.stream.?.activate();

        log.info("established (start_revision={d})", .{start_rev});
    }
};

fn encodePb(comptime T: type, allocator: Allocator, msg: T) ![]const u8 {
    var buf: Io.Writer.Allocating = .init(allocator);
    defer buf.deinit();
    try msg.encode(&buf.writer, allocator);
    return buf.toOwnedSlice();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "prefixEnd: normal prefix" {
    const gpa = testing.allocator;
    const pe = try prefixEnd(gpa, "foo");
    defer pe.deinit(gpa);
    try testing.expectEqualSlices(u8, "fop", pe.owned);
}

test "prefixEnd: single byte" {
    const gpa = testing.allocator;
    const pe = try prefixEnd(gpa, "a");
    defer pe.deinit(gpa);
    try testing.expectEqualSlices(u8, "b", pe.owned);
}

test "prefixEnd: trailing 0xFF bytes are stripped" {
    const gpa = testing.allocator;
    const pe = try prefixEnd(gpa, "a\xFF\xFF");
    defer pe.deinit(gpa);
    try testing.expectEqualSlices(u8, "b", pe.owned);
}

test "prefixEnd: all 0xFF yields unbounded" {
    const pe = try prefixEnd(testing.allocator, "\xFF\xFF\xFF");
    try testing.expect(pe == .unbounded);
}
