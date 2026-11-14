//! etcd key ranges: one key, a prefix, an explicit half-open range, or the
//! whole keyspace, converted to the `key` and `range_end` a request carries.

const std = @import("std");
const testing = std.testing;

/// The all-keys convention from etcd's client: key and range_end both NUL.
const all_keys = "\x00";

pub const Bounds = struct {
    key: []const u8,
    range_end: []const u8,
};

pub const KeyRange = union(enum) {
    /// One exact key.
    single: []const u8,
    /// Every key that starts with this prefix.
    prefix: []const u8,
    /// The half-open range [start, end).
    range: struct { start: []const u8, end: []const u8 },
    /// The whole keyspace.
    all,

    /// Bytes that `bounds` needs in its output buffer.
    pub fn bufferLen(self: KeyRange) usize {
        return switch (self) {
            .prefix => |prefix| prefix.len + 1,
            else => 0,
        };
    }

    /// Bytes that a copy of this range needs. For a caller that stores the
    /// range past the life of the input slices.
    pub fn storageLen(self: KeyRange) usize {
        return switch (self) {
            .single => |key| key.len,
            .prefix => |prefix| prefix.len,
            .range => |explicit| explicit.start.len + explicit.end.len,
            .all => 0,
        };
    }

    /// The request fields for this range. A prefix range writes its upper
    /// bound into `out`, so `out` must hold `bufferLen` bytes.
    pub fn bounds(self: KeyRange, out: []u8) !Bounds {
        return switch (self) {
            .single => |key| .{ .key = key, .range_end = "" },
            .prefix => |prefix| prefixBounds(prefix, out),
            .range => |explicit| .{ .key = explicit.start, .range_end = explicit.end },
            .all => .{ .key = all_keys, .range_end = all_keys },
        };
    }
};

/// The name the readme and the watcher use.
pub const Range = KeyRange;

fn prefixBounds(prefix: []const u8, out: []u8) !Bounds {
    // An empty prefix covers everything, and etcd spells that as NUL..NUL.
    if (prefix.len == 0) return .{ .key = all_keys, .range_end = all_keys };
    return .{
        .key = prefix,
        .range_end = try prefixEnd(out, prefix),
    };
}

/// The exclusive end of a prefix range: the prefix with its last byte below
/// 0xff incremented, and trailing 0xff bytes removed. All 0xff gives an empty
/// range_end, which etcd reads as "to the end of the keyspace".
pub fn prefixEnd(out: []u8, prefix: []const u8) ![]const u8 {
    if (out.len < prefix.len) return error.NoSpace;
    if (prefix.len == 0) return out[0..0];

    @memcpy(out[0..prefix.len], prefix);
    var index = prefix.len;
    while (index > 0) {
        index -= 1;
        if (out[index] == 0xff) continue;
        out[index] += 1;
        return out[0 .. index + 1];
    }
    return out[0..0];
}

test "prefixEnd: normal prefix" {
    var buffer: [8]u8 = undefined;
    try testing.expectEqualStrings("fop", try prefixEnd(&buffer, "foo"));
}

test "prefixEnd: trailing 0xff bytes are stripped" {
    var buffer: [8]u8 = undefined;
    try testing.expectEqualStrings("b", try prefixEnd(&buffer, "a\xff\xff"));
}

test "prefixEnd: all 0xff yields an unbounded range" {
    var buffer: [8]u8 = undefined;
    try testing.expectEqualStrings("", try prefixEnd(&buffer, "\xff\xff\xff"));
}

test "bounds: each range variant" {
    var buffer: [8]u8 = undefined;

    const single = try (KeyRange{ .single = "key" }).bounds(&buffer);
    try testing.expectEqualStrings("key", single.key);
    try testing.expectEqualStrings("", single.range_end);

    const prefix = try (KeyRange{ .prefix = "foo/" }).bounds(&buffer);
    try testing.expectEqualStrings("foo/", prefix.key);
    try testing.expectEqualStrings("foo0", prefix.range_end);

    const explicit = try (KeyRange{ .range = .{ .start = "a", .end = "z" } }).bounds(&buffer);
    try testing.expectEqualStrings("a", explicit.key);
    try testing.expectEqualStrings("z", explicit.range_end);

    const everything: KeyRange = .all;
    const all = try everything.bounds(&buffer);
    try testing.expectEqualStrings(all_keys, all.key);
    try testing.expectEqualStrings(all_keys, all.range_end);

    const every = try (KeyRange{ .prefix = "" }).bounds(&buffer);
    try testing.expectEqualStrings(all_keys, every.key);
    try testing.expectEqualStrings(all_keys, every.range_end);
}

test "prefixEnd reports a short buffer" {
    var buffer: [2]u8 = undefined;
    try testing.expectError(error.NoSpace, prefixEnd(&buffer, "long"));
}
