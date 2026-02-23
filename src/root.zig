pub const grpc = @import("grpc.zig");
pub const H2Connection = @import("h2_connection.zig").H2Connection;
pub const etcdserverpb = @import("proto/etcdserverpb.pb.zig");
pub const mvccpb = @import("proto/mvccpb.pb.zig");
pub const watch = @import("watch.zig");
pub const WatchIterator = watch.WatchIterator;
pub const KeyRange = watch.KeyRange;
pub const PrefixEnd = watch.PrefixEnd;

pub const c = @cImport({
    @cInclude("nghttp2/nghttp2.h");
});

test "nghttp2 linked" {
    const info = c.nghttp2_version(0);
    const std = @import("std");
    try std.testing.expect(info != null);
}
