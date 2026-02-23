const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const etcd = @import("etcd");
const etcdserverpb = etcd.etcdserverpb;
const mvccpb = etcd.mvccpb;

const log = std.log.scoped(.main);

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // -- One-time setup: Put + Range to establish a baseline revision --

    var last_revision: i64 = 0;
    {
        var conn: etcd.H2Connection = try .connect(allocator, "localhost", 2379);
        defer conn.deinit();
        try conn.performHandshake();

        // Put key="hello" value="world"
        {
            const req_bytes = try encodePb(etcdserverpb.PutRequest, allocator, .{
                .key = "hello",
                .value = "world",
            });
            defer allocator.free(req_bytes);

            const response = try etcd.grpc.unaryCall(
                allocator,
                &conn,
                "/etcdserverpb.KV/Put",
                req_bytes,
            );
            defer response.deinit(allocator);

            if (response.grpc_status == 0 and response.data.len > 0) {
                var reader: Io.Reader = .fixed(response.data);
                var put_resp: etcdserverpb.PutResponse = try .decode(&reader, allocator);
                defer put_resp.deinit(allocator);

                if (put_resp.header) |hdr| {
                    last_revision = hdr.revision;
                    log.info("put: revision={d}", .{hdr.revision});
                }
            }
        }

        // Range to confirm
        {
            const req_bytes = try encodePb(etcdserverpb.RangeRequest, allocator, .{
                .key = "hello",
            });
            defer allocator.free(req_bytes);

            const response = try etcd.grpc.unaryCall(
                allocator,
                &conn,
                "/etcdserverpb.KV/Range",
                req_bytes,
            );
            defer response.deinit(allocator);

            if (response.grpc_status == 0 and response.data.len > 0) {
                var reader: Io.Reader = .fixed(response.data);
                var range_resp: etcdserverpb.RangeResponse = try .decode(&reader, allocator);
                defer range_resp.deinit(allocator);

                for (range_resp.kvs.items) |kv| {
                    log.info("range: key={s} value={s}", .{ kv.key, kv.value });
                }
            }
        }
    }

    // -- Watch using WatchIterator --

    var iter: etcd.WatchIterator = .init(allocator, "localhost", 2379, .{
        .target = .all,
        .start_revision = last_revision,
    });
    defer iter.deinit();

    while (iter.next()) |resp| {
        var r = resp;
        defer r.deinit(allocator);

        if (resp.created) {
            log.info("watch: created (watch_id={d})", .{resp.watch_id});
            continue;
        }

        for (resp.events.items) |event| {
            const type_str: []const u8 = switch (event.type) {
                .PUT => "PUT",
                .DELETE => "DELETE",
                _ => "UNKNOWN",
            };
            if (event.kv) |kv| {
                log.info("watch: {s} key={s} value={s} mod_revision={d}", .{
                    type_str, kv.key, kv.value, kv.mod_revision,
                });
            }
        }
    }

    log.info("watch ended (canceled by server)", .{});
}

fn encodePb(comptime T: type, allocator: Allocator, msg: T) ![]const u8 {
    var buf: Io.Writer.Allocating = .init(allocator);
    defer buf.deinit();
    try msg.encode(&buf.writer, allocator);
    return buf.toOwnedSlice();
}
