# etcd-zig

A Zig client library for [etcd](https://etcd.io/), built on HTTP/2 (h2c) with [nghttp2](https://nghttp2.org/).

**Work in progress** — API will change.

## What works

- **Unary gRPC calls** — `Put`, `Range`, and other request/response RPCs via `grpc.unaryCall`
- **Server-streaming gRPC** — `Watch` via `grpc.openServerStream` / `GrpcStreamReader`
- **WatchIterator** — high-level watch API with automatic reconnect and revision tracking
- **Protobuf** — generated Zig bindings for etcd's protobuf types via [zig-protobuf](https://github.com/Arwalk/zig-protobuf)

## Requirements

- Zig 0.15.2
- A running etcd instance (h2c, no TLS)

## Building

```
zig build
```

## Usage

```zig
const etcd = @import("etcd");

// Unary call (Put)
var conn: etcd.H2Connection = try .connect(allocator, "localhost", 2379);
defer conn.deinit();
try conn.performHandshake();

const resp = try etcd.grpc.unaryCall(allocator, &conn, "/etcdserverpb.KV/Put", req_bytes);
defer resp.deinit(allocator);

// Watch with automatic reconnect
var iter: etcd.WatchIterator = .init(allocator, "localhost", 2379, .{
    .target = .{ .prefix = "my-prefix/" },
    .start_revision = 0,
});
defer iter.deinit();

while (iter.next()) |resp| {
    var r = resp;
    defer r.deinit(allocator);
    for (resp.events.items) |event| {
        // handle event
    }
}
```

## Watch targets

`WatchIterator` accepts a `KeyRange`:

| Variant | Description |
|---|---|
| `.single = "key"` | Watch a single key |
| `.prefix = "pfx/"` | Watch all keys with a prefix |
| `.range = .{ .start = "a", .end = "z" }` | Watch a half-open range [a, z) |
| `.all` | Watch the entire keyspace |

## Running the demo

Start etcd, then:

```
zig build run
```

In another terminal:

```
etcdctl put hello world
etcdctl put hello universe
etcdctl del hello
```

## Tests

```
zig build test
```

## Project structure

```
src/
  root.zig           — Library entry point and public exports
  grpc.zig           — gRPC framing (unary + server-streaming)
  h2_connection.zig  — HTTP/2 connection (nghttp2 wrapper)
  watch.zig          — WatchIterator, KeyRange, PrefixEnd
  main.zig           — Demo executable
  proto/             — Generated protobuf bindings
proto/               — .proto source files
```
