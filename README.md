# etcd-zig

An etcd client for Zig 0.17. It uses
[`baskingspot/grpc`](https://github.com/baskingspot/grpc) for gRPC over HTTP/2
and [`baskingspot/protobuf`](https://github.com/baskingspot/protobuf) for the
message codec. The library has no C dependency and no generated code.

**Work in progress.** The API is unstable.

## What works

- KV calls: Put, Range, DeleteRange, and Compact.
- Lease calls: LeaseGrant, LeaseRevoke, and LeaseTimeToLive.
- Watch: create, cancel, progress, and automatic reconnect.
- Authentication: Auth.Authenticate, token metadata, and rejection
  classification.
- Maintenance Status and MemberList.
- etcdserverpb and mvccpb messages as plain Zig structs.

Not implemented:

- Txn, because its request and response operations form a recursive message
  graph.
- Lease keepalive sessions.
- TLS. The transport is plaintext h2c.
- The cluster, election, and lock services.

## Requirements

- Zig 0.17.1.
- An etcd instance on plaintext h2c.

## Build

```
nix develop
zig build
```

`nix develop` provides Zig 0.17, `zls`, `ziglint`, `just`, and `etcd` from nixpkgs.

## Usage

```zig
const etcd = @import("etcd");

var client: etcd.Client = undefined;
try client.init(gpa, io, "127.0.0.1:2379", .{});
defer client.deinit();

var put = try client.put(.{ .key = "hello", .value = "world" }, .{});
defer put.deinit();
const response = try put.tryValue();
std.debug.print("revision {d}\n", .{response.header.?.revision});
```

`Client.init` wires internal pointers to the client's own address. Initialize
the client where it lives. Do not move it after `init`.

A `Reply` owns an arena. Call `deinit` when the reply is no longer needed. The
decoded value borrows this arena, so keep the reply alive while the value is in
use. `Reply.tryValue` returns an error when the gRPC status is not OK.

### Ranges

```zig
var scratch: [64]u8 = undefined;
const target: etcd.KeyRange = .{ .prefix = "app/" };
const bounds = try target.bounds(&scratch);

var reply = try client.range(.{
    .key = bounds.key,
    .range_end = bounds.range_end,
}, .{});
defer reply.deinit();
```

| Variant | Meaning |
| --- | --- |
| `.single = "key"` | One key. |
| `.prefix = "pfx/"` | Every key with the prefix. |
| `.range = .{ .start = "a", .end = "z" }` | The half-open range `[a, z)`. |
| `.all` | The whole keyspace. |

### Watch

```zig
var watcher = try etcd.Watcher.init(gpa, io, "127.0.0.1:2379", .{
    .target = .{ .prefix = "app/" },
});
defer watcher.deinit();

while (try watcher.next()) |response| {
    var parsed = response;
    defer parsed.deinit();
    for (parsed.value.events) |event| {
        // event.type is .put or .delete
    }
}
```

`Watcher.next` tracks the last revision. It reconnects after a transport
failure and starts the new watch at `last_revision + 1`. A server-side cancel
ends the loop: `next` returns null.

Call `watcher.cancel(watch_id)` to stop a watch. The server sends a
confirmation, and the next `next` call returns null.

### Authentication

```zig
const result = try client.authenticate(.{
    .username = "root",
    .password = "secret",
}, .{});
switch (result) {
    .authenticated => {},
    .disabled => {},
}
```

The client sends its token as the `token` metadata field on every call, except
Auth.Authenticate. `etcd.auth.needsReauth(reply.status.message)` reports
whether a rejection can succeed after a new Authenticate call.

`Watcher` accepts credentials and authenticates on every new connection. It
also accepts a fixed token.

### Metadata and deadlines

`CallOptions` carries a deadline and extra gRPC metadata. The default deadline
is five seconds. Set `.timeout = .none` for no deadline. Metadata names must be
lowercase.

### Streaming

`Client.open` opens a stream and adds the token to its metadata.
`Client.stream` returns the byte-stream facade over the call. The watch
implementation uses this interface.

## Tests

```
just test
just test-integration
just demo
```

`just test` runs the unit tests through the testingx runner. `just
test-integration` starts two throwaway etcd instances, one plaintext and one
with authentication enabled, and runs the tests against both. `just demo`
runs the example in `src/main.zig`. `just bench` starts a throwaway etcd and
measures sequential Put, Range, and watch-event latency. `bench/README.md`
has the comparison against the pre-rewrite tree.

## Dependencies

The lockstep of `http`, `grpc`, `protobuf`, and `stdx` matters. The package
pins `grpc` and `protobuf` to untagged commits, because the released protobuf
tag does not carry oneof support. `http` and `stdx` arrive through `grpc` at
the same revisions. `testingx` is a lazy dependency and is absent from a
consumer's fetch.

## Project structure

```
src/
  root.zig          — public namespace
  client.zig        — Client, Reply, typed calls
  watch.zig         — reconnecting Watcher
  auth.zig          — credentials and rejection classification
  endpoint.zig      — host:port parsing and dialing
  range.zig         — KeyRange and prefix ends
  etcdserverpb.zig  — etcd messages as plain structs
  mvccpb.zig        — key, value, and event messages
  integration.zig   — tests against a live etcd
  main.zig          — demo
```
