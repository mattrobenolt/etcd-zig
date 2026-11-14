# Benchmarks

`just bench` starts a throwaway etcd and runs `bench/bench.zig` against it.
The harness measures sequential Put, sequential Range, and the time from a Put
response to the matching watch event. It overwrites one key, so the backend
stays small.

## Pre-rewrite comparison

The old tree is commit `4becbee`: nghttp2, generated protobuf, Zig 0.16. The
new tree is this one: baskingspot/http, grpc, protobuf, Zig 0.17.1. Both ran
against etcd 3.7.3 with `--unsafe-no-fsync`, with etcd on core 2 and the
client on core 3. Values are the median of five rounds in ReleaseFast. Put and
Range used 2000 iterations; watch used 1000.

| Operation | Old | Old + TCP_NODELAY | New | New, no deadline |
| --- | ---: | ---: | ---: | ---: |
| Put | 42.1 ms | 52.7 us | 55.7 us | 54.1 us |
| Range | 42.1 ms | 44.7 us | 46.8 us | 45.4 us |
| Watch event | 6.37 us | 6.37 us | 3.80 us | - |

The old column is the old client without a socket option. The middle column
sets TCP_NODELAY on the old client's socket from the benchmark. Nagle held the
second small frame of each call for a delayed ACK, which cost about 42 ms per
call. The new endpoint layer sets TCP_NODELAY for every socket, so the fixed
number is the first column.

With the socket option matched, the new client is about 4 percent slower on
Put and 5 percent slower on Range. The watch event path is about 40 percent
faster. The new binary is statically linked; the old binary needs glibc.
Stripped sizes are 639,880 bytes and 634,264 bytes, within 1 percent. A cold
build with an empty cache took 83.4 s for the new tree and 49.8 s for the old
tree. The new tree compiles the Zig HTTP/2 and gRPC stack; the old tree
compiles nghttp2 and the protobuf generator.

## Where the Range gap went

A raw gRPC call and the full typed call differ by less than 0.5 us, so protobuf
decode is not the cause. Two things in the stack were:

- grpc flushed the queued request head before it moved the request body into
  the connection. A call sent HEADERS and DATA as two `sendmsg` calls.
  nghttp2 queued both frames and wrote them together. grpc `a3f8b59` queues
  the body behind the head, so the new call now sends one write. This change
  moved Range from 48.3 us to 46.8 us.
- The new call has a 5 s deadline. The old client had no deadline at all.
  http enforces the deadline by waiting on the socket with a poll, which adds
  one `ppoll` before each read. With `.timeout = .none` the poll disappears:
  Range falls from 46.8 us to 45.4 us, and Put falls from 55.7 us to 54.1 us.
  Range is then 0.6 us from the old client; Put is 1.4 us away.

Reproduce the new column with `just bench`. The old comparison needs the old
commit in a separate worktree.
