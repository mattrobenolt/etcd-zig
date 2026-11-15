//! A watch that survives reconnects.
//!
//! A watch is a gRPC server stream that stays open for the life of the
//! process. This module owns one connection for that stream, creates the
//! watch, tracks the last revision, and reconnects with `start_revision`
//! set past what was already delivered. A server-side cancel ends the
//! iteration; every transport failure is retried.

const std = @import("std");
const Io = std.Io;
const mem = std.mem;
const crypto = std.crypto;
const Allocator = mem.Allocator;
const testing = std.testing;

const grpc = @import("grpc");
const pb = @import("protobuf");
const time = @import("stdx").time;

const auth = @import("auth.zig");
const Client = @import("client.zig").Client;
const Endpoint = @import("endpoint.zig").Endpoint;
const etcdserverpb = @import("etcdserverpb.zig");
const KeyRange = @import("range.zig").KeyRange;
const watch_service = @import("client.zig").watch_service;

const log = std.log.scoped(.etcd_watch);

const Session = struct {
    client: Client,
    calls: [1]grpc.Call = undefined,
};

const Next = union(enum) {
    response: pb.Parsed(etcdserverpb.WatchResponse),
    closed,
};

pub const Event = union(enum) {
    response: pb.Parsed(etcdserverpb.WatchResponse),
    /// A transport failure or a clean stream end. The caller decides when
    /// to retry; the connection is already torn down.
    reconnecting: anyerror,
    stopped,
};

pub const Options = struct {
    target: KeyRange = .all,
    /// Zero watches live changes. A reconnect resumes from the last revision
    /// already delivered plus one, so this value only applies to the first
    /// connection.
    start_revision: i64 = 0,
    prev_kv: bool = false,
    progress_notify: bool = false,
    /// When set, every connection authenticates with these credentials.
    credentials: ?auth.Credentials = null,
    /// A token to use when credentials are not set. The watcher copies it.
    token: ?[]const u8 = null,
    retry_delay: time.Duration = .fromSecondsComptime(1),
    /// The unary reply space for the authentication calls. Small is enough.
    reply_capacity: usize = 64 * 1024,
};

const OwnedTarget = struct {
    buffer: []u8,
    range: KeyRange,

    fn init(gpa: Allocator, target: KeyRange) !OwnedTarget {
        const buffer = try gpa.alloc(u8, target.storageLen());
        errdefer gpa.free(buffer);

        var offset: usize = 0;
        const range: KeyRange = switch (target) {
            .single => |key| .{ .single = copyInto(buffer, &offset, key) },
            .prefix => |prefix| .{ .prefix = copyInto(buffer, &offset, prefix) },
            .range => |explicit| .{ .range = .{
                .start = copyInto(buffer, &offset, explicit.start),
                .end = copyInto(buffer, &offset, explicit.end),
            } },
            .all => .all,
        };
        return .{ .buffer = buffer, .range = range };
    }
};

fn copyInto(buffer: []u8, offset: *usize, bytes: []const u8) []const u8 {
    const start = offset.*;
    @memcpy(buffer[start..][0..bytes.len], bytes);
    offset.* = start + bytes.len;
    return buffer[start..offset.*];
}

pub const Watcher = struct {
    gpa: Allocator,
    io: Io,
    endpoint: []u8,
    target: KeyRange,
    target_buffer: []u8,
    start_revision: i64,
    prev_kv: bool,
    progress_notify: bool,
    credentials: ?auth.OwnedCredentials,
    token: ?[]u8,
    retry_delay: time.Duration,
    reply_capacity: usize,
    last_revision: i64,
    session: ?*Session = null,
    stream: ?grpc.Stream = null,
    stopped: std.atomic.Value(bool) = .init(false),
    /// Serializes the session pointer between the watch thread and stop().
    session_lock: Io.Mutex = .init,

    pub fn init(
        gpa: Allocator,
        // ziglint-ignore: Z023 -- the allocator stays first to mirror Client.init.
        io: Io,
        endpoint: []const u8,
        options: Options,
    ) !Watcher {
        _ = try Endpoint.parse(endpoint);

        const endpoint_copy = try gpa.dupe(u8, endpoint);
        errdefer gpa.free(endpoint_copy);

        const target = try OwnedTarget.init(gpa, options.target);
        errdefer gpa.free(target.buffer);

        var credentials: ?auth.OwnedCredentials = null;
        if (options.credentials) |value| credentials = try .init(gpa, value);
        errdefer if (credentials) |*value| value.deinit(gpa);

        var token: ?[]u8 = null;
        if (options.token) |value| token = try gpa.dupe(u8, value);
        errdefer if (token) |value| {
            crypto.secureZero(u8, value);
            gpa.free(value);
        };

        return .{
            .gpa = gpa,
            .io = io,
            .endpoint = endpoint_copy,
            .target = target.range,
            .target_buffer = target.buffer,
            .start_revision = options.start_revision,
            .prev_kv = options.prev_kv,
            .progress_notify = options.progress_notify,
            .credentials = credentials,
            .token = token,
            .retry_delay = options.retry_delay,
            .reply_capacity = options.reply_capacity,
            .last_revision = 0,
        };
    }

    pub fn deinit(self: *Watcher) void {
        self.teardown();
        self.gpa.free(self.endpoint);
        self.gpa.free(self.target_buffer);
        if (self.credentials) |*credentials| credentials.deinit(self.gpa);
        if (self.token) |token| {
            crypto.secureZero(u8, token);
            self.gpa.free(token);
        }
        self.* = undefined;
    }

    /// The next watch response, or null when the server canceled the watch.
    /// A transport failure reconnects after `retry_delay`; only a fatal error
    /// (out of memory, bad credentials) returns an error.
    pub fn next(self: *Watcher) !?pb.Parsed(etcdserverpb.WatchResponse) {
        while (true) {
            switch (try self.nextEvent()) {
                .response => |response| {
                    if (response.value.canceled) {
                        log.info("watch canceled at compact revision {d}: {s}", .{
                            response.value.compact_revision,
                            response.value.cancel_reason,
                        });
                        var canceled = response;
                        canceled.deinit();
                        self.teardown();
                        return null;
                    }
                    return response;
                },
                .reconnecting => |err| {
                    log.warn("watch transport ended: {s}; retrying", .{@errorName(err)});
                    self.sleepRetry();
                },
                .stopped => return error.Stopped,
            }
        }
    }

    /// The next watch event. Unlike `next`, a transport failure returns
    /// `.reconnecting` without sleeping; the caller owns the retry policy.
    /// A server-side cancel is returned as a response with `canceled` set so
    /// the caller can read `compact_revision` and `cancel_reason`.
    pub fn nextEvent(self: *Watcher) !Event {
        if (self.stopped.load(.acquire)) return .stopped;

        if (self.stream == null) {
            self.establish() catch |err| {
                if (self.stopped.load(.acquire)) return .stopped;
                if (isFatal(err)) return err;
                self.teardown();
                return .{ .reconnecting = err };
            };
            if (self.stopped.load(.acquire)) {
                self.teardown();
                return .stopped;
            }
        }

        const outcome = self.read() catch |err| {
            if (self.stopped.load(.acquire)) return .stopped;
            self.teardown();
            return .{ .reconnecting = err };
        };

        return switch (outcome) {
            .response => |response| .{ .response = response },
            .closed => blk: {
                self.teardown();
                if (self.stopped.load(.acquire)) break :blk .stopped;
                break :blk .{ .reconnecting = error.StreamClosed };
            },
        };
    }

    /// Stop the watcher. Sets a flag and shuts the open socket down so a
    /// blocked read returns. The watch thread observes the flag and returns
    /// `.stopped` from `nextEvent`.
    pub fn stop(self: *Watcher, io: Io) void {
        self.stopped.store(true, .release);
        self.session_lock.lockUncancelable(io);
        defer self.session_lock.unlock(io);
        if (self.session) |session| {
            if (session.client.transport.socket) |*socket| {
                socket.shutdown(io, .both) catch {};
            }
        }
    }

    /// Send a WatchCancelRequest. The confirmation arrives from `next` as a
    /// response with `canceled` set, and then `next` returns null.
    pub fn cancel(self: *Watcher, watch_id: i64) !void {
        const stream = if (self.stream) |*stream| stream else return error.NotConnected;
        return self.send(stream, .{ .request = .{ .cancel_request = .{
            .watch_id = watch_id,
        } } });
    }

    /// The highest revision seen in a response header, or the starting
    /// revision before the first response.
    pub fn lastRevision(self: *const Watcher) i64 {
        return if (self.last_revision != 0) self.last_revision else self.start_revision;
    }

    /// Send a WatchProgressRequest. The server answers with a response whose
    /// header carries its current revision.
    pub fn requestProgress(self: *Watcher) !void {
        const stream = if (self.stream) |*stream| stream else return error.NotConnected;
        return self.send(stream, .{ .request = .{ .progress_request = .{} } });
    }

    fn read(self: *Watcher) !Next {
        const stream = if (self.stream) |*stream| stream else return error.NotConnected;
        while (true) {
            if (try stream.nextMessage()) |message| {
                var reader: Io.Reader = .fixed(message.bytes);
                const parsed = pb.decode(
                    etcdserverpb.WatchResponse,
                    self.gpa,
                    &reader,
                    .{},
                ) catch |err| {
                    log.warn("watch decode failed: {s}; skipping message", .{@errorName(err)});
                    continue;
                };

                self.observe(parsed.value);
                return .{ .response = parsed };
            }

            if (stream.result()) |result| {
                if (result.code == .ok) return .closed;
                log.warn("watch stream ended: {s} {s}", .{
                    @tagName(result.code),
                    result.message,
                });
                // A token failure only heals with credentials. Without them,
                // reconnecting would replay the same rejection forever.
                if (auth.needsReauth(result.message) and self.credentials == null) {
                    return error.AuthenticationRequired;
                }
                return .closed;
            }

            if (!try stream.advance()) return .closed;
        }
    }

    fn establish(self: *Watcher) !void {
        const session = try self.gpa.create(Session);
        errdefer self.gpa.destroy(session);

        try session.client.init(self.gpa, self.io, self.endpoint, .{
            .reply_capacity = self.reply_capacity,
        });
        errdefer session.client.deinit();

        if (self.credentials) |*credentials| {
            const result = session.client.authenticate(.{
                .username = credentials.username,
                .password = credentials.password,
            }, .{}) catch |err| {
                log.warn("watch authentication failed: {s}", .{@errorName(err)});
                return error.AuthenticationFailed;
            };
            if (result == .disabled) log.debug("watch: authentication is disabled", .{});
        } else if (self.token) |token| {
            try session.client.setToken(token);
        }

        session.calls[0] = try session.client.open(watch_service, "Watch", .{});
        errdefer session.calls[0].deinit();

        var stream = session.client.stream(&session.calls[0], session.calls[0..]);
        try self.sendCreate(&stream);

        self.session_lock.lockUncancelable(self.io);
        self.session = session;
        self.stream = stream;
        self.session_lock.unlock(self.io);
        log.info("watch established from revision {d}", .{self.lastRevision()});
    }

    fn sendCreate(self: *Watcher, stream: *grpc.Stream) !void {
        const scratch = try self.gpa.alloc(u8, self.target.bufferLen());
        defer self.gpa.free(scratch);
        const bounds = try self.target.bounds(scratch);

        const start_revision = if (self.last_revision != 0)
            self.last_revision + 1
        else
            self.start_revision;

        return self.send(stream, .{ .request = .{ .create_request = .{
            .key = bounds.key,
            .range_end = bounds.range_end,
            .start_revision = start_revision,
            .prev_kv = self.prev_kv,
            .progress_notify = self.progress_notify,
        } } });
    }

    fn send(self: *Watcher, stream: *grpc.Stream, request: etcdserverpb.WatchRequest) !void {
        var out: Io.Writer.Allocating = .init(self.gpa);
        defer out.deinit();
        try pb.encode(etcdserverpb.WatchRequest, request, .{}, &out.writer);

        while (!try stream.send(out.written())) {
            if (!try stream.advance()) return error.WatchSendBlocked;
        }
    }

    fn observe(self: *Watcher, response: etcdserverpb.WatchResponse) void {
        // The create confirmation carries the store's current revision and
        // no events. Adopting it would resume past events that were never
        // delivered. Progress notifications are safe to adopt.
        if (response.created) return;
        const header = response.header orelse return;
        if (header.revision > self.last_revision) self.last_revision = header.revision;
    }

    fn teardown(self: *Watcher) void {
        self.session_lock.lockUncancelable(self.io);
        defer self.session_lock.unlock(self.io);
        if (self.stream) |*stream| {
            stream.deinit();
            self.stream = null;
        }
        if (self.session) |session| {
            session.client.deinit();
            self.gpa.destroy(session);
            self.session = null;
        }
    }

    fn sleepRetry(self: *Watcher) void {
        const delay: Io.Duration = .fromNanoseconds(self.retry_delay.nanoseconds);
        self.io.sleep(delay, .awake) catch |err|
            log.debug("watch retry sleep failed: {s}", .{@errorName(err)});
    }
};

fn isFatal(err: anyerror) bool {
    return switch (err) {
        error.OutOfMemory,
        error.AuthenticationFailed,
        error.AuthenticationRequired,
        => true,
        else => false,
    };
}

test "observe ignores the create response revision" {
    var watcher: Watcher = undefined;
    watcher.last_revision = 7;
    watcher.observe(.{ .created = true, .header = .{ .revision = 100 } });
    try std.testing.expectEqual(@as(i64, 7), watcher.last_revision);
    watcher.observe(.{ .created = false, .header = .{ .revision = 8 } });
    try std.testing.expectEqual(@as(i64, 8), watcher.last_revision);
}

test "Watcher owns a copy of its target" {
    var prefix = "app/".*;
    var watcher: Watcher = try .init(testing.allocator, testing.io, "127.0.0.1:2379", .{
        .target = .{ .prefix = &prefix },
    });
    defer watcher.deinit();

    @memset(&prefix, 'x');
    try testing.expectEqualStrings("app/", watcher.target.prefix);
}
