//! An etcd endpoint: `host:port`, with an IPv4 or bracketed IPv6 literal
//! taking the direct path and a hostname going through the resolver.

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const math = std.math;
const mem = std.mem;
const testing = std.testing;

const posix = @import("stdx").posix;

/// TCP keepalive policy applied to dialed etcd sockets so a silently vanished
/// peer (for example, a killed host without FIN/RST) fails the socket instead
/// of leaving a blocking watch read hung forever.
pub const KeepAlive = struct {
    idle_sec: u32 = 5,
    interval_sec: u32 = 1,
    count: u32 = 3,

    pub const default: KeepAlive = .{};

    pub fn validate(self: KeepAlive) error{InvalidTcpKeepAlive}!void {
        if (self.idle_sec == 0 or self.idle_sec > math.maxInt(i32)) {
            return error.InvalidTcpKeepAlive;
        }
        if (self.interval_sec == 0 or self.interval_sec > math.maxInt(i32)) {
            return error.InvalidTcpKeepAlive;
        }
        if (self.count == 0 or self.count > math.maxInt(i32)) {
            return error.InvalidTcpKeepAlive;
        }
    }

    pub fn apply(self: KeepAlive, fd: std.posix.fd_t) !void {
        try self.validate();
        try posix.setsockopt(fd, std.posix.SOL.SOCKET, std.posix.SO.KEEPALIVE, 1);
        const idle_opt = if (builtin.os.tag == .macos)
            std.posix.TCP.KEEPALIVE
        else
            std.posix.TCP.KEEPIDLE;
        try posix.setsockopt(
            fd,
            std.posix.IPPROTO.TCP,
            idle_opt,
            @as(i32, @intCast(self.idle_sec)),
        );
        try posix.setsockopt(
            fd,
            std.posix.IPPROTO.TCP,
            std.posix.TCP.KEEPINTVL,
            @as(i32, @intCast(self.interval_sec)),
        );
        try posix.setsockopt(
            fd,
            std.posix.IPPROTO.TCP,
            std.posix.TCP.KEEPCNT,
            @as(i32, @intCast(self.count)),
        );
    }
};

pub const Endpoint = struct {
    host: []const u8,
    port: u16,
    /// The text a gRPC call sends as `:authority`.
    authority: []const u8,

    pub const Error = error{
        InvalidEndpoint,
        MissingPort,
        InvalidPort,
    };

    /// Parse `host:port`. Brackets are required around an IPv6 literal.
    /// The returned slices borrow `text`.
    pub fn parse(text: []const u8) Error!Endpoint {
        if (Io.net.IpAddress.parseLiteral(text)) |address| {
            const port = switch (address) {
                .ip4 => |ip| ip.port,
                .ip6 => |ip| ip.port,
            };
            if (port == 0) return error.MissingPort;
            return .{ .host = hostPart(text), .port = port, .authority = text };
        } else |_| {}

        const colon = mem.findScalarLast(u8, text, ':') orelse
            return error.InvalidEndpoint;
        if (colon == 0) return error.InvalidEndpoint;
        if (colon + 1 == text.len) return error.InvalidEndpoint;
        if (text[0] == '[') return error.InvalidEndpoint;

        const port = std.fmt.parseInt(u16, text[colon + 1 ..], 10) catch
            return error.InvalidPort;
        if (port == 0) return error.InvalidPort;
        return .{
            .host = text[0..colon],
            .port = port,
            .authority = text,
        };
    }

    fn hostPart(text: []const u8) []const u8 {
        if (text[0] == '[') {
            const end = mem.findScalar(u8, text, ']') orelse return text;
            return text[1..end];
        }
        if (mem.findScalar(u8, text, ':')) |colon| return text[0..colon];
        return text;
    }

    /// Dial the endpoint with the default TCP keepalive policy. A literal
    /// address skips the resolver. The socket gets TCP_NODELAY: a gRPC call
    /// writes its head and body as separate small frames, and Nagle would hold
    /// the second one for a delayed ACK.
    pub fn connect(self: Endpoint, io: Io) !Io.net.Stream {
        return self.connectWithKeepAlive(io, .default);
    }

    /// Dial the endpoint with an explicit TCP keepalive policy, or null to
    /// leave keepalive off.
    pub fn connectWithKeepAlive(
        self: Endpoint,
        io: Io,
        keepalive: ?KeepAlive,
    ) !Io.net.Stream {
        const stream = if (Io.net.IpAddress.parseLiteral(self.authority)) |address|
            try address.connect(io, .{ .mode = .stream })
        else |_| blk: {
            const host_name: Io.net.HostName = try .init(self.host);
            break :blk try host_name.connect(io, self.port, .{ .mode = .stream });
        };
        errdefer stream.close(io);

        try posix.setsockopt(
            stream.socket.handle,
            std.posix.IPPROTO.TCP,
            std.posix.TCP.NODELAY,
            1,
        );
        if (keepalive) |ka| try ka.apply(stream.socket.handle);
        return stream;
    }
};

test "IPv4 endpoint" {
    const endpoint = try Endpoint.parse("127.0.0.1:2379");
    try testing.expectEqualStrings("127.0.0.1", endpoint.host);
    try testing.expectEqual(@as(u16, 2379), endpoint.port);
    try testing.expectEqualStrings("127.0.0.1:2379", endpoint.authority);
}

test "bracketed IPv6 endpoint" {
    const endpoint = try Endpoint.parse("[::1]:2379");
    try testing.expectEqual(@as(u16, 2379), endpoint.port);
    try testing.expectEqualStrings("[::1]:2379", endpoint.authority);
}

test "hostname endpoint" {
    const endpoint = try Endpoint.parse("etcd.example.com:2379");
    try testing.expectEqualStrings("etcd.example.com", endpoint.host);
    try testing.expectEqual(@as(u16, 2379), endpoint.port);
}

test "a port is required" {
    try testing.expectError(error.MissingPort, Endpoint.parse("127.0.0.1"));
    try testing.expectError(error.MissingPort, Endpoint.parse("[::1]"));
    try testing.expectError(error.InvalidEndpoint, Endpoint.parse("localhost"));
    try testing.expectError(error.InvalidEndpoint, Endpoint.parse(":2379"));
    try testing.expectError(error.InvalidEndpoint, Endpoint.parse("localhost:"));
    try testing.expectError(error.InvalidPort, Endpoint.parse("localhost:notaport"));
    try testing.expectError(error.InvalidPort, Endpoint.parse("localhost:0"));
}

test "connect sets TCP_NODELAY" {
    var address: Io.net.IpAddress = try .parse("127.0.0.1", 0);
    var listener = try address.listen(testing.io, .{ .reuse_address = true });
    defer listener.deinit(testing.io);

    var endpoint_buffer: [32]u8 = undefined;
    const text = try mem.print(&endpoint_buffer, "127.0.0.1:{d}", .{
        listener.socket.address.getPort(),
    });
    const endpoint = try Endpoint.parse(text);

    const stream = try endpoint.connect(testing.io);
    defer stream.close(testing.io);

    const value = try posix.getsockopt(
        stream.socket.handle,
        std.posix.IPPROTO.TCP,
        std.posix.TCP.NODELAY,
    );
    try testing.expect(value != 0);
    const keepalive = try posix.getsockopt(
        stream.socket.handle,
        std.posix.SOL.SOCKET,
        std.posix.SO.KEEPALIVE,
    );
    try testing.expect(keepalive != 0);
    const idle_opt = if (builtin.os.tag == .macos)
        std.posix.TCP.KEEPALIVE
    else
        std.posix.TCP.KEEPIDLE;
    try testing.expectEqual(@as(i32, KeepAlive.default.idle_sec), try posix.getsockopt(
        stream.socket.handle,
        std.posix.IPPROTO.TCP,
        idle_opt,
    ));
    try testing.expectEqual(@as(i32, KeepAlive.default.interval_sec), try posix.getsockopt(
        stream.socket.handle,
        std.posix.IPPROTO.TCP,
        std.posix.TCP.KEEPINTVL,
    ));
    try testing.expectEqual(@as(i32, KeepAlive.default.count), try posix.getsockopt(
        stream.socket.handle,
        std.posix.IPPROTO.TCP,
        std.posix.TCP.KEEPCNT,
    ));
}

test "KeepAlive.validate rejects zero and out-of-range values" {
    try KeepAlive.default.validate();
    try testing.expectError(error.InvalidTcpKeepAlive, (KeepAlive{ .idle_sec = 0 }).validate());
    try testing.expectError(error.InvalidTcpKeepAlive, (KeepAlive{ .interval_sec = 0 }).validate());
    try testing.expectError(error.InvalidTcpKeepAlive, (KeepAlive{ .count = 0 }).validate());
    try testing.expectError(
        error.InvalidTcpKeepAlive,
        (KeepAlive{ .idle_sec = math.maxInt(u32) }).validate(),
    );
}
