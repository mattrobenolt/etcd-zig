//! The mvccpb messages etcd returns in a Range or a Watch response.
//!
//! The field numbers are from etcd's api/mvccpb/kv.proto. There is no
//! generated code: `protobuf` reads the struct through this table.

const pb = @import("protobuf");

pub const KeyValue = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .key = pb.bytes(1),
        .create_revision = pb.field(2),
        .mod_revision = pb.field(3),
        .version = pb.field(4),
        .value = pb.bytes(5),
        .lease = pb.field(6),
    };

    key: []const u8 = "",
    create_revision: i64 = 0,
    mod_revision: i64 = 0,
    version: i64 = 0,
    value: []const u8 = "",
    lease: i64 = 0,
};

pub const Event = struct {
    pub const Type = enum(i32) {
        put = 0,
        delete = 1,
    };

    pub const protobuf: pb.Descriptor(@This()) = .{
        .type = pb.field(1),
        .kv = pb.field(2),
        .prev_kv = pb.field(3),
    };

    type: Type = .put,
    kv: KeyValue = .{},
    prev_kv: ?KeyValue = null,
};
