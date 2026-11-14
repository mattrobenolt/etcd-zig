//! The etcdserverpb messages this client uses, as plain Zig structs.
//!
//! The field numbers come from etcd's api/etcdserverpb/rpc.proto. A message
//! here is a row of the wire format: an absent field decodes to its default,
//! and a repeated field is an array. Txn is not modeled because its
//! request/response operations form a recursive message graph.

const pb = @import("protobuf");

const mvccpb = @import("mvccpb.zig");

pub const ResponseHeader = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .cluster_id = pb.field(1),
        .member_id = pb.field(2),
        .revision = pb.field(3),
        .raft_term = pb.field(4),
    };

    cluster_id: u64 = 0,
    member_id: u64 = 0,
    revision: i64 = 0,
    raft_term: u64 = 0,
};

// -- KV ---------------------------------------------------------------------

pub const RangeRequest = struct {
    pub const SortOrder = enum(i32) {
        none = 0,
        ascend = 1,
        descend = 2,
    };

    pub const SortTarget = enum(i32) {
        key = 0,
        version = 1,
        create = 2,
        mod = 3,
        value = 4,
    };

    pub const protobuf: pb.Descriptor(@This()) = .{
        .key = pb.bytes(1),
        .range_end = pb.bytes(2),
        .limit = pb.field(3),
        .revision = pb.field(4),
        .sort_order = pb.field(5),
        .sort_target = pb.field(6),
        .serializable = pb.field(7),
        .keys_only = pb.field(8),
        .count_only = pb.field(9),
        .min_mod_revision = pb.field(10),
        .max_mod_revision = pb.field(11),
        .min_create_revision = pb.field(12),
        .max_create_revision = pb.field(13),
    };

    key: []const u8 = "",
    range_end: []const u8 = "",
    limit: i64 = 0,
    revision: i64 = 0,
    sort_order: SortOrder = .none,
    sort_target: SortTarget = .key,
    serializable: bool = false,
    keys_only: bool = false,
    count_only: bool = false,
    min_mod_revision: i64 = 0,
    max_mod_revision: i64 = 0,
    min_create_revision: i64 = 0,
    max_create_revision: i64 = 0,
};

pub const RangeResponse = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .header = pb.field(1),
        .kvs = pb.field(2),
        .more = pb.field(3),
        .count = pb.field(4),
    };

    header: ?ResponseHeader = null,
    kvs: []const mvccpb.KeyValue = &.{},
    more: bool = false,
    count: i64 = 0,
};

pub const PutRequest = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .key = pb.bytes(1),
        .value = pb.bytes(2),
        .lease = pb.field(3),
        .prev_kv = pb.field(4),
        .ignore_value = pb.field(5),
        .ignore_lease = pb.field(6),
    };

    key: []const u8 = "",
    value: []const u8 = "",
    lease: i64 = 0,
    prev_kv: bool = false,
    ignore_value: bool = false,
    ignore_lease: bool = false,
};

pub const PutResponse = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .header = pb.field(1),
        .prev_kv = pb.field(2),
    };

    header: ?ResponseHeader = null,
    prev_kv: ?mvccpb.KeyValue = null,
};

pub const DeleteRangeRequest = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .key = pb.bytes(1),
        .range_end = pb.bytes(2),
        .prev_kv = pb.field(3),
    };

    key: []const u8 = "",
    range_end: []const u8 = "",
    prev_kv: bool = false,
};

pub const DeleteRangeResponse = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .header = pb.field(1),
        .deleted = pb.field(2),
        .prev_kvs = pb.field(3),
    };

    header: ?ResponseHeader = null,
    deleted: i64 = 0,
    prev_kvs: []const mvccpb.KeyValue = &.{},
};

pub const CompactionRequest = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .revision = pb.field(1),
        .physical = pb.field(2),
    };

    revision: i64 = 0,
    physical: bool = false,
};

pub const CompactionResponse = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .header = pb.field(1),
    };

    header: ?ResponseHeader = null,
};

// -- Watch ------------------------------------------------------------------

pub const WatchCreateRequest = struct {
    pub const FilterType = enum(i32) {
        noput = 0,
        nodelete = 1,
    };

    pub const protobuf: pb.Descriptor(@This()) = .{
        .key = pb.bytes(1),
        .range_end = pb.bytes(2),
        .start_revision = pb.field(3),
        .progress_notify = pb.field(4),
        .filters = pb.field(5),
        .prev_kv = pb.field(6),
        .watch_id = pb.field(7),
        .fragment = pb.field(8),
    };

    key: []const u8 = "",
    range_end: []const u8 = "",
    start_revision: i64 = 0,
    progress_notify: bool = false,
    filters: []const FilterType = &.{},
    prev_kv: bool = false,
    watch_id: i64 = 0,
    fragment: bool = false,
};

pub const WatchCancelRequest = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .watch_id = pb.field(1),
    };

    watch_id: i64 = 0,
};

pub const WatchProgressRequest = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{};
};

pub const WatchRequestUnion = union(enum) {
    pub const protobuf: pb.Variants(@This()) = .{
        .create_request = pb.field(1),
        .cancel_request = pb.field(2),
        .progress_request = pb.field(3),
    };

    create_request: WatchCreateRequest,
    cancel_request: WatchCancelRequest,
    progress_request: WatchProgressRequest,
};

pub const WatchRequest = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .request = pb.oneof(),
    };

    request: ?WatchRequestUnion = null,
};

pub const WatchResponse = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .header = pb.field(1),
        .watch_id = pb.field(2),
        .created = pb.field(3),
        .canceled = pb.field(4),
        .compact_revision = pb.field(5),
        .cancel_reason = pb.string(6),
        .fragment = pb.field(7),
        .events = pb.field(11),
    };

    header: ?ResponseHeader = null,
    watch_id: i64 = 0,
    created: bool = false,
    canceled: bool = false,
    compact_revision: i64 = 0,
    cancel_reason: []const u8 = "",
    fragment: bool = false,
    events: []const mvccpb.Event = &.{},
};

// -- Lease ------------------------------------------------------------------

pub const LeaseGrantRequest = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .ttl = pb.field(1),
        .id = pb.field(2),
    };

    ttl: i64 = 0,
    id: i64 = 0,
};

pub const LeaseGrantResponse = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .header = pb.field(1),
        .id = pb.field(2),
        .ttl = pb.field(3),
        .error_message = pb.string(4),
    };

    header: ?ResponseHeader = null,
    id: i64 = 0,
    ttl: i64 = 0,
    error_message: []const u8 = "",
};

pub const LeaseRevokeRequest = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .id = pb.field(1),
    };

    id: i64 = 0,
};

pub const LeaseRevokeResponse = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .header = pb.field(1),
    };

    header: ?ResponseHeader = null,
};

pub const LeaseTimeToLiveRequest = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .id = pb.field(1),
        .keys = pb.field(2),
    };

    id: i64 = 0,
    keys: bool = false,
};

pub const LeaseTimeToLiveResponse = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .header = pb.field(1),
        .id = pb.field(2),
        .ttl = pb.field(3),
        .granted_ttl = pb.field(4),
        .keys = pb.bytes(5),
    };

    header: ?ResponseHeader = null,
    id: i64 = 0,
    ttl: i64 = 0,
    granted_ttl: i64 = 0,
    keys: []const []const u8 = &.{},
};

// -- Auth -------------------------------------------------------------------

pub const AuthenticateRequest = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .name = pb.string(1),
        .password = pb.string(2),
    };

    name: []const u8 = "",
    password: []const u8 = "",
};

pub const AuthenticateResponse = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .header = pb.field(1),
        .token = pb.string(2),
    };

    header: ?ResponseHeader = null,
    token: []const u8 = "",
};

// -- Maintenance ------------------------------------------------------------

pub const StatusRequest = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{};
};

pub const StatusResponse = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .header = pb.field(1),
        .version = pb.string(2),
        .db_size = pb.field(3),
        .leader = pb.field(4),
        .raft_index = pb.field(5),
        .raft_term = pb.field(6),
        .raft_applied_index = pb.field(7),
        .errors = pb.string(8),
        .db_size_in_use = pb.field(9),
        .is_learner = pb.field(10),
    };

    header: ?ResponseHeader = null,
    version: []const u8 = "",
    db_size: i64 = 0,
    leader: u64 = 0,
    raft_index: u64 = 0,
    raft_term: u64 = 0,
    raft_applied_index: u64 = 0,
    errors: []const []const u8 = &.{},
    db_size_in_use: i64 = 0,
    is_learner: bool = false,
};

// -- Cluster ----------------------------------------------------------------

pub const Member = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .id = pb.field(1),
        .name = pb.string(2),
        .peer_urls = pb.string(3),
        .client_urls = pb.string(4),
        .is_learner = pb.field(5),
    };

    id: u64 = 0,
    name: []const u8 = "",
    peer_urls: []const []const u8 = &.{},
    client_urls: []const []const u8 = &.{},
    is_learner: bool = false,
};

pub const MemberListRequest = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{};
};

pub const MemberListResponse = struct {
    pub const protobuf: pb.Descriptor(@This()) = .{
        .header = pb.field(1),
        .members = pb.field(2),
    };

    header: ?ResponseHeader = null,
    members: []const Member = &.{},
};
