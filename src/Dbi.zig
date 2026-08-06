//! Database - wrappers around mdb_dbi*

const root = @import("root.zig");
const std = @import("std");
const c = @import("c");

const Cursor = @import("Cursor.zig");
const Env = @import("Env.zig");
const Txn = @import("Txn.zig");
const Val = @import("Val.zig");

const utils = @import("utils.zig");
const log = std.log.scoped(.lmdb);

const Dbi = @This();

handle: c.MDB_dbi,

pub fn init(txn: Txn.ReadWrite, name: ?[:0]const u8, flags: InitFlags) !Dbi {
    switch (txn.base.status) {
        .open => {},
        .committed => return error.Committed,
        .aborted => return error.Aborted,
        else => unreachable,
    }

    var flags_int: c_uint = 0;
    inline for (std.meta.fields(InitFlags)) |flag| {
        if (@field(flags, flag.name))
            flags_int |= @field(root.all_flags, flag.name);
    }

    var dbi: c.MDB_dbi = undefined;

    switch (root.errno(
        c.mdb_dbi_open(txn.base.inner, if (name) |n| n.ptr else null, flags_int, &dbi),
    )) {
        .SUCCESS => return .{ .handle = dbi },
        .NOTFOUND => return error.NotFound,
        .DBS_FULL => return error.TooMany, // maxdbs reached
        .BAD_VALSIZE => return error.BadValsize, // unsupported size of key/db name/data, or wrong DUPFIXED size
        .INCOMPATIBLE => return error.Incompatible, // database was dropped and opened with different flags

        else => |rc| return root.lmdbUnhandledError(@src(), rc),
    }
}

pub fn stats(this: Dbi, txn: Txn) c.MDB_stat {
    var stat: c.MDB_stat = undefined;
    if (c.mdb_stat(txn.inner, this.handle, &stat) != @intFromEnum(root.E.SUCCESS)) unreachable;
    return stat;
}

/// Deletes all data contained in database, doesnt free handle
/// Returns true on success
pub fn emptyContents(this: Dbi, txn: Txn) bool {
    return c.mdb_drop(txn.inner, this.handle, 0) == @intFromEnum(root.E.SUCCESS);
}

/// This is unnecessary! Use with care!
///
/// This call is not mutex protected. Handles should only be closed
/// by a single thread, and only if no other threads are going to reference
/// the database handle or one of its cursors any further. Do not close a
/// handle if an existing transaction has modified its database. Doing so
/// can cause misbehavior from database corruption to errors like
/// MDB_BAD_VALSIZE (since the DB name is gone).
///
/// Closing a database handle is not necessary, but lets mdb_dbi_open() reuse
/// the handle value. Usually it's better to set a bigger mdb_env_set_maxdbs(),
/// unless that value would be large.
pub fn freeHandle_CodeSmell(this: *Dbi, env: Env) void {
    c.mdb_dbi_close(env.inner, this.handle);
    this.* = undefined;
}

/// Please see `free_handle()` for important documentation about how to use this function.
///
/// Deletes database from the environment and frees handle
/// Returns true on success
pub fn emptyFreeHandle_CodeSmell(this: *Dbi, txn: Txn) bool {
    if (c.mdb_drop(txn.inner, this.handle, 1) == @intFromEnum(root.E.SUCCESS)) {
        this.* = undefined;
        return true;
    }

    return false;
}

pub fn cmpKey(this: Dbi, txn: Txn, a: []const u8, b: []const u8) std.math.Order {
    var c_a: Val = .from_const(a);
    var c_b: Val = .from_const(b);

    const ordering_int = c.mdb_cmp(txn.inner, this.handle, c_a.alias(), c_b.alias());

    if (ordering_int < 0) return .lt;
    if (ordering_int > 0) return .gt;
    return .eq;
}

pub fn cmpData(this: Dbi, txn: Txn, a: []const u8, b: []const u8) std.math.Order {
    var c_a: Val = .from_const(a);
    var c_b: Val = .from_const(b);

    const ordering_int = c.mdb_dcmp(txn.inner, this.handle, c_a.alias(), c_b.alias());

    if (ordering_int < 0) return .lt;
    if (ordering_int > 0) return .gt;
    return .eq;
}

pub const InitFlags = packed struct {
    reverse_key: bool = false,
    dup_sort: bool = false,
    integer_key: bool = false,
    dup_fixed: bool = false,
    integer_dup: bool = false,
    reverse_dup: bool = false,
    create: bool = false,
};
