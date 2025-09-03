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

pub fn init(txn: Txn, name: ?[:0]const u8, flags: InitFlags) !Dbi {
    switch (txn.status) {
        .open => {},
        .committed => return error.Committed,
        .aborted => return error.Aborted,
        else => unreachable,
    }

    if (utils.DEBUG) {
        if (txn.debug.access != .read_write and flags.create) {
            log.err("Dbi.init(..., .{{ .create = true }}) called with read_only Txn", .{});
            return error.BadAccess;
        }
    }

    var flags_int: c_uint = 0;
    inline for (std.meta.fields(InitFlags)) |flag| {
        if (@field(flags, flag.name))
            flags_int |= @field(root.all_flags, flag.name);
    }

    var dbi: c.MDB_dbi = undefined;

    return switch (root.errno(
        c.mdb_dbi_open(txn.inner, if (name) |n| n.ptr else null, flags_int, &dbi),
    )) {
        .SUCCESS => .{ .handle = dbi },
        .NOTFOUND => error.NotFound,
        .DBS_FULL => error.TooMany, // maxdbs reached
        .BAD_VALSIZE => error.BadValsize, // unsupported size of key/db name/data, or wrong DUPFIXED size
        .INCOMPATIBLE => error.Incompatible, // database was dropped and opened with different flags

        else => |rc| {
            try root.lmdbUnhandledError(@src(), rc);
            unreachable;
        },
    };
}

pub fn get_stats(this: Dbi, txn: Txn) c.MDB_stat {
    var stat: c.MDB_stat = undefined;
    if (c.mdb_stat(txn.inner, this.handle, &stat) != @intFromEnum(root.E.SUCCESS)) unreachable;
    return stat;
}

pub fn get(this: Dbi, txn: Txn, key: []const u8) !?[]u8 {
    var c_key: Val = .from_const(key);
    var c_out: Val = .empty;

    return switch (root.errno(
        c.mdb_get(txn.inner, this.handle, c_key.alias(), c_out.alias()),
    )) {
        .SUCCESS => c_out.unalias(),
        .NOTFOUND => null,

        _ => |rc| {
            try root.lmdbUnhandledError(@src(), rc);
            unreachable;
        },

        else => |rc| switch (@as(std.posix.E, @enumFromInt(@intFromEnum(rc)))) {
            .INVAL => return error.Invalid,

            else => {
                try root.lmdbUnhandledError(@src(), rc);
                unreachable;
            },
        },
    };
}

pub inline fn get_const(this: Dbi, txn: Txn, key: []const u8) !?[]const u8 {
    return this.get(txn, key);
}

pub fn put(this: Dbi, txn: Txn, key: []const u8, data: []const u8) !void {
    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    return this.put_impl(txn, c_key.alias(), c_data.alias(), 0);
}

/// `put()` with `no_dup_data` flag
/// supported for DUPSORT databases
pub fn put_no_clobber(this: Dbi, txn: Txn, key: []const u8, data: []const u8) !void {
    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    return this.put_impl(txn, c_key.alias(), c_data.alias(), root.all_flags.no_dup_data);
}

/// `put()` with `no_overwrite` flag
pub fn put_get(this: Dbi, txn: Txn, key: []const u8, data: []const u8) ![]u8 {
    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    this.put_impl(txn, c_key.alias(), c_data.alias(), root.all_flags.no_overwrite) catch |e| switch (e) {
        error.AlreadyExists => {},
        else => return e,
    };
    return c_data.unalias();
}

/// `put()` with `append` flag
/// keys must be sorted
pub fn put_append(this: Dbi, txn: Txn, key: []const u8, data: []const u8) !void {
    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    return this.put_impl(txn, c_key.alias(), c_data.alias(), root.all_flags.append) catch |e| switch (e) {
        error.AlreadyExists => error.Unsorted,
        else => e,
    };
}

/// `put()` with `append_dup` flag
/// supported for DUPSORT databases
pub fn put_append_dup(this: Dbi, txn: Txn, key: []const u8, data: []const u8) !void {
    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    return this.put_impl(txn, c_key.alias(), c_data.alias(), root.all_flags.append_dup) catch |e| switch (e) {
        error.AlreadyExists => error.Unsorted,
        else => e,
    };
}

/// `put()` with `reserve` flag
/// NOT supported for DUPSORT databased
pub fn put_reserve(this: Dbi, txn: Txn, key: []const u8, size: usize) ![]u8 {
    var c_key: Val = .from_const(key);
    var c_data: Val = .of_size(size);

    try this.put_impl(txn, c_key.alias(), c_data.alias(), root.all_flags.reserve);
    return c_data.unalias();
}

fn put_impl(this: Dbi, txn: Txn, c_key: ?*c.MDB_val, c_data: ?*c.MDB_val, flags: c_uint) !void {
    switch (txn.status) {
        .open => {},
        .committed => return error.Committed,
        .aborted => return error.Aborted,
        else => unreachable,
    }

    return switch (root.errno(
        c.mdb_put(txn.inner, this.handle, c_key, c_data, flags),
    )) {
        .SUCCESS => {},
        .MAP_FULL => error.MapFull,
        .TXN_FULL => error.TxnFull,
        .KEYEXIST => error.AlreadyExists,

        _ => |rc| {
            try root.lmdbUnhandledError(@src(), rc);
            unreachable;
        },

        else => |rc| switch (@as(std.posix.E, @enumFromInt(@intFromEnum(rc)))) {
            .ACCES => error.ReadOnly,
            .INVAL => error.Invalid,

            else => {
                try root.lmdbUnhandledError(@src(), rc);
                unreachable;
            },
        },
    };
}

/// returns true if deleted, false if not found, error otherwise
pub fn del(this: Dbi, txn: Txn, key: []const u8, data: ?[]const u8) !bool {
    switch (txn.status) {
        .open => {},
        .committed => return error.Committed,
        .aborted => return error.Aborted,
        else => unreachable,
    }

    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    return switch (root.errno(c.mdb_del(txn.inner, this.handle, c_key.alias(), c_data.alias()))) {
        .SUCCESS => true,
        .NOTFOUND => false,

        _ => |rc| {
            try root.lmdbUnhandledError(@src(), rc);
            unreachable;
        },

        else => |rc| switch (@as(std.posix.E, @enumFromInt(@intFromEnum(rc)))) {
            .ACCES => error.ReadOnly,
            .INVAL => error.Invalid,

            else => {
                try root.lmdbUnhandledError(@src(), rc);
                unreachable;
            },
        },
    };
}

/// Deletes all data contained in database, doesnt free handle
/// Returns true on success
pub fn empty_contents(this: Dbi, txn: Txn) bool {
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
pub fn free_handle(this: *Dbi, env: Env) void {
    c.mdb_dbi_close(env.inner, this.handle);
    this.* = undefined;
}

/// Please see `free_handle()` for important documentation about how to use this function.
///
/// Deletes database from the environment and frees handle
/// Returns true on success
pub fn delete_and_free(this: *Dbi, txn: Txn) bool {
    if (c.mdb_drop(txn.inner, this.handle, 1) == @intFromEnum(root.E.SUCCESS)) {
        this.* = undefined;
        return true;
    }

    return false;
}

pub fn cmp_keys(this: Dbi, txn: Txn, a: []const u8, b: []const u8) std.math.Order {
    var c_a: Val = .from_const(a);
    var c_b: Val = .from_const(b);

    const ordering_int = c.mdb_cmp(txn.inner, this.handle, c_a.alias(), c_b.alias());

    if (ordering_int < 0) return .lt;
    if (ordering_int > 0) return .gt;
    return .eq;
}

pub fn cmp_data(this: Dbi, txn: Txn, a: []const u8, b: []const u8) std.math.Order {
    var c_a: Val = .from_const(a);
    var c_b: Val = .from_const(b);

    const ordering_int = c.mdb_dcmp(txn.inner, this.handle, c_a.alias(), c_b.alias());

    if (ordering_int < 0) return .lt;
    if (ordering_int > 0) return .gt;
    return .eq;
}

pub inline fn cursor(dbi: Dbi, src: std.builtin.SourceLocation, txn: *const Txn) !Cursor {
    return Cursor.init(src, dbi, txn);
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
