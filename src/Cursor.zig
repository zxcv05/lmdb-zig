//! Cursor -
//!
//! Read-only `Cursor`s (`Cursor`s owned by read-only `Txn`s) keep read database pages alive
//! for as long as the `Cursor` is alive. As such, it must be closed explicity with
//! `cursor.deinit()` regardless of the owning `Txn` closing.
//!
//! The above doesn't apply to read/write `Cursor`s. Those are closed when the owning txn closes.
//! You can also `deinit` them any time before the owning txn closes.

const root = @import("root.zig");
const std = @import("std");
const c = @import("c");

const Dbi = @import("Dbi.zig");
const Txn = @import("Txn.zig");
const Val = @import("Val.zig");

const utils = @import("utils.zig");
const log = std.log.scoped(.lmdb);

const Cursor = @This();

inner: *c.MDB_cursor,

/// Cursor inherits `txn`s access mode, must not outlive `txn`
pub fn init(dbi: Dbi, txn: Txn) !Cursor {
    var ptr: ?*c.MDB_cursor = null;

    switch (root.errno(
        c.mdb_cursor_open(txn.inner, dbi.handle, &ptr),
    )) {
        .SUCCESS => return .{ .inner = ptr.? },
        else => |rc| return root.lmdbUnhandledError(@src(), rc),
    }
}

/// Please see top-level comment for usage information
pub fn deinit(this: Cursor) void {
    c.mdb_cursor_close(this.inner);
}

/// Renew a read-only cursor
pub fn renew(this: Cursor, txn: Txn) void {
    if (c.mdb_cursor_renew(txn.inner, this.inner) != @intFromEnum(root.E.SUCCESS))
        unreachable;
}

/// Warning: Errors will be treated as "Not found" and will return null
/// Make sure if doing DUPSORT operations that the dbi is actually DUPSORT
pub fn get(this: Cursor, op: GetOp, key: ?[]const u8, data: ?[]const u8) ?Kv {
    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    const found = this.getImpl(@intFromEnum(op), c_key.alias(), c_data.alias());
    if (!found) return null;

    return .{
        c_key.unalias_maybe() orelse &.{},
        c_data.unalias_maybe() orelse &.{},
    };
}

/// Warning: Errors will be treated as "Not found" and will return null
pub fn getMultiple(
    this: Cursor,
    comptime T: type,
    op: GetMultipleOp,
    key: ?[]const u8,
) ?[]align(1) const T {
    var c_key: Val = .from_const(key);
    var c_data: Val = .empty;

    const found = this.getImpl(@intFromEnum(op), c_key.alias(), c_data.alias());
    if (!found) return null;

    const num_elems = @divExact(c_data.data.mv_size, @sizeOf(T));
    const typed_ptr: [*]align(1) const T = @ptrCast(c_data.data.mv_data);

    return typed_ptr[0..num_elems];
}

/// returns true if data found, false if not found or error occured
fn getImpl(this: Cursor, op: c_uint, c_key: ?*c.MDB_val, c_data: ?*c.MDB_val) bool {
    return c.mdb_cursor_get(this.inner, c_key, c_data, op) >= 0;
}

pub const iterator = GetIterator.init;

pub fn put(this: Cursor, key: []const u8, data: []const u8) !void {
    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    return this.putImpl(c_key.alias(), c_data.alias(), 0);
}

/// `put()` with `current` flag
/// key must match item at the current cursor position
pub fn putReplace(this: Cursor, key: []const u8, data: []const u8) !void {
    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    return this.putImpl(c_key.alias(), c_data.alias(), root.all_flags.current);
}

/// `put()` with `no_dup_data` flag
/// supported for DUPSORT databases
pub fn putNoClobber(this: Cursor, key: []const u8, data: []const u8) !void {
    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    return this.putImpl(c_key.alias(), c_data.alias(), root.all_flags.no_dup_data);
}

/// `put()` with `no_overwrite` flag
/// will put data (if not existing) or return it (if existing)
pub fn putGet(this: Cursor, key: []const u8, data: []const u8) ![]u8 {
    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    this.putImpl(c_key.alias(), c_data.alias(), root.all_flags.no_overwrite) catch |e| switch (e) {
        error.AlreadyExists => return c_data.unalias(),
        else => return e,
    };
}

/// `put()` with `append` flag
/// must be sorted
pub fn putAppend(this: Cursor, key: []const u8, data: []const u8) !void {
    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    this.putImpl(c_key.alias(), c_data.alias(), root.all_flags.append) catch |e| switch (e) {
        error.AlreadyExists => return error.Unsorted,
        else => return e,
    };
}

/// `put()` with `append_dup` flag
/// supported for DUPSORT databases
pub fn putAppendDup(this: Cursor, key: []const u8, data: []const u8) !void {
    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    this.putImpl(c_key.alias(), c_data.alias(), root.all_flags.append_dup) catch |e| switch (e) {
        error.AlreadyExists => return error.Unsorted,
        else => return e,
    };
}

/// `put()` with `reserve` flag
/// NOT supported for DUPSORT databases
pub fn putReserve(this: Cursor, key: []const u8, size: usize) ![]u8 {
    var c_key: Val = .from_const(key);
    var c_data: Val = .of_size(size);

    try this.putImpl(c_key.alias(), c_data.alias(), root.all_flags.reserve);
    return c_data.unalias();
}

/// `put()` with `multiple` flag
/// supported for DUPFIXED databases
pub fn putMultiple(this: Cursor, comptime T: type, key: []const u8, data: []const T) !usize {
    var data_actual: [2]c.MDB_val = .{
        .{ .mv_size = @sizeOf(T), .mv_data = @ptrCast(@constCast(data.ptr)) },
        .{ .mv_size = data.len, .mv_data = undefined },
    };

    var c_key: Val = .from_const(key);

    try this.putImpl(c_key.alias(), @ptrCast(&data_actual), root.all_flags.multiple);
    return data_actual[1].mv_size;
}

fn putImpl(this: Cursor, c_key: ?*c.MDB_val, c_data: ?*c.MDB_val, flags: c_uint) !void {
    return switch (root.errno(
        c.mdb_cursor_put(this.inner, c_key, c_data, flags),
    )) {
        .SUCCESS => {},
        .MAP_FULL => return error.MapFull,
        .TXN_FULL => return error.TxnFull,
        .KEYEXIST => return error.AlreadyExists,
        .INCOMPATIBLE => return error.Incompatible,

        else => |rc| return root.lmdbUnhandledError(@src(), rc),

        _ => |rc| switch (@as(std.posix.E, @enumFromInt(@intFromEnum(rc)))) {
            .ACCES => return error.ReadOnly,
            .INVAL => return error.Invalid,
            else => return root.lmdbUnhandledError(@src(), rc),
        },
    };
}

/// Delete current key/data pair
pub fn del(this: Cursor) !void {
    return this.delImpl(0);
}

/// Delete all items for current key
/// supported for DUPSORT databases
pub fn delAll(this: Cursor) !void {
    return this.delImpl(root.all_flags.no_dup_data);
}

fn delImpl(this: Cursor, flags: c_uint) !void {
    return switch (@as(std.posix.E, @enumFromInt(
        c.mdb_cursor_del(this.inner, flags),
    ))) {
        .SUCCESS => {},
        .ACCES => return error.ReadOnly,
        .INVAL => return error.Invalid,

        else => |rc| return root.lmdbUnhandledError(@src(), rc),
    };
}

/// errors treated as null
/// supported for DUPSORT databases
pub fn count(this: Cursor) ?usize {
    var ret: c_ulong = 0;
    if (c.mdb_cursor_count(this.inner, &ret) != @intFromEnum(root.E.SUCCESS)) return null;
    return @intCast(ret);
}

pub const Kv = struct { []const u8, []const u8 };

pub const GetOp = enum(u8) {
    first = c.MDB_FIRST,
    first_dup = c.MDB_FIRST_DUP,

    last = c.MDB_LAST,
    last_dup = c.MDB_LAST_DUP,

    get_both = c.MDB_GET_BOTH,
    get_both_range = c.MDB_GET_BOTH_RANGE,
    get_current = c.MDB_GET_CURRENT,

    set = c.MDB_SET,
    set_key = c.MDB_SET_KEY,
    set_range = c.MDB_SET_RANGE,

    next = c.MDB_NEXT,
    next_dup = c.MDB_NEXT_DUP,
    next_nodup = c.MDB_NEXT_NODUP,

    prev = c.MDB_PREV,
    prev_dup = c.MDB_PREV_DUP,
    prev_nodup = c.MDB_PREV_NODUP,
};

pub const GetMultipleOp = enum(u8) {
    get = c.MDB_GET_MULTIPLE,
    next = c.MDB_NEXT_MULTIPLE,
    prev = c.MDB_PREV_MULTIPLE,
};

pub const DelFlags = packed struct {
    no_dup_data: bool = false,
};

pub const GetIterator = struct {
    owner: Cursor,
    c_key: Val,
    c_data: Val,
    state: State,

    pub fn init(
        cursor: Cursor,
        init_op: GetOp,
        init_key: ?[]const u8,
        init_data: ?[]const u8,
        next_op: GetOp,
    ) GetIterator {
        var this: GetIterator = .{
            .owner = cursor,
            .c_key = .from_const(init_key),
            .c_data = .from_const(init_data),
            .state = .{ .next_op = next_op },
        };

        this.state.found = cursor.getImpl(
            @intFromEnum(init_op),
            this.c_key.alias(),
            this.c_data.alias(),
        );
        return this;
    }

    pub fn next(this: *GetIterator) ?Kv {
        if (!this.state.found) return null;

        if (this.state.skip) {
            this.state.skip = false;
        } else if (!this.owner.getImpl(
            @intFromEnum(this.state.next_op),
            this.c_key.alias(),
            this.c_data.alias(),
        )) {
            this.state.found = false;
            return null;
        }

        return .{
            this.c_key.unalias_maybe() orelse &.{},
            this.c_data.unalias_maybe() orelse &.{},
        };
    }

    pub const State = struct {
        next_op: GetOp,
        skip: bool = true,
        found: bool = false,
    };
};

/// Holds pages returned by `get` alive until `deinit`
pub const ReadOnly = struct {
    base: Cursor,

    pub fn deinit(this: ReadOnly) void {
        this.base.deinit();
    }

    pub inline fn renew(this: ReadOnly, txn: Txn) void {
        this.base.renew(txn);
    }

    pub inline fn get(this: ReadOnly, op: GetOp, k: ?[]const u8, d: ?[]const u8) ?Kv {
        return this.base.get(op, k, d);
    }

    pub inline fn getMultiple(
        this: ReadOnly,
        comptime T: type,
        op: GetMultipleOp,
        k: ?[]const u8,
    ) ?[]align(1) const T {
        return this.base.getMultiple(T, op, k);
    }

    pub inline fn iterator(
        this: ReadOnly,
        init_op: GetOp,
        k: ?[]const u8,
        d: ?[]const u8,
        next_op: GetOp,
    ) GetIterator {
        return this.base.iterator(init_op, k, d, next_op);
    }

    /// for use with DUPSORT databases only
    pub inline fn count(this: ReadOnly) ?usize {
        return this.base.count();
    }
};

/// Automatically deinitialized by the owning transaction
/// Memory returned by `get` is invalidated by the next `get` call
pub const ReadWrite = struct {
    base: Cursor,

    pub inline fn get(this: ReadWrite, op: GetOp, k: ?[]const u8, d: ?[]const u8) ?Kv {
        return this.base.get(op, k, d);
    }

    pub inline fn getMultiple(
        this: ReadWrite,
        comptime T: type,
        op: GetMultipleOp,
        k: ?[]const u8,
    ) ?[]align(1) const T {
        return this.base.getMultiple(T, op, k);
    }

    pub inline fn iterator(
        this: ReadWrite,
        init_op: GetOp,
        k: ?[]const u8,
        d: ?[]const u8,
        next_op: GetOp,
    ) GetIterator {
        return this.base.iterator(init_op, k, d, next_op);
    }

    /// for use with DUPSORT databases only
    pub inline fn count(this: ReadWrite) ?usize {
        return this.base.count();
    }

    pub inline fn put(this: ReadWrite, k: []const u8, d: []const u8) !void {
        return this.base.put(k, d);
    }

    pub inline fn putAppend(this: ReadWrite, k: []const u8, d: []const u8) !void {
        return this.base.putAppend(k, d);
    }

    pub inline fn putAppendDup(this: ReadWrite, k: []const u8, d: []const u8) !void {
        return this.base.putAppendDup(k, d);
    }

    pub inline fn putGet(this: ReadWrite, k: []const u8, d: []const u8) ![]u8 {
        return this.base.putGet(k, d);
    }

    pub inline fn putNoClobber(this: ReadWrite, k: []const u8, d: []const u8) !void {
        return this.base.putNoClobber(k, d);
    }

    pub inline fn putReplace(this: ReadWrite, k: []const u8, d: []const u8) !void {
        return this.base.putReplace(k, d);
    }

    pub inline fn putReserve(this: ReadWrite, k: []const u8, size: usize) ![]u8 {
        return this.base.putReserve(k, size);
    }

    pub inline fn putMultiple(
        this: ReadWrite,
        comptime T: type,
        k: []const u8,
        d: []const T,
    ) !usize {
        return this.base.putMultiple(T, k, d);
    }

    pub inline fn del(this: ReadWrite) !void {
        return this.base.del();
    }

    pub inline fn delAll(this: ReadWrite) !void {
        return this.base.delAll();
    }
};
