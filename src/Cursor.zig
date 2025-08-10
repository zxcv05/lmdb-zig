//! Cursor - wrappers around mdb_cursor*

const root = @import("root.zig");
const std = @import("std");
const c = @import("c");

const Dbi = @import("Dbi.zig");
const Txn = @import("Txn.zig");
const Val = @import("Val.zig");

const utils = @import("utils.zig");

const Cursor = @This();

inner: *c.MDB_cursor,
debug: if (utils.DEBUG) Debug else void,

/// Cursor inherits `txn`s access mode
pub fn init(src: std.builtin.SourceLocation, dbi: Dbi, txn: *const Txn) !Cursor {
    if (utils.DEBUG and txn.debug.children > 0) {
        utils.printWithSrc(src, "Cursor.init() called with {*} that has {d} children", .{ txn, txn.debug.children });
        return error.TxnHasChildren;
    }

    var ptr: ?*c.MDB_cursor = null;

    switch (root.errno(
        c.mdb_cursor_open(txn.inner, dbi.handle, &ptr),
    )) {
        .SUCCESS => return .{
            .inner = ptr.?,
            .debug = if (utils.DEBUG) .{
                .access = txn.debug.access,
                .src = src,
                .owner = txn,
            },
        },
        else => unreachable,
    }
}

/// A read_only cursor must be closed explicitly, before or after its owning txn ends
/// A read_write cursor can be closed before its owning txn ends, and will otherwise be closed when its owning txn ends
pub fn deinit(this: *const Cursor) void {
    if (utils.DEBUG and this.debug.access == .read_write and this.debug.owner.done) {
        utils.printWithSrc(
            this.debug.src,
            "deinit() called on read_write {*} whose owning {*} is already aborted or committed (skipping)",
            .{ this, this.debug.owner },
        );
        return;
    }

    c.mdb_cursor_close(this.inner);
}

/// Renew a *read-only* cursor
pub fn renew(this: *Cursor, txn: *const Txn) !void {
    if (utils.DEBUG and this.debug.access != .read_only) {
        utils.printWithSrc(this.debug.src, "renew() called on read_write {*}", .{this});
        return error.BadAccess;
    }

    if (c.mdb_cursor_renew(txn.inner, this.inner) != @intFromEnum(root.E.SUCCESS)) unreachable;

    if (utils.DEBUG) {
        this.debug.owner = txn;
    }
}

/// Warning: Errors will be treated as "Not found" and will return null
/// Make sure if doing DUPSORT operations that the dbi is actually DUPSORT
pub fn get(this: *const Cursor, op: GetOp, key: ?[]const u8, data: ?[]const u8) ?Kv {
    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    const found = this.get_impl(op, c_key.alias(), c_data.alias());
    if (!found) return null;

    return .{
        c_key.unalias_maybe() orelse &.{},
        c_data.unalias_maybe() orelse &.{},
    };
}

/// returns true if data found, false if not found or error occured
fn get_impl(this: Cursor, op: GetOp, c_key: ?*c.MDB_val, c_data: ?*c.MDB_val) bool {
    switch (root.errno(
        c.mdb_cursor_get(this.inner, c_key, c_data, @intFromEnum(op)),
    )) {
        .NOTFOUND => return false,
        else => |rc| return @intFromEnum(rc) >= @intFromEnum(root.E.SUCCESS),
    }
}

pub fn get_iter(
    this: *const Cursor,
    init_op: GetOp,
    init_key: ?[]const u8,
    init_data: ?[]const u8,
    next_op: GetOp,
) GetIterator {
    return GetIterator.init(this, init_op, init_key, init_data, next_op);
}

pub fn put(this: *Cursor, key: []const u8, data: []const u8) !void {
    if (utils.DEBUG and this.debug.access != .read_write) {
        utils.printWithSrc(this.debug.src, "put() called on read_only {*}", .{this});
        return error.BadAccess;
    }

    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    return this.put_impl(c_key.alias(), c_data.alias(), 0);
}

/// `put()` with `current` flag
/// key must match item at the current cursor position
pub fn put_replace(this: *Cursor, key: []const u8, data: []const u8) !void {
    if (utils.DEBUG and this.debug.access != .read_write) {
        utils.printWithSrc(this.debug.src, "put_replace() called on read_only {*}", .{this});
        return error.BadAccess;
    }

    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    return this.put_impl(c_key.alias(), c_data.alias(), root.all_flags.current);
}

/// `put()` with `no_dup_data` flag
/// supported for DUPSORT databases
pub fn put_no_clobber(this: *Cursor, key: []const u8, data: []const u8) !void {
    if (utils.DEBUG and this.debug.access != .read_write) {
        utils.printWithSrc(this.debug.src, "put_no_clobber() called on read_only {*}", .{this});
        return error.BadAccess;
    }

    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    return this.put_impl(c_key.alias(), c_data.alias(), root.all_flags.no_dup_data);
}

/// `put()` with `no_overwrite` flag
/// will put data (if not existing) or return it (if existing)
pub fn put_get(this: *Cursor, key: []const u8, data: []const u8) ![]u8 {
    if (utils.DEBUG and this.debug.access != .read_write) {
        utils.printWithSrc(this.debug.src, "put_get() called on read_only {*}", .{this});
        return error.BadAccess;
    }

    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    this.put_impl(c_key.alias(), c_data.alias(), root.all_flags.no_overwrite) catch |e| switch (e) {
        error.AlreadyExists => {},
        else => return e,
    };

    return c_data.unalias();
}

/// `put()` with `append` flag
/// must be sorted
pub fn put_append(this: *Cursor, key: []const u8, data: []const u8) !void {
    if (utils.DEBUG and this.debug.access != .read_write) {
        utils.printWithSrc(this.debug.src, "put_append() called on read_only {*}", .{this});
        return error.BadAccess;
    }

    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    return this.put_impl(c_key.alias(), c_data.alias(), root.all_flags.append) catch |e| switch (e) {
        error.AlreadyExists => error.Unsorted,
        else => e,
    };
}

/// `put()` with `append_dup` flag
/// supported for DUPSORT databases
pub fn put_append_dup(this: *Cursor, key: []const u8, data: []const u8) !void {
    if (utils.DEBUG and this.debug.access != .read_write) {
        utils.printWithSrc(this.debug.src, "put_append_dup() called on read_only {*}", .{this});
        return error.BadAccess;
    }

    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    return this.put_impl(c_key.alias(), c_data.alias(), root.all_flags.append_dup) catch |e| switch (e) {
        error.AlreadyExists => error.Unsorted,
        else => e,
    };
}

/// `put()` with `reserve` flag
/// NOT supported for DUPSORT databased
pub fn put_reserve(this: *Cursor, key: []const u8, size: usize) ![]u8 {
    if (utils.DEBUG and this.debug.access != .read_write) {
        utils.printWithSrc(this.debug.src, "put_reserve() called on read_only {*}", .{this});
        return error.BadAccess;
    }

    var c_key: Val = .from_const(key);
    var c_data: Val = .of_size(size);

    try this.put_impl(c_key.alias(), c_data.alias(), root.all_flags.reserve);
    return c_data.unalias();
}

/// `put()` with `multiple` flag
/// supported for DUPFIXED databases
// pub fn put_multiple(this: Cursor, comptime T: type, key: []const u8, data: []const T) !usize {
//     var data_actual: [2]c.MDB_val = .{
//         .{ .mv_size = @sizeOf(T), .mv_data = @ptrCast(@constCast(data.ptr)) },
//         .{ .mv_size = data.len, .mv_data = undefined },
//     };

//     var c_key: Val = .from_const(key);
//     var c_data: Val = .from(std.mem.asBytes(&data_actual));

//     try this.put_impl(c_key.alias(), c_data.alias(), root.all_flags.multiple);
//     return data_actual[1].mv_size;
// }

fn put_impl(this: Cursor, c_key: ?*c.MDB_val, c_data: ?*c.MDB_val, flags: c_uint) !void {
    switch (root.errno(
        c.mdb_cursor_put(this.inner, c_key, c_data, flags),
    )) {
        .SUCCESS => {},
        .MAP_FULL => return error.MapFull,
        .TXN_FULL => return error.TxnFull,
        .KEYEXIST => return error.AlreadyExists,
        else => |rc| switch (@as(std.posix.E, @enumFromInt(@intFromEnum(rc)))) {
            .ACCES => return error.ReadOnly,
            .INVAL => return error.Invalid,
            else => unreachable,
        },
    }
}

/// Delete current key/data pair
pub fn del(this: *Cursor) !void {
    if (utils.DEBUG and this.debug.access != .read_write) {
        utils.printWithSrc(this.debug.src, "del() called on read_only {*}", .{this});
        return error.BadAccess;
    }

    return this.del_impl(0);
}

/// Delete all items for current key
/// supported for DUPSORT databases
pub fn del_all(this: *Cursor) !void {
    if (utils.DEBUG and this.debug.access != .read_write) {
        utils.printWithSrc(this.debug.src, "del_all() called on read_only {*}", .{this});
        return error.BadAccess;
    }

    return this.del_impl(root.all_flags.no_dup_data);
}

fn del_impl(this: Cursor, flags: c_uint) !void {
    return switch (@as(std.posix.E, @enumFromInt(
        c.mdb_cursor_del(this.inner, flags),
    ))) {
        .SUCCESS => {},
        .ACCES => error.ReadOnly,
        .INVAL => error.Invalid,
        else => unreachable,
    };
}

const Debug = struct {
    access: Txn.Access,
    src: std.builtin.SourceLocation,
    owner: *const Txn,
};

pub const Kv = struct { []const u8, []const u8 };

pub const GetOp = enum(u5) {
    first = c.MDB_FIRST,
    first_dup = c.MDB_FIRST_DUP,
    last = c.MDB_LAST,
    last_dup = c.MDB_LAST_DUP,
    get_both = c.MDB_GET_BOTH,
    get_both_range = c.MDB_GET_BOTH_RANGE,
    get_current = c.MDB_GET_CURRENT,
    get_multiple = c.MDB_GET_MULTIPLE,
    set = c.MDB_SET,
    set_key = c.MDB_SET_KEY,
    set_range = c.MDB_SET_RANGE,
    next = c.MDB_NEXT,
    next_dup = c.MDB_NEXT_DUP,
    next_nodup = c.MDB_NEXT_NODUP,
    next_multiple = c.MDB_NEXT_MULTIPLE,
    prev = c.MDB_PREV,
    prev_dup = c.MDB_PREV_DUP,
    prev_nodup = c.MDB_PREV_NODUP,
    prev_multiple = c.MDB_PREV_MULTIPLE,
};

pub const DelFlags = packed struct {
    no_dup_data: bool = false,
};

pub const GetIterator = struct {
    owner: *const Cursor,
    c_key: Val,
    c_data: Val,
    state: State,

    pub fn init(
        cursor: *const Cursor,
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

        this.state.found = cursor.get_impl(init_op, this.c_key.alias(), this.c_data.alias());
        return this;
    }

    pub fn next(this: *GetIterator) ?Kv {
        if (!this.state.found) return null;

        if (this.state.skip) {
            this.state.skip = false;
        } else if (!this.owner.get_impl(
            this.state.next_op,
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

    pub const State = packed struct {
        next_op: GetOp,
        skip: bool = true,
        found: bool = false,
    };
};
