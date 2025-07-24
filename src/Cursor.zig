//! Cursor - wrappers around mdb_cursor*

const root = @import("root.zig");
const std = @import("std");
const c = @import("c");

const Txn = @import("Txn.zig");
const Val = @import("Val.zig");

const Cursor = @This();

inner: *c.MDB_cursor,

// init: see `Dbi.cursor(...)`

/// Transaction must outlive this call if writes occurred
pub fn deinit(this: Cursor) void {
    c.mdb_cursor_close(this.inner);
}

pub fn renew(this: Cursor, txn: Txn) void {
    if (c.mdb_cursor_renew(txn.inner, this.inner) != @intFromEnum(root.E.SUCCESS)) unreachable;
}

pub fn get(this: Cursor, op: GetOp, key: ?[]const u8, data: ?[]const u8) !?Kv {
    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    switch (root.errno(
        c.mdb_cursor_get(this.inner, c_key.alias(), c_data.alias(), @intFromEnum(op)),
    )) {
        .SUCCESS => {},
        .NOTFOUND => return null,
        else => unreachable,
    }

    return .{
        .key = c_key.unalias(),
        .value = c_data.unalias(),
    };
}

// TODO: put_multiple
pub fn put(this: Cursor, key: []const u8, data: []const u8, flags: PutFlags) !void {
    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    var flags_int: c_uint = 0;
    inline for (std.meta.fields(PutFlags)) |flag| {
        if (@field(flags, flag.name))
            flags_int |= @field(root.all_flags, flag.name);
    }

    switch (root.errno(
        c.mdb_cursor_put(this.inner, c_key.alias(), c_data.alias(), flags_int),
    )) {
        .SUCCESS => {},
        .MAP_FULL => return error.MapFull,
        .TXN_FULL => return error.TxnFull,
        else => |rc| switch (std.posix.errno(@intFromEnum(rc))) {
            .ACCES => return error.ReadOnly,
            .INVAL => return error.Invalid,
            else => unreachable,
        },
    }
}

pub fn del(this: Cursor, flags: DelFlags) !void {
    var flags_int: c_uint = 0;
    inline for (std.meta.fields(DelFlags)) |flag| {
        if (@field(flags, flag.name))
            flags_int |= @field(root.all_flags, flag.name);
    }

    switch (std.posix.errno(
        c.mdb_cursor_del(this.inner, flags_int),
    )) {
        .SUCCESS => {},
        .ACCES => return error.ReadOnly,
        .INVAL => return error.Invalid,
        else => unreachable,
    }
}

pub const Kv = struct {
    key: []const u8,
    value: []const u8,
};

pub const GetOp = enum(c_uint) {
    first,
    /// need dup_sort
    first_dup,
    /// need dup_sort
    get_both,
    /// need dup_sort
    get_both_range,
    get_current,
    /// need dup_sort + dup_fixed
    get_multiple,
    last,
    /// need dup_sort
    last_dup,
    next,
    /// need dup_sort
    next_dup,
    /// need dup_sort + dup_fixed
    next_multiple,
    next_nodup,
    prev,
    /// need dup_sort
    prev_dup,
    /// need dup_sort + dup_fixed
    prev_multiple,
    prev_nodup,
    set,
    set_key,
    set_range,
};

pub const PutFlags = packed struct {
    current: bool = false,
    no_dup_data: bool = false,
    no_overwrite: bool = false,
    reserve: bool = false,
    append: bool = false,
    append_dup: bool = false,
    // multiple,
};

pub const DelFlags = packed struct {
    no_dup_data: bool = false,
};
