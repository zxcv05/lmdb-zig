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

pub fn get(this: Cursor, key: []const u8, data: ?[]const u8, op: GetOp) !?Kv {
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
    /// need dupsort
    first_dup,
    /// need dupsort
    get_both,
    /// need dupsort
    get_both_range,
    get_current,
    /// need dupsort+dupfixed
    get_multiple,
    last,
    /// need dupsort
    last_dup,
    next,
    /// need dupsort
    next_dup,
    /// need dupsort+dupfixed
    next_multiple,
    next_nodup,
    prev,
    /// need dupsort
    prev_dup,
    /// need dupsort+dupfixed
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
