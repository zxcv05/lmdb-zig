//! Database - wrappers around mdb_dbi*

const root = @import("root.zig");
const std = @import("std");
const c = @import("c");

const Cursor = @import("Cursor.zig");
const Env = @import("Env.zig");
const Txn = @import("Txn.zig");
const Val = @import("Val.zig");

const Dbi = @This();

handle: c.MDB_dbi,

pub fn init(txn: Txn, name: ?[:0]const u8, flags: Flags) !Dbi {
    var flags_int: c_uint = 0;
    inline for (std.meta.fields(Flags)) |flag| {
        if (@field(flags, flag.name))
            flags_int |= @field(root.all_flags, flag.name);
    }

    var dbi: c.MDB_dbi = undefined;

    switch (root.errno(
        c.mdb_dbi_open(txn.inner, if (name) |n| n.ptr else null, 0, &dbi),
    )) {
        .SUCCESS => return .{ .handle = dbi },
        .NOTFOUND => return error.NotFound,
        .DBS_FULL => return error.TooMany,
        else => unreachable,
    }
}

pub fn statistics(this: Dbi, txn: Txn) c.MDB_stat {
    var stat: c.MDB_stat = undefined;
    if (c.mdb_stat(txn.inner, this.handle, &stat) != @intFromEnum(root.E.SUCCESS)) unreachable;
    return stat;
}

pub fn get(this: Dbi, txn: Txn, key: []const u8) ?[]u8 {
    var c_key: Val = .from_const(key);
    var c_out: Val = .empty;

    switch (root.errno(
        c.mdb_get(txn.inner, this.handle, c_key.alias(), c_out.alias()),
    )) {
        .SUCCESS => return c_out.unalias(),
        .NOTFOUND => return null,
        else => unreachable,
    }
}

pub fn get_const(this: Dbi, txn: Txn, key: []const u8) ?[]const u8 {
    return this.get(txn, key);
}

pub fn put(this: Dbi, txn: Txn, key: []const u8, data: []const u8, flags: PutFlags) !void {
    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    var flags_int: c_uint = 0;
    inline for (std.meta.fields(PutFlags)) |field| {
        if (@field(flags, field.name))
            flags_int |= @field(root.all_flags, field.name);
    }

    try this.put_impl(txn, &c_key, &c_data, flags_int);
}

/// Always specifies no_overwrite
pub fn put_get(this: Dbi, txn: Txn, key: []const u8, data: []const u8, flags: PutFlags) ![]u8 {
    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    var flags_int: c_uint = root.all_flags.no_overwrite;
    inline for (std.meta.fields(PutFlags)) |field| {
        if (@field(flags, field.name))
            flags_int |= @field(root.all_flags, field.name);
    }

    try this.put_impl(txn, &c_key, &c_data, flags_int);
    return c_data.unalias();
}

fn put_impl(this: Dbi, txn: Txn, key: *Val, data: *Val, flags: c_uint) !void {
    switch (root.errno(
        c.mdb_put(txn.inner, this.handle, key.alias(), data.alias(), flags),
    )) {
        .SUCCESS => {},
        .MAP_FULL => return error.DbFull,
        .TXN_FULL => return error.TxnFull,
        else => |rc| switch (std.posix.errno(@intFromEnum(rc))) {
            .ACCES => return error.ReadOnly,
            .INVAL => return error.Invalid,
            else => unreachable,
        },
    }
}

pub fn del(this: Dbi, txn: Txn, key: []const u8, data: ?[]const u8) !void {
    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    switch (root.errno(c.mdb_del(txn.inner, this.handle, c_key.alias(), c_data.alias()))) {
        .SUCCESS => {},
        else => |rc| switch (std.posix.errno(@intFromEnum(rc))) {
            .ACCES => return error.ReadOnly,
            .INVAL => return error.Invalid,
            else => unreachable,
        },
    }
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

pub fn cursor(this: Dbi, txn: Txn) Cursor {
    var ptr: ?*c.MDB_cursor = undefined;
    if (c.mdb_cursor_open(txn.inner, this.handle, &ptr) != @intFromEnum(root.E.SUCCESS)) unreachable;
    return .{ .inner = ptr.? };
}

pub const Flags = packed struct {
    reverse_key: bool = false,
    dup_sort: bool = false,
    integer_key: bool = false,
    dup_fixed: bool = false,
    integer_dup: bool = false,
    reverse_dup: bool = false,
    create: bool = false,
};

pub const PutFlags = packed struct {
    no_dup_data: bool = false,
    reserve: bool = false,
    append: bool = false,
    append_dup: bool = false,
};
