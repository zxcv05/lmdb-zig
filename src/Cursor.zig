//! Cursor - wrappers around mdb_cursor*

const root = @import("root.zig");
const std = @import("std");
const c = @import("c");

const Txn = @import("Txn.zig");
const Val = @import("Val.zig");

// todo: track state to ensure correctness and provide debugging
// - track is owned txn is rw/ro, only rw txn's free cursors, ro needs manual deinit()

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

pub fn get(this: Cursor, op: GetOp, key: ?[]const u8, data: ?[]const u8) ?Kv {
    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    const found = this.get_impl(op, c_key.alias(), c_data.alias());
    if (!found) return null;

    return .{
        c_key.unalias(),
        c_data.unalias_maybe() orelse &.{},
    };
}

/// returns if data was found
fn get_impl(this: Cursor, op: GetOp, c_key: ?*c.MDB_val, c_data: ?*c.MDB_val) bool {
    switch (root.errno(
        c.mdb_cursor_get(this.inner, c_key, c_data, @intFromEnum(op)),
    )) {
        .NOTFOUND => return false,
        else => |rc| if (@intFromEnum(rc) >= @intFromEnum(root.E.SUCCESS))
            return true
        else {
            std.debug.print("get_impl: {any}\n", .{rc});
            unreachable;
        },
    }
}

pub fn get_iter(
    this: *const Cursor,
    init_op: GetOp,
    init_key: ?[]const u8,
    init_data: ?[]const u8,
    loop_op: GetOp,
) GetIterator {
    return GetIterator.init(this, init_op, init_key, init_data, loop_op);
}

pub fn put(this: Cursor, key: []const u8, data: []const u8, flags: PutFlags) !void {
    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    var flags_int: c_uint = 0;
    inline for (std.meta.fields(PutFlags)) |flag| {
        if (@field(flags, flag.name))
            flags_int |= @field(root.all_flags, flag.name);
    }

    return this.put_impl(c_key.alias(), c_data.alias(), flags_int);
}

/// Always specifies no_overwrite
pub fn put_get(this: Cursor, key: []const u8, data: []const u8, flags: PutFlags) ![]u8 {
    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    var flags_int: c_uint = root.all_flags.no_overwrite;
    inline for (std.meta.fields(PutFlags)) |field| {
        if (@field(flags, field.name))
            flags_int |= @field(root.all_flags, field.name);
    }

    this.put_impl(c_key.alias(), c_data.alias(), flags_int) catch |e| switch (e) {
        error.AlreadyExists => {},
        else => return e,
    };
    return c_data.unalias();
}

// TODO: make this function work and rewrite a unit test
// pub fn put_multiple(this: Cursor, comptime T: type, key: []const u8, data: []const T, flags: PutFlags) !usize {
//     var data_actual: [2]c.MDB_val = .{
//         .{ .mv_size = @sizeOf(T), .mv_data = @ptrCast(@constCast(data.ptr)) },
//         .{ .mv_size = data.len, .mv_data = undefined },
//     };

//     std.debug.print("{any}\n", .{data_actual});

//     var c_key: Val = .from_const(key);
//     var c_data: Val = .from(std.mem.asBytes(&data_actual));

//     var flags_int: c_uint = root.all_flags.multiple;
//     inline for (std.meta.fields(PutFlags)) |flag| {
//         if (@field(flags, flag.name))
//             flags_int |= @field(root.all_flags, flag.name);
//     }

//     this.put_impl(c_key.alias(), c_data.alias(), flags_int) catch |e| switch (e) {
//         error.AlreadyExists => if (flags.append or flags.append_dup) return error.Unsorted else unreachable,
//         else => return e,
//     };

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

pub const PutFlags = packed struct {
    current: bool = false,
    no_dup_data: bool = false,
    no_overwrite: bool = false,
    // reserve: bool = false,
    append: bool = false,
    append_dup: bool = false,
    // multiple,
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
        loop_op: GetOp,
    ) GetIterator {
        var this: GetIterator = .{
            .owner = cursor,
            .c_key = .from_const(init_key),
            .c_data = .from_const(init_data),
            .state = .{ .loop_op = loop_op },
        };

        this.state.found = cursor.get_impl(init_op, this.c_key.alias(), this.c_data.alias());
        return this;
    }

    pub fn next(this: *GetIterator) ?Kv {
        if (!this.state.found) return null;

        if (this.state.skip) {
            this.state.skip = false;
        } else if (!this.owner.get_impl(
            this.state.loop_op,
            this.c_key.alias(),
            this.c_data.alias(),
        )) {
            this.state.found = false;
            return null;
        }

        return .{
            this.c_key.unalias(),
            this.c_data.unalias_maybe() orelse &.{},
        };
    }

    pub const State = packed struct {
        loop_op: GetOp,
        skip: bool = true,
        found: bool = false,
    };
};
