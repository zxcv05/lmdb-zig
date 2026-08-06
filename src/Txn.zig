//! Transaction - wrappers around mdb_txn*

const root = @import("root.zig");
const std = @import("std");
const c = @import("c");

const utils = @import("utils.zig");

const Cursor = @import("Cursor.zig");
const Dbi = @import("Dbi.zig");
const Env = @import("Env.zig");
const Val = @import("Val.zig");

const log = std.log.scoped(.lmdb);

const Txn = @This();

inner: *c.MDB_txn,
status: Status = .open,

/// Create new transaction - See `Env.begin*(...)`
pub fn init(
    env: Env,
    parent: ?*Txn,
    access: Access,
    flags: InitFlags,
) !Txn {
    if (parent) |ptxn| std.debug.assert(ptxn.status == .open);

    var flags_int: c_uint = 0;
    inline for (std.meta.fields(InitFlags)) |flag| {
        if (@field(flags, flag.name))
            flags_int |= @field(root.all_flags, flag.name);
    }
    if (access == .read_only) flags_int |= root.all_flags.read_only;

    var maybe_txn: ?*c.MDB_txn = null;
    return switch (root.errno(c.mdb_txn_begin(
        env.inner,
        if (parent) |ptxn| ptxn.inner else null,
        flags_int,
        &maybe_txn,
    ))) {
        .SUCCESS => .{ .inner = maybe_txn.? },
        .PANIC => error.Panic,
        .BAD_TXN => error.BadParent,
        .MAP_RESIZED => error.MapResized,
        .READERS_FULL => error.ReadersFull,

        else => |rc| root.lmdbUnhandledError(@src(), rc),

        _ => |rc| switch (@as(std.posix.E, @enumFromInt(@intFromEnum(rc)))) {
            // If NO_TLS isnt set on the Env then only one read only txn per thread is allowed
            .INVAL => error.BlockedByReadOnlyTxn,
            .NOMEM => error.OutOfMemory,

            else => root.lmdbUnhandledError(@src(), rc),
        },
    };
}

/// Commit transaction's changes to db
/// Sets `status` to `.committed` on success and `.invalid` if `error.Invalid` is returned.
pub fn commit(this: *Txn) !void {
    switch (this.status) {
        .open => {},
        .committed => return error.Committed,
        .aborted => return error.Aborted,
        else => unreachable,
    }

    return switch (@as(std.posix.E, @enumFromInt(
        c.mdb_txn_commit(this.inner),
    ))) {
        .SUCCESS => this.status = .committed,
        .INVAL => {
            this.status = .invalid;
            return error.Invalid;
        },
        .NOSPC => error.NoSpaceLeft,
        .NOMEM => error.OutOfMemory,
        .IO => error.IoError,

        else => |rc| root.lmdbUnhandledError(@src(), rc),
    };
}

/// Abandon all changes made by this transaction
/// Sets `status` to `.aborted`
pub fn abort(this: *Txn) void {
    switch (this.status) {
        .open, .reset => {},
        else => return,
    }

    defer this.status = .aborted;
    c.mdb_txn_abort(this.inner);
}

/// Reset a read only txn (to later `renew()`)
/// Sets `status` to `.reset`
pub fn reset(this: *Txn) !void {
    switch (this.status) {
        .open, .aborted => {},
        .reset => return,
        else => unreachable,
    }

    defer this.status = .reset;
    c.mdb_txn_reset(this.inner);
}

/// Renew a reset read only txn
/// Sets `status` to `.open`
pub fn renew(this: *Txn) !void {
    switch (this.status) {
        .reset => {},
        else => unreachable,
    }

    switch (root.errno(c.mdb_txn_renew(this.inner))) {
        .SUCCESS => this.status = .open,
        else => {
            this.status = .invalid;
            return error.Failed;
        },
    }
}

pub fn get(txn: Txn, dbi: Dbi, key: []const u8) !?[]u8 {
    var c_key: Val = .from_const(key);
    var c_out: Val = .empty;

    return switch (root.errno(
        c.mdb_get(txn.inner, dbi.handle, c_key.alias(), c_out.alias()),
    )) {
        .SUCCESS => return c_out.unalias(),
        .NOTFOUND => return null,

        else => |rc| return root.lmdbUnhandledError(@src(), rc),

        _ => |rc| switch (@as(std.posix.E, @enumFromInt(@intFromEnum(rc)))) {
            .INVAL => return error.Invalid,
            else => return root.lmdbUnhandledError(@src(), rc),
        },
    };
}

pub inline fn getConst(txn: Txn, dbi: Dbi, key: []const u8) !?[]const u8 {
    return txn.get(dbi, key);
}

pub fn put(txn: Txn, dbi: Dbi, key: []const u8, data: []const u8) !void {
    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    return txn.putImpl(dbi, c_key.alias(), c_data.alias(), 0);
}

/// `put()` with `no_dup_data` flag
/// supported for DUPSORT databases
pub fn putNoClobber(txn: Txn, dbi: Dbi, key: []const u8, data: []const u8) !void {
    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    return txn.putImpl(dbi, c_key.alias(), c_data.alias(), root.all_flags.no_dup_data);
}

/// `put()` with `no_overwrite` flag
pub fn putGet(txn: Txn, dbi: Dbi, key: []const u8, data: []const u8) ![]u8 {
    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    txn.putImpl(dbi, c_key.alias(), c_data.alias(), root.all_flags.no_overwrite) catch |e| switch (e) {
        error.AlreadyExists => {},
        else => return e,
    };

    return c_data.unalias();
}

/// `put()` with `append` flag
/// keys must be sorted
pub fn putAppend(txn: Txn, dbi: Dbi, key: []const u8, data: []const u8) !void {
    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    txn.putImpl(dbi, c_key.alias(), c_data.alias(), root.all_flags.append) catch |e| switch (e) {
        error.AlreadyExists => return error.Unsorted,
        else => return e,
    };
}

/// `put()` with `append_dup` flag
/// supported for DUPSORT databases
pub fn putAppendDup(txn: Txn, dbi: Dbi, key: []const u8, data: []const u8) !void {
    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    txn.putImpl(dbi, c_key.alias(), c_data.alias(), root.all_flags.append_dup) catch |e| switch (e) {
        error.AlreadyExists => return error.Unsorted,
        else => return e,
    };
}

/// `put()` with `reserve` flag
/// NOT supported for DUPSORT databased
pub fn putReserve(txn: Txn, dbi: Dbi, key: []const u8, size: usize) ![]u8 {
    var c_key: Val = .from_const(key);
    var c_data: Val = .of_size(size);

    try txn.putImpl(dbi, c_key.alias(), c_data.alias(), root.all_flags.reserve);
    return c_data.unalias();
}

fn putImpl(txn: Txn, dbi: Dbi, c_key: ?*c.MDB_val, c_data: ?*c.MDB_val, flags: c_uint) !void {
    switch (txn.status) {
        .open => {},
        .committed => return error.Committed,
        .aborted => return error.Aborted,
        else => unreachable,
    }

    switch (root.errno(
        c.mdb_put(txn.inner, dbi.handle, c_key, c_data, flags),
    )) {
        .SUCCESS => {},
        .MAP_FULL => return error.MapFull,
        .TXN_FULL => return error.TxnFull,
        .KEYEXIST => return error.AlreadyExists,

        else => |rc| return root.lmdbUnhandledError(@src(), rc),

        _ => |rc| switch (@as(std.posix.E, @enumFromInt(@intFromEnum(rc)))) {
            .ACCES => return error.ReadOnly,
            .INVAL => return error.Invalid,

            else => return root.lmdbUnhandledError(@src(), rc),
        },
    }
}

/// returns true if deleted, false if not found, error otherwise
pub fn del(txn: Txn, dbi: Dbi, key: []const u8, data: ?[]const u8) !bool {
    switch (txn.status) {
        .open => {},
        .committed => return error.Committed,
        .aborted => return error.Aborted,
        else => unreachable,
    }

    var c_key: Val = .from_const(key);
    var c_data: Val = .from_const(data);

    switch (root.errno(c.mdb_del(txn.inner, dbi.handle, c_key.alias(), c_data.alias()))) {
        .SUCCESS => return true,
        .NOTFOUND => return false,

        else => |rc| return root.lmdbUnhandledError(@src(), rc),

        _ => |rc| switch (@as(std.posix.E, @enumFromInt(@intFromEnum(rc)))) {
            .ACCES => return error.ReadOnly,
            .INVAL => return error.Invalid,
            else => return root.lmdbUnhandledError(@src(), rc),
        },
    }
}

pub inline fn cursor(txn: Txn, dbi: Dbi) !Cursor {
    return Cursor.init(dbi, txn);
}

pub const Access = enum {
    read_only,
    read_write,
};

pub const Status = enum {
    invalid,
    open,
    committed,
    aborted,
    reset,
};

pub const InitFlags = packed struct {
    no_sync: bool = false,
    no_meta_sync: bool = false,
};

/// Holds pages alive until aborted
pub const ReadOnly = struct {
    base: Txn,

    pub inline fn abort(this: *ReadOnly) void {
        this.base.abort();
    }

    pub inline fn reset(this: *ReadOnly) !void {
        return this.base.reset();
    }

    pub inline fn renew(this: *ReadOnly) !void {
        return this.base.renew();
    }

    pub inline fn cursor(this: ReadOnly, dbi: Dbi) !Cursor.ReadOnly {
        return .{ .base = try this.base.cursor(dbi) };
    }

    pub inline fn get(this: ReadOnly, dbi: Dbi, k: []const u8) !?[]const u8 {
        return this.base.getConst(dbi, k);
    }

    pub const getConst = ReadOnly.get;
};

/// Memory returned by `get` is invalidated by the next `get` call
pub const ReadWrite = struct {
    base: Txn,

    pub inline fn commit(this: *ReadWrite) !void {
        return this.base.commit();
    }

    pub inline fn abort(this: *ReadWrite) void {
        this.base.abort();
    }

    pub inline fn cursor(this: ReadWrite, dbi: Dbi) !Cursor.ReadWrite {
        return .{ .base = try this.base.cursor(dbi) };
    }

    /// Modifications to return value are valid ONLY IF
    /// env initialized with `write_map` flag
    pub inline fn get(this: ReadWrite, dbi: Dbi, k: []const u8) !?[]u8 {
        return this.base.get(dbi, k);
    }

    pub inline fn getConst(this: ReadWrite, dbi: Dbi, k: []const u8) !?[]const u8 {
        return this.base.getConst(dbi, k);
    }

    pub inline fn put(this: ReadWrite, dbi: Dbi, k: []const u8, d: []const u8) !void {
        return this.base.put(dbi, k, d);
    }

    pub inline fn putAppend(this: ReadWrite, dbi: Dbi, k: []const u8, d: []const u8) !void {
        return this.base.putAppend(dbi, k, d);
    }

    pub inline fn putAppendDup(this: ReadWrite, dbi: Dbi, k: []const u8, d: []const u8) !void {
        return this.base.putAppendDup(dbi, k, d);
    }

    pub inline fn putGet(this: ReadWrite, dbi: Dbi, k: []const u8, d: []const u8) ![]u8 {
        return this.base.putGet(dbi, k, d);
    }

    pub inline fn putNoClobber(this: ReadWrite, dbi: Dbi, k: []const u8, d: []const u8) !void {
        return this.base.putNoClobber(dbi, k, d);
    }

    pub inline fn putReserve(this: ReadWrite, dbi: Dbi, k: []const u8, size: usize) ![]u8 {
        return this.base.putReserve(dbi, k, size);
    }

    pub inline fn del(this: ReadWrite, dbi: Dbi, k: []const u8, d: ?[]const u8) !bool {
        return this.base.del(dbi, k, d);
    }
};
