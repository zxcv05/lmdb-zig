//! Transaction - wrappers around mdb_txn*

const root = @import("root.zig");
const std = @import("std");
const c = @import("c");

const utils = @import("utils.zig");

const Cursor = @import("Cursor.zig");
const Dbi = @import("Dbi.zig");
const Env = @import("Env.zig");

const log = std.log.scoped(.lmdb);

// TODO: make Txn and Cursor generic with comptime known access mode, replace BadAccess w/ compile error

const Txn = @This();

inner: *c.MDB_txn,
done: bool = false,
debug: if (utils.DEBUG) Debug else void,

/// Create new transaction - See `Env.begin*(...)`
/// Only one read_only txn per thread is allowed
pub fn init(
    env: Env,
    src: std.builtin.SourceLocation,
    parent: ?*Txn,
    access: Access,
    flags: InitFlags,
) !Txn {
    if (parent) |ptxn| std.debug.assert(!ptxn.done);

    var flags_int: c_uint = 0;
    inline for (std.meta.fields(InitFlags)) |flag| {
        if (@field(flags, flag.name))
            flags_int |= @field(root.all_flags, flag.name);
    }
    if (access == .read_only) flags_int |= root.all_flags.read_only;

    var maybe_txn: ?*c.MDB_txn = null;
    switch (root.errno(c.mdb_txn_begin(
        env.inner,
        if (parent) |ptxn| ptxn.inner else null,
        flags_int,
        &maybe_txn,
    ))) {
        .SUCCESS => {},
        .PANIC => return error.Panic,
        .BAD_TXN => return error.BadParent,
        .MAP_RESIZED => return error.MapResized,
        .READERS_FULL => return error.ReadersFull,

        _ => |rc| {
            log.debug("Txn.init: {t}", .{rc});
            unreachable;
        },

        else => |rc| switch (@as(std.posix.E, @enumFromInt(@intFromEnum(rc)))) {
            .NOMEM => return error.OutOfMemory,
            .INVAL => return error.BlockedByReadOnlyTxn,

            else => {
                log.debug("Txn.init: {any}", .{rc});
                unreachable;
            },
        },
    }

    if (utils.DEBUG) {
        if (parent) |ptxn| ptxn.debug.children += 1;
    }

    return .{
        .inner = maybe_txn.?,
        .debug = if (utils.DEBUG) .{
            .access = access,
            .src = src,
            .parent = parent,
        },
    };
}

/// Commit transaction's changes to db
pub fn commit(this: *Txn) !void {
    if (this.done) {
        if (utils.DEBUG)
            utils.printWithSrc(this.debug.src, "commit() called on {*} while already committed or aborted", .{this});
        return error.Invalid;
    }

    if (utils.DEBUG) {
        if (this.debug.parent) |ptxn| ptxn.debug.children -= 1;
    }

    defer this.done = true;
    return switch (@as(std.posix.E, @enumFromInt(
        c.mdb_txn_commit(this.inner),
    ))) {
        .SUCCESS => {},
        .INVAL => error.Invalid,
        .NOSPC => error.NoSpaceLeft,
        .NOMEM => error.OutOfMemory,
        .IO => error.IoError,

        else => |rc| {
            log.debug("Txn.commit: {any}", .{rc});
            unreachable;
        },
    };
}

/// Abandon all changes made by this transaction
pub fn abort(this: *Txn) void {
    if (this.done) return;

    if (utils.DEBUG) {
        if (this.debug.parent) |ptxn| ptxn.debug.children -= 1;
    }

    defer this.done = true;
    c.mdb_txn_abort(this.inner);
}

/// reset() then renew() a *read-only* transaction (optimization)
pub fn reset_renew(this: *Txn) !void {
    if (utils.DEBUG) {
        if (this.debug.children > 0) {
            utils.printWithSrc(this.debug.src, "reset_renew() called on {*} with {d} children", .{ this, this.debug.children });
            return error.TxnHasChildren;
        } else if (this.debug.access != .read_only) {
            utils.printWithSrc(this.debug.src, "reset_renew() called on read_write {*}", .{this});
            return error.BadAccess;
        }
    }

    c.mdb_txn_reset(this.inner);

    switch (root.errno(c.mdb_txn_renew(this.inner))) {
        .SUCCESS => this.done = false,
        else => return error.RenewFailed,
    }
}

pub inline fn get(this: Txn, dbi: Dbi, key: []const u8) ?[]u8 {
    return dbi.get(this, key);
}

pub inline fn get_const(this: Txn, dbi: Dbi, key: []const u8) ?[]const u8 {
    return this.get(dbi, key);
}

pub inline fn put(this: Txn, dbi: Dbi, key: []const u8, data: []const u8) !void {
    return dbi.put(this, key, data);
}

/// `put()` with `no_dup_data` flag
/// supported for DUPSORT databases
pub inline fn put_no_clobber(this: Txn, dbi: Dbi, key: []const u8, data: []const u8) !void {
    return dbi.put_no_clobber(this, key, data);
}

/// `put()` with `no_overwrite` flag
pub inline fn put_get(this: Txn, dbi: Dbi, key: []const u8, data: []const u8) ![]u8 {
    return dbi.put_get(this, key, data);
}

/// `put()` with `append` flag
/// must be sorted
pub inline fn put_append(this: Txn, dbi: Dbi, key: []const u8, data: []const u8) !void {
    return dbi.put_append(this, key, data);
}

/// `put()` with `append_dup` flag
/// supported for DUPSORT databases
pub inline fn put_append_dup(this: Txn, dbi: Dbi, key: []const u8, data: []const u8) !void {
    return dbi.put_append_dup(this, key, data);
}

/// `put()` with `reserve` flag
/// NOT supported for DUPSORT databased
pub inline fn put_reserve(this: Txn, dbi: Dbi, key: []const u8, size: usize) ![]u8 {
    return dbi.put_reserve(this, key, size);
}

/// note: "NOTFOUND" is not considered an error condition
pub inline fn del(this: Txn, dbi: Dbi, key: []const u8, data: ?[]const u8) !void {
    return dbi.del(this, key, data);
}

pub inline fn cursor(txn: *const Txn, src: std.builtin.SourceLocation, dbi: Dbi) !Cursor {
    return Cursor.init(src, dbi, txn);
}

const Debug = struct {
    access: Txn.Access,
    src: std.builtin.SourceLocation,
    parent: ?*Txn,
    children: usize = 0,
};

pub const Access = enum {
    read_only,
    read_write,
};

pub const InitFlags = packed struct {
    no_sync: bool = false,
    no_meta_sync: bool = false,
};
