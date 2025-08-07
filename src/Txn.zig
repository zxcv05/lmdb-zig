//! Transaction - wrappers around mdb_txn*

const root = @import("root.zig");
const std = @import("std");
const c = @import("c");

const utils = @import("utils.zig");

const Cursor = @import("Cursor.zig");
const Dbi = @import("Dbi.zig");
const Env = @import("Env.zig");

// TODO: make Txn and Cursor generic with comptime known access mode, make BadAccess raise compile error

const Txn = @This();

inner: *c.MDB_txn,
done: bool = false,
debug: if (utils.DEBUG) Debug,

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
        else => |rc| switch (@as(std.posix.E, @enumFromInt(@intFromEnum(rc)))) {
            .NOMEM => return error.OutOfMemory,
            .INVAL => return error.BlockedByReadOnlyTxn,
            else => unreachable,
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
    switch (@as(std.posix.E, @enumFromInt(
        c.mdb_txn_commit(this.inner),
    ))) {
        .SUCCESS => {},
        .INVAL => return error.Invalid,
        .NOSPC => return error.NoSpaceLeft,
        .NOMEM => return error.OutOfMemory,
        .IO => return error.IoError,
        else => unreachable,
    }
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
