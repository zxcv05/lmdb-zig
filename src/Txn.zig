//! Transaction - wrappers around mdb_txn*

const root = @import("root.zig");
const std = @import("std");
const c = @import("c");

const utils = @import("utils.zig");

const Cursor = @import("Cursor.zig");
const Dbi = @import("Dbi.zig");
const Env = @import("Env.zig");

const log = std.log.scoped(.lmdb);

const Txn = @This();

inner: *c.MDB_txn,
status: Status = .open,
debug: if (utils.DEBUG) Debug else void,

/// Create new transaction - See `Env.begin*(...)`
pub fn init(
    env: Env,
    src: std.builtin.SourceLocation,
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
        .SUCCESS => {
            if (utils.DEBUG) {
                if (parent) |ptxn| ptxn.debug.children += 1;
            }

            return .{
                .inner = maybe_txn.?,
                .debug = if (utils.DEBUG) .{
                    .src = src,
                    .parent = parent,
                    .access = access,
                },
            };
        },
        .PANIC => error.Panic,
        .BAD_TXN => error.BadParent,
        .MAP_RESIZED => error.MapResized,
        .READERS_FULL => error.ReadersFull,

        _ => |rc| {
            try root.lmdbUnhandledError(@src(), rc);
            unreachable;
        },

        else => |rc| switch (@as(std.posix.E, @enumFromInt(@intFromEnum(rc)))) {
            // If NO_TLS isnt set on the Env then only one read only txn per thread is allowed
            .INVAL => error.BlockedByReadOnlyTxn,
            .NOMEM => error.OutOfMemory,

            else => {
                try root.lmdbUnhandledError(@src(), rc);
                unreachable;
            },
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

    if (utils.DEBUG) {
        if (this.debug.access == .read_only) {
            utils.printWithSrc(
                this.debug.src,
                "commit() called on {t} {*}",
                .{ this.debug.access, this },
            );
            return error.BadAccess;
        }
    }

    return switch (@as(std.posix.E, @enumFromInt(
        c.mdb_txn_commit(this.inner),
    ))) {
        .SUCCESS => {
            this.status = .committed;

            if (utils.DEBUG) {
                if (this.debug.parent) |ptxn| ptxn.debug.children -= 1;
            }
        },
        .INVAL => {
            this.status = .invalid;
            return error.Invalid;
        },
        .NOSPC => error.NoSpaceLeft,
        .NOMEM => error.OutOfMemory,
        .IO => error.IoError,

        else => |rc| {
            try root.lmdbUnhandledError(@src(), rc);
            unreachable;
        },
    };
}

/// Abandon all changes made by this transaction
/// Sets `status` to `.aborted`
pub fn abort(this: *Txn) void {
    switch (this.status) {
        .open, .reset => {},
        else => return,
    }

    if (utils.DEBUG) {
        if (this.debug.parent) |ptxn| ptxn.debug.children -= 1;
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

    if (utils.DEBUG) {
        if (this.debug.access != .read_only) {
            utils.printWithSrc(
                this.debug.src,
                "reset() called on {t} {*}",
                .{ this.debug.access, this },
            );
            return error.BadAccess;
        }

        if (this.debug.children > 0) {
            utils.printWithSrc(
                this.debug.src,
                "reset() called on {t} {*} with {d} children",
                .{ this.status, this, this.debug.children },
            );
            return error.TxnHasChildren;
        }

        this.debug.parent = null; // TODO: docs dont mention if this happens or not
    }

    defer this.status = .reset;
    c.mdb_txn_reset(this.inner);
}

/// Renew a reset read only txn
pub fn renew(this: *Txn) !void {
    switch (this.status) {
        .reset => {},
        else => unreachable,
    }

    if (utils.DEBUG) std.debug.assert(this.debug.access == .read_only);

    switch (root.errno(c.mdb_txn_renew(this.inner))) {
        .SUCCESS => this.status = .open,
        else => {
            this.status = .invalid;
            return error.Failed;
        },
    }
}

pub inline fn get(this: Txn, dbi: Dbi, key: []const u8) !?[]u8 {
    return dbi.get(this, key);
}

pub inline fn get_const(this: Txn, dbi: Dbi, key: []const u8) !?[]const u8 {
    return dbi.get_const(this, key);
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

/// returns true if deleted, false if not found, error otherwise
pub inline fn del(this: Txn, dbi: Dbi, key: []const u8, data: ?[]const u8) !bool {
    return dbi.del(this, key, data);
}

pub inline fn cursor(txn: *const Txn, src: std.builtin.SourceLocation, dbi: Dbi) !Cursor {
    return Cursor.init(src, dbi, txn);
}

const Debug = struct {
    src: std.builtin.SourceLocation,
    access: Access,

    children: usize = 0,
    parent: ?*Txn = null,
};

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
