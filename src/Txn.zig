//! Transaction - wrappers around mdb_txn*

const root = @import("root.zig");
const std = @import("std");
const c = @import("c");

const Env = @import("Env.zig");

const Txn = @This();

inner: *c.MDB_txn,
done: bool = false,

/// Create new transaction
pub fn init(env: Env, parent: ?*Txn, access: Access, flags: Flags) !Txn {
    if (parent) |ptxn| std.debug.assert(!ptxn.done);

    var flags_int: c_uint = 0;
    inline for (std.meta.fields(Flags)) |flag| {
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
        .SUCCESS => return .{ .inner = maybe_txn.? },
        .PANIC => return error.Panic,
        .MAP_RESIZED => return error.MapResized,
        .READERS_FULL => return error.ReadersFull,
        else => |rc| switch (std.posix.errno(@intFromEnum(rc))) {
            .NOMEM => return error.OutOfMemory,
            else => unreachable,
        },
    }
}

/// Commit transaction's changes to db
pub fn commit(this: *Txn) !void {
    if (this.done) return;
    this.done = true;

    switch (std.posix.errno(
        c.mdb_txn_commit(this.inner),
    )) {
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
    this.done = true;

    c.mdb_txn_abort(this.inner);
}

/// Reset then renew a read-only transaction (optimization)
pub fn reset_renew(this: *Txn) !void {
    c.mdb_txn_reset(this.inner);

    switch (root.errno(c.mdb_txn_renew(this.inner))) {
        .SUCCESS => this.done = false,
        else => return error.RenewFailed,
    }
}

pub const Access = enum {
    read_only,
    read_write,
};

pub const Flags = packed struct {
    no_sync: bool = false,
    no_meta_sync: bool = false,
};
