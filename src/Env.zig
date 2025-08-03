//! Environment - wrappers around mdb_env*

const root = @import("root.zig");
const std = @import("std");
const c = @import("c");

const Dbi = @import("Dbi.zig");
const Txn = @import("Txn.zig");

const Env = @This();

inner: *c.MDB_env,

pub fn init(path: [:0]const u8, options: InitOptions) !Env {
    // create environment

    const env: *c.MDB_env = wrap: {
        var maybe_env: ?*c.MDB_env = null;
        _ = c.mdb_env_create(&maybe_env);
        break :wrap maybe_env orelse return error.EnvCreateFailed;
    };
    errdefer c.mdb_env_close(env);

    // setup environment

    if (options.max_readers) |max_readers| {
        if (c.mdb_env_set_maxreaders(env, @intCast(max_readers)) != @intFromEnum(root.E.SUCCESS)) unreachable;
    }

    if (options.map_size) |map_size| {
        if (c.mdb_env_set_mapsize(env, @intCast(map_size)) != @intFromEnum(root.E.SUCCESS)) unreachable;
    }

    if (options.max_dbs) |max_dbs| {
        if (c.mdb_env_set_maxdbs(env, @intCast(max_dbs)) != @intFromEnum(root.E.SUCCESS)) unreachable;
    }

    // open db

    var flags_int: c_uint = 0;
    inline for (std.meta.fields(InitFlags)) |flag| {
        if (@field(options.flags, flag.name))
            flags_int |= @field(root.all_flags, flag.name);
    }

    switch (root.errno(c.mdb_env_open(
        env,
        path.ptr,
        flags_int,
        @intCast(options.mode),
    ))) {
        .SUCCESS => {},
        .INVALID => return error.CorruptedHeaders,
        .VERSION_MISMATCH => return error.VersionMismatch,
        else => |rc| switch (@as(std.posix.E, @enumFromInt(@intFromEnum(rc)))) {
            .NOTDIR => return error.NotADirectory,
            .NOENT => return error.PathDoesntExist,
            .ACCES => return error.PermissionDenied,
            .AGAIN => return error.EnvironmentLocked,
            else => |rc2| {
                std.debug.print("{any} {any} {d}", .{ rc, rc2, @intFromEnum(rc) });
                unreachable;
            },
        },
    }

    return .{ .inner = env };
}

/// All transactions and cursors must already be closed before this call
pub fn deinit(this: Env) void {
    c.mdb_env_close(this.inner);
}

/// flush env to disk
pub fn sync(this: Env, force: bool) !void {
    switch (@as(std.posix.E, @enumFromInt(
        c.mdb_env_sync(this.inner, @intFromBool(force)),
    ))) {
        .SUCCESS => {},
        .ACCES => return error.ReadOnly,
        .INVAL => return error.Invalid,
        .IO => return error.IoError,
        else => unreachable,
    }
}

/// Open a database
pub fn open(this: Env, txn: Txn, name: ?[:0]const u8, flags: Dbi.InitFlags) !Dbi {
    _ = this;
    return Dbi.init(txn, name, flags);
}

/// Create a transaction
pub fn begin(this: Env, access: Txn.Access, flags: Txn.InitFlags) !Txn {
    return Txn.init(this, null, access, flags);
}

/// Create a nested transaction
pub fn begin_nested(this: Env, parent: *const Txn, access: Txn.Access, flags: Txn.InitFlags) !Txn {
    return Txn.init(this, parent, access, flags);
}

/// See `http://www.lmdb.tech/doc/group__mdb.html#ga5040d0de1f14000fa01fc0b522ff1f86`
/// return indicates success
pub fn copy_to_fd(this: Env, fd: std.posix.fd_t, flags: CopyFlags) bool {
    var flags_int: c_uint = 0;
    inline for (std.meta.fields(CopyFlags)) |flag| {
        if (@field(flags, flag.name))
            flags_int |= @field(root.all_flags, flag.name);
    }

    return c.mdb_env_copyfd2(this.inner, fd, flags_int) == @intFromEnum(root.E.SUCCESS);
}

pub fn get_stats(this: Env) c.MDB_stat {
    var stat: c.MDB_stat = undefined;
    _ = c.mdb_env_stat(this.inner, &stat);
    return stat;
}

pub fn get_info(this: Env) c.MDB_envinfo {
    var envinfo: c.MDB_envinfo = undefined;
    _ = c.mdb_env_info(this.inner, &envinfo);
    return envinfo;
}

pub fn get_fd(this: Env) std.posix.fd_t {
    var fd: std.posix.fd_t = undefined;
    _ = c.mdb_env_get_fd(this.inner, &fd);
    return fd;
}

pub const InitOptions = struct {
    /// if db doesnt exist, lmdb will create it with this mode (ignored on windows)
    mode: std.posix.mode_t = 0o664,
    flags: InitFlags = .{},

    max_readers: ?usize = null,
    map_size: ?usize = null,
    max_dbs: ?usize = null,
};

pub const InitFlags = packed struct {
    no_subdir: bool = false,
    read_only: bool = false,
    fixed_map: bool = false,
    write_map: bool = false,
    map_async: bool = false,
    no_meta_sync: bool = false,
    no_sync: bool = false,
    no_tls: bool = false,
    no_lock: bool = false,
    no_read_ahead: bool = false,
    no_mem_init: bool = false,
    previous_snapshot: bool = false,
};

pub const CopyFlags = packed struct {
    cp_compact: bool = false,
};
