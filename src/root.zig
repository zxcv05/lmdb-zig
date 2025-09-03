const std = @import("std");
const c = @import("c");

pub const Cursor = @import("Cursor.zig");
pub const Dbi = @import("Dbi.zig");
pub const Env = @import("Env.zig");
pub const Txn = @import("Txn.zig");

const log = std.log.scoped(.lmdb);

// zig fmt: off
pub const E = enum(c_int) {
    SUCCESS = c.MDB_SUCCESS,
    PROBLEM = c.MDB_PROBLEM,

    VERSION_MISMATCH = c.MDB_VERSION_MISMATCH,
    INCOMPATIBLE  = c.MDB_INCOMPATIBLE,
    MAP_RESIZED   = c.MDB_MAP_RESIZED,
    KEYEXIST  = c.MDB_KEYEXIST,
    NOTFOUND  = c.MDB_NOTFOUND,
    INVALID   = c.MDB_INVALID,
    PANIC     = c.MDB_PANIC,

    READERS_FULL = c.MDB_READERS_FULL,
    TXN_FULL = c.MDB_TXN_FULL,
    MAP_FULL = c.MDB_MAP_FULL,
    DBS_FULL = c.MDB_DBS_FULL,
    TLS_FULL = c.MDB_TLS_FULL,

    BAD_VALSIZE  = c.MDB_BAD_VALSIZE,
    BAD_RSLOT    = c.MDB_BAD_RSLOT,
    BAD_TXN      = c.MDB_BAD_TXN,
    BAD_DBI      = c.MDB_BAD_DBI,

    CORRUPTED      = c.MDB_CORRUPTED,
    PAGE_NOTFOUND  = c.MDB_PAGE_NOTFOUND,
    CURSOR_FULL    = c.MDB_CURSOR_FULL,
    PAGE_FULL      = c.MDB_PAGE_FULL,
    _,
}; // zig fmt: on

// zig fmt: off
pub const all_flags = struct {
    // environment flags
    pub const fixed_map     = c.MDB_FIXEDMAP;
    pub const no_subdir     = c.MDB_NOSUBDIR;
    pub const no_sync       = c.MDB_NOSYNC;
    pub const read_only     = c.MDB_RDONLY;
    pub const no_meta_sync  = c.MDB_NOMETASYNC;
    pub const write_map     = c.MDB_WRITEMAP;
    pub const map_async     = c.MDB_MAPASYNC;
    pub const no_tls        = c.MDB_NOTLS;
    pub const no_lock       = c.MDB_NOLOCK;
    pub const no_read_ahead = c.MDB_NORDAHEAD;
    pub const no_mem_init   = c.MDB_NOMEMINIT;
    pub const previous_snapshot = c.MDB_PREVSNAPSHOT;
    // db flags
    pub const reverse_key   = c.MDB_REVERSEKEY;
    pub const dup_sort      = c.MDB_DUPSORT;
    pub const integer_key   = c.MDB_INTEGERKEY;
    pub const dup_fixed     = c.MDB_DUPFIXED;
    pub const integer_dup   = c.MDB_INTEGERDUP;
    pub const reverse_dup   = c.MDB_REVERSEDUP;
    pub const create        = c.MDB_CREATE;
    // write flags
    pub const no_overwrite  = c.MDB_NOOVERWRITE;
    pub const no_dup_data   = c.MDB_NODUPDATA;
    pub const current       = c.MDB_CURRENT;
    pub const reserve       = c.MDB_RESERVE;
    pub const append        = c.MDB_APPEND;
    pub const append_dup    = c.MDB_APPENDDUP;
    pub const multiple      = c.MDB_MULTIPLE;
    // copy flags
    pub const cp_compact    = c.MDB_CP_COMPACT;
}; // zig fmt: on

pub fn errno(rc: anytype) E {
    return @enumFromInt(rc);
}

pub fn lmdbUnhandledError(src: std.builtin.SourceLocation, rc: anytype) !void {
    const PANIC_ON_UNHANDLED_ERROR = false;

    log.err("{s}: Unhandled error: {any}", .{ src.fn_name, rc });
    if (PANIC_ON_UNHANDLED_ERROR) @panic("trap: unhandled error");
    return error.UnknownError;
}

test {
    try std.testing.expectEqual(
        @intFromEnum(std.posix.E.SUCCESS),
        @intFromEnum(E.SUCCESS),
    );

    // TODO: this hack only works because the test runner is single threaded.
    //       will need to revisit if/when that changes
    std.fs.cwd().makeDir("testdb") catch {};
    std.fs.cwd().deleteFile("testdb/data.mdb") catch {};
    std.fs.cwd().deleteFile("testdb/lock.mdb") catch {};

    std.testing.refAllDeclsRecursive(@This());
    std.testing.refAllDeclsRecursive(@import("Val.zig"));
    std.testing.refAllDecls(@import("behavior_tests.zig"));
}
