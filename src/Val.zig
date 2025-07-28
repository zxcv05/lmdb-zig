//! Value - wrappers around mdb_val*

const std = @import("std");
const c = @import("c");

const Val = @This();

data: c.MDB_val,

/// Intended to be overwritten by a c function call
pub const empty: Val = .{ .data = std.mem.zeroes(c.MDB_val) };

/// Create a MDB_val from a zig slice
pub inline fn from(maybe_slice: ?[]u8) Val {
    return if (maybe_slice) |slice| .{ .data = .{
        .mv_size = @intCast(slice.len),
        .mv_data = @ptrCast(slice.ptr),
    } } else .empty;
}

/// Same as `from` but with a const slice
pub inline fn from_const(maybe_slice: ?[]const u8) Val {
    return from(@constCast(maybe_slice));
}

/// For use with `Dbi.put*` with the `reserve` flag set
pub inline fn of_size(size: usize) Val {
    return .{ .data = .{
        .mv_size = @intCast(size),
        .mv_data = undefined,
    } };
}

/// return a pointer compatible with [*c]c.MDB_val
pub inline fn alias(this: *const Val) ?*c.MDB_val {
    return @constCast(&this.data);
}

//

/// Unwraps the MDB_val struct into a slice, if possible
pub inline fn unalias_maybe(this: Val) ?[]u8 {
    if (this.data.mv_size == 0 or @intFromPtr(this.data.mv_data) == 0) return null;

    const ptr: [*]u8 = @ptrCast(this.data.mv_data);
    return ptr[0..this.data.mv_size];
}

/// Same as `unalias_maybe` except asserts non-null
pub inline fn unalias(this: Val) []u8 {
    return this.unalias_maybe().?;
}
