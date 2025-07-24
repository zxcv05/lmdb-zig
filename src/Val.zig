//! Value - wrappers around mdb_val*

const std = @import("std");
const c = @import("c");

const Val = @This();

data: c.MDB_val,

/// Intended to be overwritten by a c function call
pub const empty: Val = .{ .data = undefined };

/// Create a MDB_val from a zig slice
pub inline fn from(maybe_slice: ?[]u8) Val {
    return if (maybe_slice) |slice| .{ .data = .{
        .mv_size = @intCast(slice.len),
        .mv_data = @ptrCast(slice.ptr),
    } } else .{ .data = .{
        .mv_size = 0,
        .mv_data = @ptrFromInt(0),
    } };
}

/// Same as `from` but with a const slice
pub inline fn from_const(slice: ?[]const u8) Val {
    return from(@constCast(slice));
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

/// return a slice where MDB_val is pointing to
/// asserts non-null
pub inline fn unalias(this: Val) []u8 {
    return this.unalias_maybe().?;
}

/// Same as `unalias` but the return value is a const slice
pub inline fn unalias_const(this: Val) []const u8 {
    return this.unalias();
}

/// Same as `unalias` except
pub inline fn unalias_maybe(this: Val) ?[]u8 {
    const ptr: [*]u8 = @ptrCast(this.data.mv_data);
    return ptr[0..this.data.mv_size];
}

/// Same as `unalias_maybe` but the return value is a const slice
pub inline fn unalias_const_maybe(this: Val) ?[]const u8 {
    return this.unalias_maybe();
}
