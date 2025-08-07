const std = @import("std");

pub const DEBUG = switch (@import("builtin").mode) {
    .Debug, .ReleaseSafe => true,
    .ReleaseFast, .ReleaseSmall => false,
};

pub fn printWithSrc(
    src: std.builtin.SourceLocation,
    comptime fmt: []const u8,
    args: anytype,
) void {
    if (@import("builtin").is_test) return;

    std.debug.print("lmdb: " ++ fmt ++ "\n- \x1b[2m[hint: Check {s}.{s}@{s}:{d}:{d}]\x1b[m\n", args ++ .{
        src.module,
        src.fn_name,
        src.file,
        src.line,
        src.column,
    });
}
