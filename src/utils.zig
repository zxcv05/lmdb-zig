const std = @import("std");

const log = std.log.scoped(.lmdb);

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

    log.err(fmt ++ " | hint: {s}.{s} in {s}:{d}:{d}", args ++ .{
        src.module,
        src.fn_name,
        src.file,
        src.line,
        src.column,
    });
}
