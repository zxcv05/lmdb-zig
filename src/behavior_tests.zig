const std = @import("std");
const lib = @import("root.zig");

fn create_env() !struct { lib.Env, lib.Dbi } {
    const env: lib.Env = try .init("/tmp/behavior.mdb", .{ .flags = .{ .no_subdir = true } });
    errdefer env.deinit();

    const dbi: lib.Dbi = init_dbi: {
        var txn = try env.begin(.read_write, .{});
        defer txn.abort();

        const dbi: lib.Dbi = try .init(txn, null, .{ .dup_sort = true });
        try txn.commit();

        break :init_dbi dbi;
    };

    return .{ env, dbi };
}

test "simple write-then-read" {
    const env, const dbi = try create_env();
    defer env.deinit();

    var txn = try env.begin(.read_write, .{});
    defer txn.abort();

    const cursor = dbi.cursor(txn);
    defer cursor.deinit();

    try cursor.put("hello", "world", .{});
    const kv = try cursor.get("hello", null, .set) orelse return error.Failed;

    try std.testing.expectEqualStrings("hello", kv.key);
    try std.testing.expectEqualStrings("world", kv.value);
}

test "nested transactions" {
    const env, const dbi = try create_env();
    defer env.deinit();

    {
        var txn_1 = try env.begin(.read_write, .{});
        defer txn_1.abort();

        const cursor_1 = dbi.cursor(txn_1);
        defer cursor_1.deinit();

        var txn_2 = try env.begin_nested(&txn_1, .read_write, .{});
        defer txn_2.abort();

        const cursor_2 = dbi.cursor(txn_2);
        defer cursor_2.deinit();

        try cursor_1.put("\x01", "wow", .{});
        try cursor_2.put("\x02", "yay", .{});
        try cursor_1.put("\x03", "huh", .{});
        try txn_2.commit();

        try cursor_1.put("\x04", "pip", .{});
        try txn_1.commit();
    }

    var txn = try env.begin(.read_only, .{});
    defer txn.abort();

    const cursor = dbi.cursor(txn);

    const kv1 = try cursor.get(.set, "\x01", null) orelse return error.Failed;
    const kv2 = try cursor.get(.next, null, null) orelse return error.Failed;
    const kv3 = try cursor.get(.next, null, null) orelse return error.Failed;
    const kv4 = try cursor.get(.next, null, null) orelse return error.Failed;

    try std.testing.expectEqualStrings("wow", kv1.value);
    try std.testing.expectEqualStrings("yay", kv2.value);
    try std.testing.expectEqualStrings("huh", kv3.value);
    try std.testing.expectEqualStrings("pip", kv4.value);
}
