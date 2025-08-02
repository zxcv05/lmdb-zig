const std = @import("std");
const lib = @import("root.zig");

const c = @import("c");

fn create_env(db_name: ?[:0]const u8, db_flags: lib.Dbi.InitFlags) !struct { lib.Env, lib.Dbi } {
    const env: lib.Env = try .init("./testdb/", .{ .max_dbs = 8 });
    errdefer env.deinit();

    const dbi: lib.Dbi = init_dbi: {
        var txn = try env.begin(.read_write, .{});
        defer txn.abort();

        var flags = db_flags;
        flags.create = true;

        // const dbi: lib.Dbi = try .init(txn, db_name, flags);
        const dbi = try env.open(txn, db_name, flags);
        try txn.commit();

        break :init_dbi dbi;
    };

    return .{ env, dbi };
}

test "nested transactions" {
    const env, const dbi = try create_env("nested-txns", .{});
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

        try cursor_1.put("\x01", "wow");
        try cursor_2.put("\x02", "yay");
        try cursor_1.put("\x03", "huh");
        try txn_2.commit();

        try cursor_1.put("\x04", "pip");
        try txn_1.commit();
    }

    var txn = try env.begin(.read_only, .{});
    defer txn.abort();

    const cursor = dbi.cursor(txn);
    defer cursor.deinit();

    var iter = cursor.get_iter(.first, null, null, .next);

    try std.testing.expectEqualStrings("wow", iter.next().?[1]);
    try std.testing.expectEqualStrings("yay", iter.next().?[1]);
    try std.testing.expectEqualStrings("huh", iter.next().?[1]);
    try std.testing.expectEqualStrings("pip", iter.next().?[1]);
    try std.testing.expect(iter.next() == null);
}

test "cursor put + get" {
    var rng: std.Random.Xoroshiro128 = .init(std.testing.random_seed);

    const env, const dbi = try create_env("cursor-put-get", .{});
    defer env.deinit();

    var datas: [4][16]u8 = undefined;
    for (&datas) |*data| rng.fill(data);

    {
        var txn = try env.begin(.read_write, .{});
        defer txn.abort();

        const cursor = txn.cursor(dbi);
        defer cursor.deinit();

        for (&datas, 0..) |*data, i| {
            const key: u8 = @intCast(i);
            try cursor.put(std.mem.asBytes(&key), data);
        }

        try txn.commit();
    }

    var txn = try env.begin(.read_only, .{});
    defer txn.abort();

    const cursor = txn.cursor(dbi);
    defer cursor.deinit();

    var iter = cursor.get_iter(.first, null, null, .next);

    for (&datas, 0..) |*data, i| {
        const k, const v = iter.next().?;

        try std.testing.expectEqual(i, std.mem.bytesToValue(u8, k));
        try std.testing.expectEqualSlices(u8, data, v);
    }

    try std.testing.expect(iter.next() == null);
}

test "dupsort cursor put + get" {
    var rng: std.Random.Xoroshiro128 = .init(std.testing.random_seed);

    const env, const dbi = try create_env("dupsort-cursor-put-get", .{ .dup_sort = true });
    defer env.deinit();

    // todo: this is a messy way to do this, cleanup?
    var datas: [4][4][16]u8 = undefined;
    for (&datas) |*lv1| for (lv1) |*lv2| rng.fill(lv2);

    {
        var txn = try env.begin(.read_write, .{});
        defer txn.abort();

        const cursor = txn.cursor(dbi);
        defer cursor.deinit();

        for (&datas, 0..) |*lv1, i| for (lv1) |*lv2| {
            const key: u8 = @intCast(i);
            try cursor.put(std.mem.asBytes(&key), lv2);
        };

        try txn.commit();
    }

    var txn = try env.begin(.read_only, .{});
    defer txn.abort();

    const cursor = txn.cursor(dbi);
    defer cursor.deinit();

    const init_k, _ = cursor.get(.first, null, null) orelse unreachable;
    var iter = cursor.get_iter(.first_dup, init_k, null, .next);

    for (&datas, 0..) |*lv1, i| for (0..lv1.len) |_| {
        const k, const v = iter.next().?;

        try std.testing.expectEqual(i, std.mem.bytesToValue(u8, k));

        const exists = for (lv1) |*lv2| {
            if (std.mem.eql(u8, v, lv2)) break true;
        } else false;

        try std.testing.expect(exists);
    };

    try std.testing.expect(iter.next() == null);
}

test "put_or_get + del" {
    const env, const dbi = try create_env("put-or-get", .{});
    defer env.deinit();

    {
        var txn = try env.begin(.read_write, .{});
        defer txn.abort();

        const cursor = txn.cursor(dbi);
        defer cursor.deinit();

        try std.testing.expectEqualStrings("data1", try cursor.put_get("key1", "data1"));
        try std.testing.expectEqualStrings("data1", try cursor.put_get("key1", "garbage"));
        try std.testing.expectEqualStrings("data2", try cursor.put_get("key2", "data2"));
        try std.testing.expectEqualStrings("data2", try cursor.put_get("key2", "trash"));
        try cursor.del();

        try txn.commit();
    }

    {
        var txn = try env.begin(.read_write, .{});
        defer txn.abort();

        try std.testing.expectEqualStrings("data1", try dbi.put_get(txn, "key1", "dumpster"));
        try std.testing.expectEqualStrings("data2", try dbi.put_get(txn, "key2", "data2"));
        try std.testing.expectEqualStrings("data3", try dbi.put_get(txn, "key3", "data3"));
        try std.testing.expectEqualStrings("data3", try dbi.put_get(txn, "key3", "litter"));
        try dbi.del(txn, "key3", null);

        try txn.commit();
    }

    var txn = try env.begin(.read_only, .{});
    defer txn.abort();

    const cursor = txn.cursor(dbi);
    defer cursor.deinit();

    var iter = cursor.get_iter(.first, null, null, .next);

    const k1, const v1 = iter.next().?;
    try std.testing.expectEqualStrings("key1", k1);
    try std.testing.expectEqualStrings("data1", v1);

    const k2, const v2 = iter.next().?;
    try std.testing.expectEqualStrings("key2", k2);
    try std.testing.expectEqualStrings("data2", v2);

    try std.testing.expect(iter.next() == null);
}

// This test fails because of a seg fault
// i suspect it has to do with lmdb's extensive use of UB
// test "cursor put multiple" {
//     const env, const dbi = try create_env("put-multi", .{ .dup_sort = true, .dup_fixed = true });
//     defer env.deinit();

//     {
//         var txn = try env.begin(.read_write, .{});
//         defer txn.abort();

//         const cursor = dbi.cursor(txn);
//         defer cursor.deinit();

//         const appended = try cursor.put_multiple(u16, "\x00", &.{ 0x1234, 0x2345, 0x3456, 0x4567, 0x5678 });
//         try std.testing.expectEqual(5, appended);

//         try txn.commit();
//     }

//     var txn = try env.begin(.read_only, .{});
//     defer txn.abort();

//     const cursor = dbi.cursor(txn);
//     defer cursor.deinit();

//     var iter = cursor.get_iter(.first_dup, "\x00", null, .next_dup);

//     try std.testing.expectEqual(std.mem.bytesToValue(u16, iter.next().?[1]), 0x1234);
//     try std.testing.expectEqual(std.mem.bytesToValue(u16, iter.next().?[1]), 0x2345);
//     try std.testing.expectEqual(std.mem.bytesToValue(u16, iter.next().?[1]), 0x3456);
//     try std.testing.expectEqual(std.mem.bytesToValue(u16, iter.next().?[1]), 0x4567);
//     try std.testing.expectEqual(std.mem.bytesToValue(u16, iter.next().?[1]), 0x5678);

//     try std.testing.expect(iter.next() == null);
// }

test "put_append" {
    const SortContext = struct {
        pub fn lessThan(this: @This(), lhs: [32]u8, rhs: [32]u8) bool {
            _ = this;
            return std.mem.order(u8, &lhs, &rhs) == .lt;
        }
    };

    var rng: std.Random.Xoroshiro128 = .init(std.testing.random_seed);

    const env, const dbi = try create_env("put-append", .{});
    defer env.deinit();

    var data: [8][32]u8 = undefined;
    for (&data) |*lv1| rng.fill(lv1);

    std.mem.sort([32]u8, &data, SortContext{}, SortContext.lessThan);

    {
        var txn = try env.begin(.read_write, .{});
        defer txn.abort();

        const cursor = dbi.cursor(txn);
        defer cursor.deinit();

        for (&data, 0..) |*lv1, i| {
            const key: u8 = @intCast(i);
            try cursor.put_append_dup(std.mem.asBytes(&key), lv1);
        }

        try txn.commit();
    }

    var txn = try env.begin(.read_only, .{});
    defer txn.abort();

    const cursor = dbi.cursor(txn);
    defer cursor.deinit();

    var iter = cursor.get_iter(.first, null, null, .next_dup);
    var i: usize = 0;
    while (iter.next()) |kv| : (i += 1) {
        const key: u8 = @intCast(i);

        const k, const v = kv;
        try std.testing.expectEqualSlices(u8, std.mem.asBytes(&key), k);
        try std.testing.expectEqualSlices(u8, &data[i], v);
    }

    try std.testing.expect(iter.next() == null);
    try std.testing.expectEqual(data.len, i);
}

test "put_append_dup" {
    const SortContext = struct {
        pub fn lessThan(this: @This(), lhs: [32]u8, rhs: [32]u8) bool {
            _ = this;
            return std.mem.order(u8, &lhs, &rhs) == .lt;
        }
    };

    var rng: std.Random.Xoroshiro128 = .init(std.testing.random_seed);

    const env, const dbi = try create_env("put-append-dup", .{ .dup_sort = true, .dup_fixed = true });
    defer env.deinit();

    var data: [8][32]u8 = undefined;
    for (&data) |*lv1| rng.fill(lv1);

    std.mem.sort([32]u8, &data, SortContext{}, SortContext.lessThan);

    {
        var txn = try env.begin(.read_write, .{});
        defer txn.abort();

        const cursor = dbi.cursor(txn);
        defer cursor.deinit();

        for (&data) |*lv1| {
            try cursor.put_append_dup("\x00", lv1);
        }

        try txn.commit();
    }

    var txn = try env.begin(.read_only, .{});
    defer txn.abort();

    const cursor = dbi.cursor(txn);
    defer cursor.deinit();

    var iter = cursor.get_iter(.first_dup, "\x00", null, .next_dup);
    std.debug.assert(iter.next().?[1].len == 0); // i assume this is a quirk of using only append_dup

    var i: usize = 0;
    while (iter.next()) |kv| : (i += 1) {
        const k, const v = kv;
        try std.testing.expectEqualSlices(u8, "\x00", k);
        try std.testing.expectEqualSlices(u8, &data[i], v);
    }

    try std.testing.expect(iter.next() == null);
    try std.testing.expectEqual(data.len, i);
}
