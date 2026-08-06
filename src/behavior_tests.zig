const std = @import("std");
const lib = @import("root.zig");

const c = @import("c");

const log = std.log.scoped(.behavior_tests);

const SetupRes = struct { lib.Env, lib.Dbi, lib.Txn.ReadWrite };
fn setup(src: std.builtin.SourceLocation, dbi_flags: lib.Dbi.InitFlags) !SetupRes {
    const env = lib.Env.init("testdb", .{ .max_dbs = 32 }) catch |e| {
        log.err("setup failed(create env): {t}", .{e});
        return error.Unrelated;
    };
    errdefer env.deinit();

    var txn = env.begin(.read_write, .{}) catch |e| {
        log.err("setup failed(create txn): {t}", .{e});
        return error.Unrelated;
    };
    errdefer txn.abort();

    const a = std.testing.allocator;
    const dbi_name = try std.fmt.allocPrint(a, "{s}\x00", .{src.fn_name[5..]});
    defer a.free(dbi_name);

    var actual_dbi_flags = dbi_flags;
    actual_dbi_flags.create = true;

    const dbi = lib.Dbi.init(txn, @ptrCast(dbi_name), actual_dbi_flags) catch |e| {
        log.err("setup failed(create dbi): {t}", .{e});
        return error.Unrelated;
    };

    return SetupRes{ env, dbi, txn };
}

const KEY_SIZE = 8;
const DATA_SIZE = 16;

const KEYS_AMT = 8;
const DATA_AMT = 8;
const DUPS_AMT = 4;

const Key = [KEY_SIZE]u8;
const Data = [DATA_SIZE]u8;

test "minimal case" {
    const env, const dbi, var txn = try setup(@src(), .{});
    defer env.deinit();
    defer txn.abort();
    try txn.commit();
    _ = dbi;
}

test "put commit get" {
    var rng: std.Random.DefaultPrng = .init(std.testing.random_seed);

    const env, const dbi, var txn = try setup(@src(), .{});
    defer env.deinit();
    defer txn.abort();

    var ctnr: struct {
        keys: [KEYS_AMT]Key,
        datas: [DATA_AMT]Data,
    } = undefined;
    rng.fill(std.mem.asBytes(&ctnr));

    for (&ctnr.keys, &ctnr.datas) |*key, *data| {
        try txn.put(dbi, key, data);
    }
    try txn.commit();

    const ro = try env.begin(.read_only, .{});
    for (&ctnr.keys, &ctnr.datas) |*key, *expected_data| {
        const actual_data = try ro.get(dbi, key) orelse return error.NotFound;
        try std.testing.expectEqualSlices(u8, expected_data, actual_data);
    }
}

test "put commit get, cursor" {
    var rng: std.Random.DefaultPrng = .init(std.testing.random_seed);

    const env, const dbi, var txn = try setup(@src(), .{});
    defer env.deinit();
    defer txn.abort();

    var ctnr: struct {
        keys: [KEYS_AMT]Key,
        datas: [DATA_AMT]Data,
    } = undefined;
    rng.fill(std.mem.asBytes(&ctnr));

    {
        const cursor = try txn.cursor(dbi);
        for (&ctnr.keys, &ctnr.datas) |*key, *data| {
            try cursor.put(key, data);
        }

        try txn.commit();
    }

    const ro = try env.begin(.read_only, .{});
    const ro_cur = try ro.cursor(dbi);
    defer ro_cur.deinit();

    for (&ctnr.keys, &ctnr.datas) |*key, *expected_data| {
        _, const actual_data = ro_cur.get(.set_key, key, null) orelse return error.NotFound;
        try std.testing.expectEqualSlices(u8, expected_data, actual_data);
    }
}

test "put commit get, dupsort" {
    var rng: std.Random.DefaultPrng = .init(std.testing.random_seed);

    const env, const dbi, var txn = try setup(@src(), .{ .dup_sort = true });
    defer env.deinit();
    defer txn.abort();

    var ctnr: struct {
        keys: [KEYS_AMT]Key,
        datass: [KEYS_AMT][DUPS_AMT]Data,
    } = undefined;
    rng.fill(std.mem.asBytes(&ctnr));

    for (&ctnr.keys, &ctnr.datass) |*key, *datas| {
        for (datas) |*data| {
            try txn.put(dbi, key, data);
        }
    }

    try txn.commit();

    const ro = try env.begin(.read_only, .{});
    const ro_cur = try ro.cursor(dbi);
    defer ro_cur.deinit();

    for (&ctnr.keys, &ctnr.datass) |*key, *datas| {
        _ = ro_cur.get(.set, key, null) orelse return error.NotFound;
        var dup_iter = ro_cur.iterator(.first_dup, key, null, .next_dup);

        while (dup_iter.next()) |dkv| {
            const dk, const dv = dkv;
            try std.testing.expectEqualSlices(u8, key, dk);

            for (datas) |*data| {
                if (std.mem.eql(u8, data, dv)) break;
            } else {
                return error.UnmatchedDup;
            }
        }
    }
}

test "putNoClobber" {
    var rng: std.Random.DefaultPrng = .init(std.testing.random_seed);

    const env, const dbi, var txn = try setup(@src(), .{ .dup_sort = true });
    defer env.deinit();
    defer txn.abort();

    var ctnr: struct {
        keys: [KEYS_AMT]Key,
        datas: [DATA_AMT]Data,
    } = undefined;
    rng.fill(std.mem.asBytes(&ctnr));

    for (&ctnr.keys, &ctnr.datas) |*key, *data| {
        try txn.putNoClobber(dbi, key, data);
        try std.testing.expectError(error.AlreadyExists, txn.putNoClobber(dbi, key, data));
    }
}

test "putGet" {
    var rng: std.Random.DefaultPrng = .init(std.testing.random_seed);

    const env, const dbi, var txn = try setup(@src(), .{});
    defer env.deinit();
    defer txn.abort();

    var ctnr: struct {
        keys: [KEYS_AMT]Key,
        datas: [DATA_AMT]Data,
    } = undefined;
    rng.fill(std.mem.asBytes(&ctnr));

    for (&ctnr.keys, &ctnr.datas) |*key, *data| {
        try std.testing.expectEqualSlices(u8, data, try txn.putGet(dbi, key, data));
        try std.testing.expectEqualSlices(u8, data, try txn.putGet(dbi, key, "garbage"));
    }
}

test "sort put commit get, putAppend" {
    var rng: std.Random.DefaultPrng = .init(std.testing.random_seed);

    const env, const dbi, var txn = try setup(@src(), .{});
    defer env.deinit();
    defer txn.abort();

    var ctnr: struct {
        keys: [KEYS_AMT]Key,
        datas: [DATA_AMT]Data,
    } = undefined;
    rng.fill(std.mem.asBytes(&ctnr));

    const SortCtx = struct {
        pub fn lessThan(_: @TypeOf(.{}), a: Key, b: Key) bool {
            return std.mem.order(u8, &a, &b) == .lt;
        }
    };
    std.mem.sort(Key, &ctnr.keys, .{}, SortCtx.lessThan);

    for (&ctnr.keys, &ctnr.datas) |*key, *data| {
        try txn.putAppend(dbi, key, data);
    }

    try txn.commit();

    const ro = try env.begin(.read_only, .{});
    const ro_cur = try ro.cursor(dbi);
    defer ro_cur.deinit();

    var iter = ro_cur.iterator(.first, null, null, .next);
    for (&ctnr.keys, &ctnr.datas) |*key, *data| {
        const k, const v = iter.next() orelse return error.NotFound;
        try std.testing.expectEqualSlices(u8, key, k);
        try std.testing.expectEqualSlices(u8, data, v);
    }

    try std.testing.expect(iter.next() == null);
}

test "sort put commit get, putAppendDup" {
    var rng: std.Random.DefaultPrng = .init(std.testing.random_seed);

    const env, const dbi, var txn = try setup(@src(), .{ .dup_sort = true });
    defer env.deinit();
    defer txn.abort();

    var ctnr: struct {
        keys: [KEYS_AMT]Key,
        datass: [KEYS_AMT][DUPS_AMT]Data,
    } = undefined;
    rng.fill(std.mem.asBytes(&ctnr));

    // sort

    const KeySortCtx = struct {
        pub fn lessThan(_: @TypeOf(.{}), a: Key, b: Key) bool {
            return std.mem.order(u8, &a, &b) == .lt;
        }
    };
    std.mem.sort(Key, &ctnr.keys, .{}, KeySortCtx.lessThan);

    const DataSortCtx = struct {
        pub fn lessThan(_: @TypeOf(.{}), a: Data, b: Data) bool {
            return std.mem.order(u8, &a, &b) == .lt;
        }
    };
    for (&ctnr.datass) |*datas| std.mem.sort(Data, datas, .{}, DataSortCtx.lessThan);

    // put

    for (&ctnr.keys, &ctnr.datass) |*key, *datas| {
        for (datas) |*data| {
            try txn.putAppendDup(dbi, key, data);
        }
    }

    // commit

    try txn.commit();

    // get

    const ro = try env.begin(.read_only, .{});
    const ro_cur = try ro.cursor(dbi);
    defer ro_cur.deinit();

    var key_iter = ro_cur.iterator(.first, null, null, .next);
    for (&ctnr.keys, &ctnr.datass) |*key, *datas| {
        const k, _ = key_iter.next() orelse return error.NotFound;
        try std.testing.expectEqualSlices(u8, key, k);

        var dup_iter = ro_cur.iterator(.first_dup, k, null, .next_dup);
        for (datas) |*data| {
            _, const dv = dup_iter.next() orelse return error.NotFound;
            try std.testing.expectEqualSlices(u8, data, dv);
        }

        try std.testing.expect(dup_iter.next() == null);
    }
    try std.testing.expect(key_iter.next() == null);
}

test "put commit get, putReserve" {
    var rng: std.Random.DefaultPrng = .init(std.testing.random_seed);

    const env, const dbi, var txn = try setup(@src(), .{});
    defer env.deinit();
    defer txn.abort();

    var ctnr: struct {
        keys: [KEYS_AMT]Key,
        datas: [DATA_AMT]Data,
    } = undefined;
    rng.fill(std.mem.asBytes(&ctnr));

    for (&ctnr.keys, &ctnr.datas) |*key, *data| {
        const buf = try txn.putReserve(dbi, key, data.len);
        @memcpy(buf, data);
    }
    try txn.commit();

    const ro = try env.begin(.read_only, .{});
    for (&ctnr.keys, &ctnr.datas) |*key, *expected_data| {
        const actual_data = try ro.get(dbi, key) orelse return error.NotFound;
        try std.testing.expectEqualSlices(u8, expected_data, actual_data);
    }
}

test "put commit get, del odd keys" {
    var rng: std.Random.DefaultPrng = .init(std.testing.random_seed);

    const env, const dbi, var txn = try setup(@src(), .{});
    defer env.deinit();
    defer txn.abort();

    var ctnr: struct {
        keys: [KEYS_AMT]Key,
        datas: [DATA_AMT]Data,
    } = undefined;
    rng.fill(std.mem.asBytes(&ctnr));

    for (&ctnr.keys, &ctnr.datas) |*key, *data| {
        try txn.put(dbi, key, data);
    }

    for (&ctnr.keys, &ctnr.datas, 0..) |*key, *data, i| {
        if (i % 2 == 0) continue;
        try std.testing.expect(try txn.del(dbi, key, data));
    }

    try txn.commit();

    const ro = try env.begin(.read_only, .{});
    for (&ctnr.keys, &ctnr.datas, 0..) |*key, *data, i| {
        const res = try ro.get(dbi, key);

        if (i % 2 == 0)
            try std.testing.expectEqualSlices(u8, data, res orelse return error.NotFound)
        else
            try std.testing.expect(res == null);
    }
}

test "put commit get, cursor del odd keys" {
    var rng: std.Random.DefaultPrng = .init(std.testing.random_seed);

    const env, const dbi, var txn = try setup(@src(), .{});
    defer env.deinit();
    defer txn.abort();

    var ctnr: struct {
        keys: [KEYS_AMT]Key,
        datas: [DATA_AMT]Data,
    } = undefined;
    rng.fill(std.mem.asBytes(&ctnr));

    // note: this is to make the test code easier to understand
    const SortCtx = struct {
        pub fn lessThan(_: @TypeOf(.{}), a: Key, b: Key) bool {
            return std.mem.order(u8, &a, &b) == .lt;
        }
    };
    std.mem.sort(Key, &ctnr.keys, .{}, SortCtx.lessThan);

    // put

    var cursor = try txn.cursor(dbi);

    for (&ctnr.keys, &ctnr.datas) |*key, *data| {
        try cursor.putAppend(key, data);
    }

    // del

    _ = cursor.get(.first, null, null).?;
    for (0..@divExact(KEYS_AMT, 2)) |_| {
        _ = cursor.get(.next, null, null).?;
        try cursor.del();
        _ = cursor.get(.next, null, null); // only null when last iteration
    }

    // commit

    try txn.commit();

    // get

    const ro = try env.begin(.read_only, .{});
    const ro_cur = try ro.cursor(dbi);
    defer ro_cur.deinit();

    for (&ctnr.keys, &ctnr.datas, 0..) |*key, *data, i| {
        const res = ro_cur.get(.set_key, key, null);

        if (i % 2 == 0) {
            _, const v = res orelse return error.NotFound;
            try std.testing.expectEqualSlices(u8, data, v);
        } else {
            try std.testing.expect(res == null);
        }
    }
}

test "put commit get, empty contents" {
    var rng: std.Random.DefaultPrng = .init(std.testing.random_seed);

    const env, const dbi, var txn = try setup(@src(), .{});
    defer env.deinit();
    defer txn.abort();

    var ctnr: struct {
        keys: [KEYS_AMT]Key,
        datas: [DATA_AMT]Data,
    } = undefined;
    rng.fill(std.mem.asBytes(&ctnr));

    for (&ctnr.keys, &ctnr.datas) |*key, *data| {
        try txn.put(dbi, key, data);
    }

    try std.testing.expect(dbi.emptyContents(txn.base));
    try txn.commit();

    const ro = try env.begin(.read_only, .{});
    for (&ctnr.keys) |*key| {
        try std.testing.expect(try ro.get(dbi, key) == null);
    }
}

test "putMultiple commit getMultiple" {
    var rng: std.Random.DefaultPrng = .init(std.testing.random_seed);

    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();

    const env, const dbi, var txn = try setup(@src(), .{ .dup_sort = true, .dup_fixed = true });
    defer env.deinit();
    defer txn.abort();

    var ctnr: struct {
        keys: [KEYS_AMT]Key,
        datas: [DATA_AMT][]u32,
        dests: [DATA_AMT][]u32,
    } = undefined;
    rng.fill(std.mem.asBytes(&ctnr.keys));

    // init

    for (&ctnr.datas) |*data| data.* = try arena.allocator().alloc(u32, 1024);
    for (&ctnr.dests) |*dest| dest.* = try arena.allocator().alloc(u32, 1024);

    var count: u32 = 0;
    for (&ctnr.datas) |data| {
        for (data) |*elem| {
            defer count += 1;
            elem.* = count;
        }
    }

    // put

    var cursor = try txn.cursor(dbi);

    for (&ctnr.keys, &ctnr.datas) |*key, data| {
        var head: usize = 0;
        while (head < ctnr.datas[0].len) {
            head += try cursor.putMultiple(u32, key, @ptrCast(data[head..]));
        }
    }

    // commmit

    try txn.commit();

    // get

    const ro = try env.begin(.read_only, .{});
    const ro_cur = try ro.cursor(dbi);
    defer ro_cur.deinit();

    for (&ctnr.keys, &ctnr.datas, &ctnr.dests) |*key, data, dest| {
        var head: usize = 0;

        _ = ro_cur.get(.set, key, null) orelse return error.NotFound;
        while (ro_cur.getMultiple(u32, .next, key)) |page| {
            defer head += page.len;
            @memmove(dest[head..][0..page.len], page);
        }

        try std.testing.expectEqual(data.len, head);
        for (dest) |b| try std.testing.expect(b < count);
    }
}

test "cursor count" {
    var rng: std.Random.DefaultPrng = .init(std.testing.random_seed);

    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();

    const env, const dbi, var txn = try setup(@src(), .{ .dup_sort = true });
    defer env.deinit();
    defer txn.abort();

    var cursor = try txn.cursor(dbi);

    var keys: [KEYS_AMT]Key = undefined;
    rng.fill(std.mem.asBytes(&keys));

    for (&keys, 1..) |*key, i| {
        for (0..i) |j| {
            const data: u128 = j; // we dont care what data is, we're counting dups
            try cursor.putAppendDup(key, std.mem.asBytes(&data));
        }
    }

    try txn.commit();

    const ro = try env.begin(.read_only, .{});
    const ro_cur = try ro.cursor(dbi);
    defer ro_cur.deinit();

    for (&keys, 1..) |*key, i| {
        _ = ro_cur.get(.set, key, null) orelse return error.NotFound;
        try std.testing.expectEqual(ro_cur.count().?, i);
    }
}

test "cursor delAll" {
    const env, const dbi, var txn = try setup(@src(), .{ .dup_sort = true });
    defer env.deinit();
    defer txn.abort();

    var rw_cur = try txn.cursor(dbi);

    for (0..2) |i| {
        const key: u8 = @intCast(i);

        for (0..DUPS_AMT) |j| {
            const data: u128 = j;
            try rw_cur.put(std.mem.asBytes(&key), std.mem.asBytes(&data));
        }
    }

    _ = rw_cur.get(.set, &.{0}, null);
    try rw_cur.delAll();
    try txn.commit();

    const ro = try env.begin(.read_only, .{});
    const ro_cur = try ro.cursor(dbi);
    defer ro_cur.deinit();

    try std.testing.expect(ro_cur.get(.set, &.{0}, null) == null);
    try std.testing.expectEqual(null, ro_cur.count());

    try std.testing.expect(ro_cur.get(.set, &.{1}, null) != null);
    try std.testing.expectEqual(DUPS_AMT, ro_cur.count() orelse return error.NotFound);
}

test "put commit get, putReplace" {
    const env, const dbi, var txn = try setup(@src(), .{ .dup_sort = true });
    defer env.deinit();
    defer txn.abort();

    var cursor = try txn.cursor(dbi);

    for (0..4) |i| {
        const key: u8 = @intCast(i);
        const data: u8 = 1;
        try cursor.putAppend(&.{key}, &.{data});
    }

    const k, _ = cursor.get(.set_key, &.{2}, null) orelse return error.NotFound;
    try cursor.putReplace(k, "x");

    try txn.commit();
}
