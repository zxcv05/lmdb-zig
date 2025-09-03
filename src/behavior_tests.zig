const std = @import("std");
const lib = @import("root.zig");

const c = @import("c");

const log = std.log.scoped(.behavior_tests);

const SetupRes = struct { lib.Env, lib.Dbi, lib.Txn };
fn setup(src: std.builtin.SourceLocation, dbi_flags: lib.Dbi.InitFlags) !SetupRes {
    const env = lib.Env.init("testdb", .{ .max_dbs = 32 }) catch |e| {
        log.err("setup failed(create env): {t}", .{e});
        return error.Unrelated;
    };
    errdefer env.deinit();

    var txn = lib.Txn.init(env, src, null, .read_write, .{}) catch |e| {
        log.err("setup failed(create txn): {t}", .{e});
        return error.Unrelated;
    };
    errdefer txn.abort();

    const dbi_name = try std.fmt.allocPrint(std.testing.allocator, "{s}\x00", .{src.fn_name[5..]});
    defer std.testing.allocator.free(dbi_name);

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
    txn = try env.begin(@src(), .read_only, .{});

    for (&ctnr.keys, &ctnr.datas) |*key, *expected_data| {
        const actual_data = try txn.get(dbi, key) orelse return error.NotFound;
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
        const cursor = try txn.cursor(@src(), dbi);
        defer cursor.deinit();

        for (&ctnr.keys, &ctnr.datas) |*key, *data| {
            try cursor.put(key, data);
        }

        try txn.commit();
    }

    txn = try env.begin(@src(), .read_only, .{});

    const cursor = try txn.cursor(@src(), dbi);
    defer cursor.deinit();

    for (&ctnr.keys, &ctnr.datas) |*key, *expected_data| {
        _, const actual_data = cursor.get(.set_key, key, null) orelse return error.NotFound;
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
    txn = try env.begin(@src(), .read_only, .{});

    const cursor = try txn.cursor(@src(), dbi);
    defer cursor.deinit();

    for (&ctnr.keys, &ctnr.datass) |*key, *datas| {
        _ = cursor.get(.set, key, null) orelse return error.NotFound;
        var dup_iter = cursor.get_iter(.first_dup, key, null, .next_dup);

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

test "put_no_clobber" {
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
        try txn.put_no_clobber(dbi, key, data);
        try std.testing.expectError(error.AlreadyExists, txn.put_no_clobber(dbi, key, data));
    }
}

test "put_get" {
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
        try std.testing.expectEqualSlices(u8, data, try txn.put_get(dbi, key, data));
        try std.testing.expectEqualSlices(u8, data, try txn.put_get(dbi, key, "garbage"));
    }
}

test "sort put commit get, put_append" {
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
        try txn.put_append(dbi, key, data);
    }

    try txn.commit();
    txn = try env.begin(@src(), .read_only, .{});

    const cursor = try txn.cursor(@src(), dbi);
    defer cursor.deinit();

    var iter = cursor.get_iter(.first, null, null, .next);
    for (&ctnr.keys, &ctnr.datas) |*key, *data| {
        const k, const v = iter.next() orelse return error.NotFound;
        try std.testing.expectEqualSlices(u8, key, k);
        try std.testing.expectEqualSlices(u8, data, v);
    }

    try std.testing.expect(iter.next() == null);
}

test "sort put commit get, put_append_dup" {
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
            try txn.put_append_dup(dbi, key, data);
        }
    }

    // commit

    try txn.commit();
    txn = try env.begin(@src(), .read_only, .{});

    // get

    const cursor = try txn.cursor(@src(), dbi);
    defer cursor.deinit();

    var key_iter = cursor.get_iter(.first, null, null, .next);
    for (&ctnr.keys, &ctnr.datass) |*key, *datas| {
        const k, _ = key_iter.next() orelse return error.NotFound;
        try std.testing.expectEqualSlices(u8, key, k);

        var dup_iter = cursor.get_iter(.first_dup, k, null, .next_dup);
        for (datas) |*data| {
            _, const dv = dup_iter.next() orelse return error.NotFound;
            try std.testing.expectEqualSlices(u8, data, dv);
        }

        try std.testing.expect(dup_iter.next() == null);
    }
    try std.testing.expect(key_iter.next() == null);
}

test "put commit get, put_reserve" {
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
        const buf = try txn.put_reserve(dbi, key, data.len);
        @memcpy(buf, data);
    }

    try txn.commit();
    txn = try env.begin(@src(), .read_only, .{});

    for (&ctnr.keys, &ctnr.datas) |*key, *expected_data| {
        const actual_data = try txn.get(dbi, key) orelse return error.NotFound;
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
    txn = try env.begin(@src(), .read_only, .{});

    for (&ctnr.keys, &ctnr.datas, 0..) |*key, *data, i| {
        const res = try txn.get(dbi, key);

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

    var cursor = try txn.cursor(@src(), dbi);

    for (&ctnr.keys, &ctnr.datas) |*key, *data| {
        try cursor.put_append(key, data);
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
    txn = try env.begin(@src(), .read_only, .{});

    // get

    cursor = try txn.cursor(@src(), dbi);
    defer cursor.deinit();

    for (&ctnr.keys, &ctnr.datas, 0..) |*key, *data, i| {
        const res = cursor.get(.set_key, key, null);

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

    try std.testing.expect(dbi.empty_contents(txn));

    try txn.commit();
    txn = try env.begin(@src(), .read_only, .{});

    for (&ctnr.keys) |*key| {
        try std.testing.expect(try txn.get(dbi, key) == null);
    }
}

test "put_multiple commit get_multiple" {
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

    var cursor = try txn.cursor(@src(), dbi);

    for (&ctnr.keys, &ctnr.datas) |*key, data| {
        var head: usize = 0;
        while (head < ctnr.datas[0].len) {
            head += try cursor.put_multiple(u32, key, data[head..]);
        }
    }

    // commmit

    try txn.commit();
    txn = try env.begin(@src(), .read_only, .{});

    // get

    cursor = try txn.cursor(@src(), dbi);
    defer cursor.deinit();

    for (&ctnr.keys, &ctnr.datas, &ctnr.dests) |*key, data, dest| {
        var head: usize = 0;

        _ = cursor.get(.set, key, null) orelse return error.NotFound;
        while (cursor.get_multiple(u32, .next, key)) |page| {
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

    var cursor = try txn.cursor(@src(), dbi);

    var keys: [KEYS_AMT]Key = undefined;
    rng.fill(std.mem.asBytes(&keys));

    for (&keys, 1..) |*key, i| {
        for (0..i) |j| {
            const data: u128 = j; // we dont care what data is, we're counting dups
            try cursor.put_append_dup(key, std.mem.asBytes(&data));
        }
    }

    try txn.commit();
    txn = try env.begin(@src(), .read_only, .{});

    cursor = try txn.cursor(@src(), dbi);
    defer cursor.deinit();

    for (&keys, 1..) |*key, i| {
        _ = cursor.get(.set, key, null) orelse return error.NotFound;
        try std.testing.expectEqual(cursor.count().?, i);
    }
}

test "cursor del_all" {
    const env, const dbi, var txn = try setup(@src(), .{ .dup_sort = true });
    defer env.deinit();
    defer txn.abort();

    var cursor = try txn.cursor(@src(), dbi);

    for (0..2) |i| {
        const key: u8 = @intCast(i);

        for (0..DUPS_AMT) |j| {
            const data: u128 = j;
            try cursor.put(std.mem.asBytes(&key), std.mem.asBytes(&data));
        }
    }

    _ = cursor.get(.set, &.{0}, null);
    try cursor.del_all();

    try txn.commit();
    txn = try env.begin(@src(), .read_only, .{});

    cursor = try txn.cursor(@src(), dbi);
    defer cursor.deinit();

    try std.testing.expect(cursor.get(.set, &.{0}, null) == null);
    try std.testing.expectEqual(null, cursor.count());

    try std.testing.expect(cursor.get(.set, &.{1}, null) != null);
    try std.testing.expectEqual(DUPS_AMT, cursor.count() orelse return error.NotFound);
}

test "put commit get, put_replace" {
    const env, const dbi, var txn = try setup(@src(), .{ .dup_sort = true });
    defer env.deinit();
    defer txn.abort();

    var cursor = try txn.cursor(@src(), dbi);

    for (0..4) |i| {
        const key: u8 = @intCast(i);
        const data: u8 = 1;
        try cursor.put_append(&.{key}, &.{data});
    }

    const k, _ = cursor.get(.set_key, &.{2}, null) orelse return error.NotFound;
    try cursor.put_replace(k, "x");

    try txn.commit();
}
