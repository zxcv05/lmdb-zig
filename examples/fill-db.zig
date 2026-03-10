const std = @import("std");
const lmdb = @import("lmdb");
const builtin = @import("builtin");

pub const main = switch (builtin.zig_version.minor) {
    15 => main_15,
    16 => main_16,
    else => @compileError("Unsupported zig version: " ++ builtin.zig_version_string),
};

// tested with zig 0.16.0-dev.2722+f16eb18ce
fn main_16(init: std.process.Init) !void {
    const io = init.io;
    const gpa = init.gpa;
    const clock = std.Io.Clock.real;

    const rng_seed: u64 = @truncate(@abs(clock.now(io).nanoseconds));
    var rng: std.Random.DefaultPrng = .init(rng_seed);
    const rand = rng.random();

    var args = try init.minimal.args.iterateAllocator(gpa);
    defer args.deinit();

    _ = args.skip(); // cmdline
    const kvs_str = args.next() orelse return error.HowManyKeyValues;
    const dbpath = args.next() orelse return error.GiveMeDbPath;
    const dbname = args.next();

    const kvs = try std.fmt.parseInt(usize, kvs_str, 0) + 1;

    return example(rand, kvs, dbpath, dbname);
}

// tested with zig 0.15.1
fn main_15() !void {
    const rng_seed: u64 = @truncate(@abs(std.time.nanoTimestamp()));
    var rng: std.Random.DefaultPrng = .init(rng_seed);
    const rand = rng.random();

    var args = std.process.args();

    _ = args.skip(); // cmdline
    const kvs_str = args.next() orelse return error.HowManyKeyValues;
    const dbpath = args.next() orelse return error.GiveMeDbPath;
    const dbname = args.next();

    const kvs = try std.fmt.parseInt(usize, kvs_str, 0) + 1;

    return example(rand, kvs, dbpath, dbname);
}

fn example(
    rand: std.Random,
    kvs: usize,
    dbpath: [:0]const u8,
    dbname: ?[:0]const u8,
) !void {
    const env: lmdb.Env = try .init(dbpath, .{
        .max_dbs = 8,
        .map_size = @sizeOf(usize) * kvs * 8,
    });
    defer env.deinit();

    var txn = try env.begin(@src(), .read_write, .{});
    defer txn.abort();

    const dbi = try env.open(txn, dbname, .{ .create = true, .dup_sort = true });

    for (0..kvs) |i| {
        const key = std.mem.nativeToBig(usize, i);
        const val = rand.int(usize);
        try dbi.put(txn, std.mem.asBytes(&key), std.mem.asBytes(&val));
    }

    try txn.commit();
}
