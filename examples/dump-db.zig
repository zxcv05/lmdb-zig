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
    const gpa = init.gpa;

    var args = try init.minimal.args.iterateAllocator(gpa);
    defer args.deinit();

    _ = args.skip();
    const dbpath = args.next() orelse return error.GiveMeDbPath;
    const dbname = args.next();

    return example(dbpath, dbname);
}

// tested with zig 0.15.1
fn main_15() !void {
    var args = std.process.args();
    _ = args.skip();

    const dbpath = args.next() orelse return error.GiveMeDbPath;
    const dbname = args.next();

    return example(dbpath, dbname);
}

fn example(dbpath: [:0]const u8, dbname: ?[:0]const u8) !void {
    const env: lmdb.Env = try .init(dbpath, .{ .max_dbs = 8 });
    defer env.deinit();

    var txn = try env.begin(@src(), .read_write, .{});
    defer txn.abort();

    const dbi = try env.open(txn, dbname, .{ .create = true, .dup_sort = true });

    const cursor = try dbi.cursor(@src(), &txn);

    var key_iter = cursor.get_iter(.first, null, null, .next);
    var i: usize = 0;
    while (key_iter.next()) |kv1| : (i += 1) {
        const k, const v_orig = kv1;
        std.debug.print("Key #{d}:\n", .{i});
        std.debug.dumpHex(k);

        var val_iter = cursor.get_iter(.first_dup, k, null, .next_dup);
        var j: usize = 0;
        while (val_iter.next()) |kv2| : (j += 1) {
            _, const v = kv2;
            std.debug.print("Data #{d}:\n", .{j});
            std.debug.dumpHex(v);
        }

        if (j == 0) {
            std.debug.print("Data:\n", .{});
            std.debug.dumpHex(v_orig);
        }

        std.debug.print("\n", .{});
    }
}
