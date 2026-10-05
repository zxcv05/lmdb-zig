const std = @import("std");
const lmdb = @import("lmdb");
const builtin = @import("builtin");

// tested with zig 0.17.0
pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;

    var args = try init.minimal.args.iterateAllocator(gpa);
    defer args.deinit();

    _ = args.skip();
    const dbpath = args.next() orelse return error.GiveMeDbPath;
    const dbname = args.next();

    return example(dbpath, dbname);
}

fn example(dbpath: [:0]const u8, dbname: ?[:0]const u8) !void {
    const env: lmdb.Env = try .init(dbpath, .{ .max_dbs = 8 });
    defer env.deinit();

    var txn = try env.begin(.read_write, .{});
    defer txn.abort();

    const dbi = try env.open(txn, dbname, .{ .create = true, .dup_sort = true });
    const cursor = try txn.cursor(dbi);

    var key_iter = cursor.iterator(.first, null, null, .next);
    var i: usize = 0;
    while (key_iter.next()) |kv1| : (i += 1) {
        const k, const v_orig = kv1;
        std.debug.print("Key #{d}:\n", .{i});
        std.debug.dumpHex(k);

        var val_iter = cursor.iterator(.first_dup, k, null, .next_dup);
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
