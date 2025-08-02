const std = @import("std");
const lmdb = @import("lmdb");

pub fn main() !void {
    const dbpath, const dbname = get: {
        var args = std.process.args();
        _ = args.skip(); // cmdline

        break :get .{
            args.next() orelse return error.NoArgGiven,
            args.next(),
        };
    };

    const env: lmdb.Env = try .init(dbpath, .{ .max_dbs = 8 });
    defer env.deinit();

    var txn = try env.begin(.read_write, .{});
    defer txn.abort();

    const dbi = try env.open(txn, dbname, .{ .create = true, .dup_sort = true });

    const cursor = dbi.cursor(txn);
    defer cursor.deinit();

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
