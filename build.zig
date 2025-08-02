const std = @import("std");

const EXAMPLES = [_][]const u8{
    "dump-db",
};

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const no_run = b.option(bool, "no-run", "Don't run anything") orelse false;
    const no_install = b.option(bool, "no-install", "Don't install anything") orelse false;

    const test_filter = b.option([]const u8, "test-filter", "Filter tests") orelse "";
    const tests_use_system_lib = b.option(bool, "tests-use-system-lib", "Use globally installed 'lmdb' library for unit tests") orelse false;

    const use_tracing = b.option(bool, "use-tracing", "Enable debug tracing logs") orelse false;

    const upstream_dep = b.dependency("upstream", .{});
    const lmdb_lib = makeLmdbLib(b, upstream_dep, target, optimize, use_tracing);
    const lmdb_c = makeLmdbC(b, upstream_dep, target, optimize);

    // zig wrappers live here
    const root_mod = b.addModule("lmdb", .{
        .root_source_file = b.path("src/root.zig"),
        .imports = &.{.{ .name = "c", .module = lmdb_c }},

        .target = target,
        .optimize = optimize,
    });

    if (no_install) {
        b.default_step.dependOn(&lmdb_lib.step);
    } else {
        const lmdb_lib_install = b.addInstallArtifact(lmdb_lib, .{});
        b.default_step.dependOn(&lmdb_lib_install.step);
    }

    // unit tests

    var filter_iter = std.mem.splitScalar(u8, test_filter, ';');
    var filters = try std.ArrayList([]const u8).initCapacity(b.allocator, 16);

    while (filter_iter.next()) |filter| {
        (try filters.addOne()).* = filter;
    }

    const unit_tests = b.addTest(.{
        .root_module = root_mod,
        .filters = filters.items,
        .use_llvm = false,
    });

    if (tests_use_system_lib) {
        unit_tests.linkSystemLibrary("lmdb");
    } else {
        unit_tests.linkLibrary(lmdb_lib);
    }

    const test_step = b.step("test", "Run unit tests");
    if (no_run) {
        test_step.dependOn(&unit_tests.step);
    } else {
        const unit_tests_run = b.addRunArtifact(unit_tests);
        test_step.dependOn(&unit_tests_run.step);
    }

    // examples

    inline for (EXAMPLES) |name| {
        const module = b.createModule(.{
            .root_source_file = b.path("examples/" ++ name ++ ".zig"),
            .imports = &.{.{ .name = "lmdb", .module = root_mod }},

            .optimize = optimize,
            .target = target,
        });

        const exe = b.addExecutable(.{
            .name = name,
            .root_module = module,
        });

        const exe_run = b.addRunArtifact(exe);
        if (b.args) |args| exe_run.addArgs(args);

        const exe_run_step = b.step("example-" ++ name, "Run example '" ++ name ++ "'");
        exe_run_step.dependOn(&exe_run.step);
    }
}

/// create static library from lmdb source (derived from the Makefile), available under artifact named `lmdb`
fn makeLmdbLib(
    b: *std.Build,
    upstream: *std.Build.Dependency,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    use_tracing: bool,
) *std.Build.Step.Compile {
    const lib = b.addLibrary(.{
        .name = "lmdb",
        .linkage = .static,

        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,

            .link_libc = true,
            .sanitize_c = .off,
        }),
    });

    lib.addCSourceFiles(.{
        .language = .c,
        .root = upstream.path("libraries/liblmdb/"),
        .files = &.{ "mdb.c", "midl.c" },
        .flags = &.{
            if (use_tracing)
                "-DMDB_DEBUG=1"
            else switch (optimize) {
                .ReleaseFast, .ReleaseSmall => "-DNDEBUG",
                else => "",
            },
            "-W",
            "-Wall",
            "-Wno-unused-parameter",
            "-Wbad-function-cast",
            "-Wuninitialized",
        },
    });

    return lib;
}

/// create translate step for lmdb headers, available under module named `c`
fn makeLmdbC(
    b: *std.Build,
    upstream: *std.Build.Dependency,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) *std.Build.Module {
    const translate = b.addTranslateC(.{
        .root_source_file = upstream.path("libraries/liblmdb/lmdb.h"),
        .use_clang = true,
        .link_libc = true,

        .target = target,
        .optimize = optimize,
    });

    return translate.addModule("c");
}
