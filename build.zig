const std = @import("std");

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const tests_use_system_lib = b.option(bool, "tests-use-system-lib", "Use globally installed 'lmdb' library for unit tests") orelse false;

    const no_run = b.option(bool, "no-run", "Don't run anything") orelse false;
    const no_install = b.option(bool, "no-install", "Don't install anything") orelse false;
    const test_filter = b.option([]const u8, "test-filter", "Filter tests") orelse "";

    const upstream_dep = b.dependency("upstream", .{});
    const lmdb_lib = makeLmdbLib(b, upstream_dep, target, optimize);
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
}

/// create static library from lmdb source (derived from the Makefile), available under artifact named `lmdb`
fn makeLmdbLib(b: *std.Build, upstream: *std.Build.Dependency, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode) *std.Build.Step.Compile {
    const lib = b.addLibrary(.{
        .name = "lmdb",
        .linkage = .static,
        .use_llvm = true,
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });

    lib.addCSourceFiles(.{
        .language = .c,
        .root = upstream.path("libraries/liblmdb/"),
        .files = &.{ "mdb.c", "midl.c" },
        .flags = &.{
            switch (optimize) {
                .Debug => "-DMDB_DEBUG=1",
                .ReleaseFast, .ReleaseSmall => "-DNDEBUG",
                .ReleaseSafe => "",
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
fn makeLmdbC(b: *std.Build, upstream: *std.Build.Dependency, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode) *std.Build.Module {
    const translate = b.addTranslateC(.{
        .root_source_file = upstream.path("libraries/liblmdb/lmdb.h"),
        .use_clang = true,
        .link_libc = true,

        .target = target,
        .optimize = optimize,
    });

    return translate.addModule("c");
}
