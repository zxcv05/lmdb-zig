# lmdb zig wrapper library

## Goals
- Functional wrappers providing for every use case covered by lmdb

## Examples
see `/examples/`

## "How do I use this?"
- First, fetch the package: `zig fetch --save=lmdb git+https://github.com/zxcv05/lmdb-zig`
- Then, modify your build.zig:
```zig
//! build.zig

const lmdb_dep = b.dependency("lmdb", .{ .target = target, .optimize = optimize });
const lmdb_mod = lmdb_dep.module("lmdb"); // for wrappers
const lmdb_lib = lmdb_dep.artifact("lmdb"); // for linking (ignore this if you want to use system-installed library instead)

// ...

my_module.addImport("lmdb", lmdb_mod);
my_exe.linkLibrary(lmdb_lib);
// OR, to use system-installed library instead:
my_exe.linkSystemLibrary("lmdb");
```
- Finally, you can use lmdb in your project
```zig
//! my-file.zig

const lmdb = @import("lmdb");

// See `/examples/` for help with usage
pub fn my_func() !void {
    const env: lmdb.Env = try .init("my-lmdb-env/", .{});
    defer env.deinit();
    // ...
}
```

## build.zig
- zig wrappers available as module `lmdb`
- translated `liblmdb` headers available as module `c`
- compiled `liblmdb` static library available as artifact `lmdb`
- available options:
  - `-Dno-install`: For default step, build but don't install `liblmdb` (default: false)
  - `-Duse-tracing`: Build `liblmdb` static library with debug tracing enabled (default: false)
  - `-Dno-run`: For `test` step, build but don't run unit tests (default: false)
  - `-Dtest-filter='x'`: For `test` step, filter which tests are run based on `x`
  - `-Dtests-use-system-lib`: For `test` step, use system installed `liblmdb` library instead of the one we build (default: false)

# License
This project is subject to the terms of the OpenLDAP Public License v2.8 (See `LICENSE`)
Copyright 2025 lmdb-zig contributors
