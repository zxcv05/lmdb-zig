# lmdb zig wrapper library

## Goals
- Functional wrappers providing for every use case covered by lmdb

## Examples
see `/examples/`

## build.zig
- zig wrappers available as module `lmdb`
- translated `liblmdb` headers available as module `c`
- compiled `liblmdb` static library available as artifact `lmdb`
- available options:
  - `-Dno-install`: For default step, build but don't install `liblmdb` (default: false)
  - `-Duse-tracing`: Build `liblmdb` static library with debug tracing enabled
  - `-Dno-run`: For `test` step, build but don't run unit tests (default: false)
  - `-Dtest-filter='x'`: For `test` step, filter which tests are run based on `x`
  - `-Dtests-use-system-lib`: For `test` step, use system installed `liblmdb` library instead of the one we build (default: false)

# License
This project is subject to the terms of the OpenLDAP Public License v2.8 (See `LICENSE`)
Copyright 2025 lmdb-zig contributors
