# lmdb zig wrapper library

## Goals
- Functional wrappers providing for every use case covered by lmdb

## build.zig
- zig wrappers available as module `lmdb`
- translated `liblmdb` headers available as module `c`
- compiled `liblmdb` static library available as artifact `lmdb`
- available options:
  - `-Dno-install`: Won't install the `liblmdb.a` library to `zig-out/lib` (default: false)

# License
This project is subject to the terms of the OpenLDAP Public License v2.8 (See `LICENSE`)
Copyright 2025 lmdb-zig contributors
