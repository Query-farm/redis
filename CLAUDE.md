# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a DuckDB extension that provides Redis client functionality, allowing interaction with Redis servers directly from SQL queries. It uses Boost.Asio for network communication and implements the Redis protocol directly.

## Build Commands

```sh
# Build release (default)
GEN=ninja make

# Build debug
GEN=ninja make debug

# Run tests
make test

# Format code
make format

# Run clang-tidy
make tidy-check
```

It is best to test with a debug build since it tests more assertions.

All extension functions should be documented inside of DuckDB with CreateScalarFunctionInfo or CreateAggregateFunctionInfo or the appropriate type for the function.  This documentation of the function should include examples, parameter types and parameter names.  The function should be categorized.

When making changes the version should always be updated to the current date plus an ordinal counter in the form of YYYYMMDDCC.

## Project Structure

- `src/redis_extension.cpp` - Main extension code containing:
  - `RedisProtocol` class: Redis RESP protocol formatting/parsing
  - `RedisConnection` class: TCP socket connection management via Boost.Asio
  - `ConnectionPool` class: Singleton connection pooling
  - All scalar and table function implementations (redis_get, redis_set, redis_hget, etc.)
- `src/redis_secret.cpp` - DuckDB secret type registration for storing Redis connection credentials
- `src/include/` - Header files
- `test/sql/` - SQLLogicTest files
- `duckdb/` - DuckDB submodule (source)
- `extension-ci-tools/` - Build infrastructure submodule

## Dependencies

- **Boost.Asio** - Network I/O (installed via vcpkg, see `vcpkg.json`)
- Build uses vcpkg toolchain: set `VCPKG_TOOLCHAIN_PATH` environment variable

## Architecture Notes

- Extension registers a `redis` secret type that stores host, port, and password
- All Redis functions take a secret name as the last parameter to identify which connection to use
- Connection pooling is per host:port combination
- Redis protocol is implemented directly (no external Redis client library)
- Scalar functions: `redis_get`, `redis_set`, `redis_hget`, `redis_hset`, `redis_lpush`, `redis_lrange`, `redis_mget`, `redis_scan`, `redis_hscan`, `redis_del`, `redis_exists`, `redis_type`
- Table functions: `redis_keys`, `redis_hgetall`, `redis_lrange_table`, `redis_hscan_over_scan`

## Testing

Tests use DuckDB's SQLLogicTest format. Test files are in `test/sql/`. The current test file needs updating as it references a `quack` extension (template leftover).
