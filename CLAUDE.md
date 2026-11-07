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
  - `RedisReplyParser` class: incremental RESP2/RESP3 parser (linear-time completeness scan, bounded counts/lengths)
  - `RedisConnection` class: asynchronous Boost.Asio I/O with timeouts and interrupt checks, pipelining
  - `RedisConnectionPool` / `RedisSession`: per-database idle-connection pool (object cache); connections are checked
    out for exclusive use and retried once when a pooled connection went stale
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

- Extension registers a `redis` secret type: host, port (required), password, username (ACL), db, timeout_ms
- All Redis functions take a secret name as the last parameter to identify which connection to use
- Connections are pooled per database instance, keyed by host, port, credentials and db index
- Every function refuses when `enable_external_access` is false; `redis_timeout_ms` bounds connects and reply waits
- Scalar functions pipeline the commands of a chunk (one round trip per distinct secret)
- Redis protocol is implemented directly (no external Redis client library)
- Scalar functions: `redis_get`, `redis_set`, `redis_hget`, `redis_hset`, `redis_lpush`, `redis_lrange`, `redis_lrange_list`, `redis_mget`, `redis_mget_list`, `redis_scan`, `redis_scan_page`, `redis_hscan`, `redis_hscan_page`, `redis_hgetall_map`, `redis_command`, `redis_del`, `redis_exists`, `redis_type`, `redis_expire`, `redis_expireat`, `redis_ttl`
- Table functions: `redis_keys`, `redis_hgetall`, `redis_lrange_table`, `redis_hscan_over_scan`

## Testing

Tests use DuckDB's SQLLogicTest format. Test files are in `test/sql/`. The default suite is offline. The live tests
(`redis_server.test`, `redis_server_features.test`) run when `REDIS_TEST_PORT` and `REDIS_TEST_PASSWORD` are set:

```sh
redis-server --port 16379 --requirepass testpw --save "" --daemonize yes
REDIS_TEST_PORT=16379 REDIS_TEST_PASSWORD=testpw ./build/release/test/unittest "test/sql/*"
```
