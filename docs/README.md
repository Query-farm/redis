<img src="https://github.com/user-attachments/assets/46a5c546-7e9b-42c7-87f4-bc8defe674e0" width=250 />

# DuckDB Redis Client Extension
This extension provides Redis client functionality for DuckDB, allowing you to interact with a Redis server directly from SQL queries.

## Usage
```sql
INSTALL redis FROM community;
LOAD redis;
```

> Experimental: USE AT YOUR OWN RISK!

## Features
Currently supported Redis operations:
- String operations: `GET`, `SET`, `MGET`
- Hash operations: `HGET`, `HSET`, `HGETALL`, `HSCAN`, `HSCAN_OVER_SCAN`
- List operations: `LPUSH`, `LRANGE`, `LRANGE_TABLE`
- Key operations: `DEL`, `EXISTS`, `TYPE`, `SCAN`, `KEYS`
- TTL operations: `EXPIRE`, `TTL`, `EXPIREAT`
- Batch and discovery operations: `SCAN`, `HSCAN_OVER_SCAN`, `KEYS`

## Quick Reference: Available Functions

| Function | Type | Description |
|----------|------|-------------|
| `redis_get(key, secret)` | Scalar | Get value of a string key |
| `redis_set(key, value, secret)` | Scalar | Set value of a string key |
| `redis_mget(keys_csv, secret)` | Scalar | Get values for multiple keys (comma-separated) |
| `redis_hget(key, field, secret)` | Scalar | Get value of a hash field |
| `redis_hset(key, field, value, secret)` | Scalar | Set value of a hash field |
| `redis_lpush(key, value, secret)` | Scalar | Push value to a list |
| `redis_lrange(key, start, stop, secret)` | Scalar | Get range from a list (comma-separated) |
| `redis_del(key, secret)` | Scalar | Delete a key (returns TRUE if deleted) |
| `redis_exists(key, secret)` | Scalar | Check if a key exists (returns TRUE if exists) |
| `redis_type(key, secret)` | Scalar | Get the type of a key |
| `redis_expire(key, seconds, secret)` | Scalar | Set TTL in seconds (returns TRUE if set) |
| `redis_ttl(key, secret)` | Scalar | Get remaining TTL (-2=key not exists, -1=no expiry) |
| `redis_expireat(key, timestamp, secret)` | Scalar | Set expiry at Unix timestamp (returns TRUE if set) |
| `redis_scan(cursor, pattern, count, secret)` | Scalar | Scan keys (returns cursor:keys_csv) |
| `redis_hscan(key, cursor, pattern, count, secret)` | Scalar | Scan fields in a hash |
| `redis_keys(pattern, secret)` | Table | List all keys matching a pattern |
| `redis_hgetall(key, secret)` | Table | List all fields and values in a hash |
| `redis_lrange_table(key, start, stop, secret)` | Table | List elements in a list as rows |
| `redis_hscan_over_scan(scan_pattern, hscan_pattern, count, secret)` | Table | For all keys matching scan_pattern, HSCAN with hscan_pattern, return (key, field, value) rows |

## Installation
```sql
INSTALL redis FROM community;
LOAD redis;
```

## Usage
### Setting up Redis Connection
First, create a secret to store your Redis connection details:

```sql
-- Create a Redis connection secret
CREATE SECRET IF NOT EXISTS redis (
        TYPE redis,
        PROVIDER config,
        host 'localhost',
        port '6379',
        password 'optional_password'
    );

-- Create a Redis cloud connection secret
CREATE SECRET IF NOT EXISTS redis (
        TYPE redis,
        PROVIDER config,
        host 'redis-1234.ec2.redns.redis-cloud.com',
        port '16959',
        password 'xxxxxx'
    );
```

### String Operations
```sql
-- Set a value
SELECT redis_set('user:1', 'John Doe', 'redis') as result;

-- Get a value
SELECT redis_get('user:1', 'redis') as user_name;

-- Set multiple values in a query
INSERT INTO users (id, name)
SELECT id, redis_set(
    'user:' || id::VARCHAR,
    name,
    'my_redis'
)
FROM new_users;
```

### Hash Operations
```sql
-- Set hash fields
SELECT redis_hset('user:1', 'email', 'john@example.com', 'redis');
SELECT redis_hset('user:1', 'age', '30', 'redis');

-- Get hash field
SELECT redis_hget('user:1', 'email', 'redis') as email;

-- Get all fields and values in a hash (table)
SELECT * FROM redis_hgetall('user:1', 'redis');

-- HSCAN a hash (pattern match fields)
SELECT redis_hscan('user:1', '0', 'email*', 100, 'redis');

-- HSCAN over SCAN: for all keys matching a pattern, get all hash fields matching a pattern
SELECT * FROM redis_hscan_over_scan('user:*', 'email*', 100, 'redis');

-- Store user profile as hash
WITH profile(id, field, value) AS (
    VALUES 
        (1, 'name', 'John Doe'),
        (1, 'email', 'john@example.com'),
        (1, 'age', '30')
)
SELECT redis_hset(
    'user:' || id::VARCHAR,
    field,
    value,
    'redis'
)
FROM profile;
```

### List Operations
```sql
-- Push items to list
SELECT redis_lpush('mylist', 'first_item', 'redis');
SELECT redis_lpush('mylist', 'second_item', 'redis');

-- Get range from list (returns comma-separated values)
SELECT redis_lrange('mylist', 0, -1, 'redis') as items;

-- Get all items in a list as rows (table)
SELECT * FROM redis_lrange_table('mylist', 0, -1, 'redis');

-- Push multiple items
WITH items(value) AS (
    VALUES ('item1'), ('item2'), ('item3')
)
SELECT redis_lpush('mylist', value, 'redis')
FROM items;
```

### Key Operations
```sql
-- List all keys matching a pattern (table)
SELECT * FROM redis_keys('user:*', 'redis');

-- Delete a key
SELECT redis_del('user:1', 'redis');

-- Check if a key exists
SELECT redis_exists('user:1', 'redis');

-- Get the type of a key
SELECT redis_type('user:1', 'redis');
```

### TTL Operations
```sql
-- Set a key to expire in 1 hour
SELECT redis_expire('session:123', 3600, 'redis');

-- Check remaining TTL
SELECT redis_ttl('session:123', 'redis') as remaining_seconds;

-- Set expiry at specific timestamp
SELECT redis_expireat('cache:item', 1736918400, 'redis');

-- Expire all session keys in a query
UPDATE sessions
SET expired = CASE WHEN redis_expire('session:' || id, 300, 'redis') THEN TRUE ELSE FALSE END;
```

### Batch and Discovery Operations
```sql
-- Get multiple keys at once
SELECT redis_mget('key1,key2,key3', 'redis') as values;

-- Scan keys matching a pattern
SELECT redis_scan('0', 'user:*', 10, 'redis') as result;

-- Scan all keys matching a pattern (recursive)
WITH RECURSIVE scan(cursor, keys) AS (
    SELECT split_part(redis_scan('0', 'user:*', 10, 'redis'), ':', 1),
           split_part(redis_scan('0', 'user:*', 10, 'redis'), ':', 2)
    UNION ALL
    SELECT split_part(redis_scan(cursor, 'user:*', 10, 'redis'), ':', 1),
           split_part(redis_scan(cursor, 'user:*', 10, 'redis'), ':', 2)
    FROM scan
    WHERE cursor != '0'
)
SELECT keys FROM scan;
```

## Error Handling
The extension functions will throw exceptions with descriptive error messages when:
- Redis secret is not found or invalid
- Unable to connect to Redis server
- Network communication errors occur
- Invalid Redis protocol responses are received

## Building from Source
Follow the standard DuckDB extension build process:

```sh
# Install vcpkg dependencies
./vcpkg/vcpkg install boost-asio

# Build the extension
make
```

## Future Enhancements
Planned features include:
- Table functions for scanning Redis keys
- Additional Redis commands (SADD, SMEMBERS, etc.)
- Batch operations using Redis pipelines
- Connection timeout handling

