<p align="center">
  <a href="https://query.farm">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://query.farm/media-kit/logo/wordmark-dark.svg">
      <img alt="Query.Farm" src="https://query.farm/media-kit/logo/wordmark-light.svg" height="64">
    </picture>
  </a>
</p>

# DuckDB Redis Client Extension

[![DuckDB](https://img.shields.io/badge/DuckDB-community_extension-fdf1e0?logo=duckdb&logoColor=fff000)](https://duckdb.org/community_extensions/extensions/redis.html)
[![v1.5 build](https://github.com/Query-farm/redis/actions/workflows/MainDistributionPipeline.yml/badge.svg?branch=v1.5)](https://github.com/Query-farm/redis/actions/workflows/MainDistributionPipeline.yml?query=branch%3Av1.5)

This extension provides Redis client functionality for DuckDB, allowing you to interact with a Redis server directly from SQL queries.

> Experimental: USE AT YOUR OWN RISK!

## Documentation

Full documentation, including installation, usage, the function reference, and cookbook examples, is available at:

**[https://query.farm/products/extensions/redis](https://query.farm/products/extensions/redis)**

## Installation

```sql
INSTALL redis FROM community;
LOAD redis;
```

## Development

For instructions on building the extension from source and running its tests, see [BUILDING.md](BUILDING.md).
