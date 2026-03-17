---
description: Docker Compose local dev stack — services, ports, and configuration
globs:
  - docker-compose.yml
  - docker/**
alwaysApply: false
---

# Docker Compose Local Dev Stack

## Services

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| PostgreSQL 16 | `postgres:16` | 5432 | Hive metastore backend DB |
| Hive 3.1.3 | `apache/hive:4.0.1` | 9083 | Thrift metastore server |
| MinIO | `minio/minio` | 9000, 9001 | S3-compatible object storage |
| MinIO init | `minio/mc` | — | Creates `hive-warehouse` and `iceberg-warehouse` buckets |
| Presto | `prestodb/presto` | 8080 | Query engine for ad-hoc validation |

## Configuration files

- `docker/hive/hive-site.xml` — Hive warehouse location (`s3a://hive-warehouse/`)
- `docker/hive/core-site.xml` — S3A endpoint and MinIO credentials (dev only)
- `docker/presto/hive.properties` — Presto Hive connector pointing at metastore + MinIO

## Critical constraints

- **CRITICAL:** MinIO credentials in Docker configs are for local dev only. Never reuse in production configs.
- **NEVER:** Expose the MinIO console port (9001) in production deployments.

## Testing changes

After modifying Docker configs, validate with `docker compose config --quiet` before starting services.
