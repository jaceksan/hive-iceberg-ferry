---
description: Project architecture, domain model, and key design decisions for iceberg-ferry
globs:
  - src/hive_to_iceberg/**
  - scripts/**
  - config*.yaml
  - docker-compose.yml
alwaysApply: false
---

# Iceberg Ferry — Project Overview

## What this project does

CLI tool that migrates data from pluggable sources to Apache Iceberg format using PySpark.
Built-in sources: Hive (via Thrift metastore) and Parquet files.
Target catalogs: Hadoop, Nessie, AWS Glue, AWS S3 Tables.

## Owns

- Migration logic and Iceberg write path (`src/hive_to_iceberg/`)
- Pluggable source framework (`src/hive_to_iceberg/sources/`)
- YAML-driven configuration for source, target, storage, Spark, and migration behavior
- Docker Compose local dev stack (Hive metastore, MinIO, Presto, PostgreSQL)
- Sample data loading scripts (`scripts/`)

## Does NOT own

- The Hive metastore itself (external dependency)
- Iceberg catalog backends (AWS Glue, S3 Tables, Nessie are external services)
- PySpark / Iceberg runtime (consumed as Maven packages)

## Critical constraints

- **CRITICAL:** Target is always Iceberg. Do not introduce alternative target formats. The platform standardizes on Iceberg as the open table format.
- **CRITICAL:** Sources are pluggable via the `Source` ABC in `sources/base.py`. All source-specific logic must live in `sources/` — never in `migrate.py`.
- **NEVER:** Hardcode AWS credentials in config files or source code. Use IAM roles or environment variables for AWS deployments.

## Key components

| Component | Path | Role |
|-----------|------|------|
| CLI | `src/hive_to_iceberg/cli.py` | Click entry point, argument parsing, summary output |
| Config | `src/hive_to_iceberg/config.py` | YAML parsing into typed dataclasses |
| Migration engine | `src/hive_to_iceberg/migrate.py` | SparkSession setup, Iceberg write, row validation |
| Source ABC | `src/hive_to_iceberg/sources/base.py` | Interface for pluggable sources |
| Source registry | `src/hive_to_iceberg/sources/registry.py` | Maps `source.type` to implementation |
| Hive source | `src/hive_to_iceberg/sources/hive.py` | Reads from Hive metastore |
| Parquet source | `src/hive_to_iceberg/sources/parquet.py` | Reads Parquet files from local/S3 |
| Sample data loader | `scripts/load_sample_data.py` | Downloads NYC taxi parquet, loads into Hive |
| YAML config validator | `scripts/validate_yaml.py` | Validates config files structure and values |

## Toolchain

- **Python 3.11–3.12** with `uv` as package manager
- **PySpark 3.5** with Iceberg 1.7.1 runtime
- **Hatchling** build backend
- Run commands via `uv run`
