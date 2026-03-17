---
description: Project architecture, domain model, and key design decisions for hive-iceberg-ferry
globs:
  - src/hive_to_iceberg/**
  - scripts/**
  - config*.yaml
  - docker-compose.yml
alwaysApply: false
---

# Hive-to-Iceberg Ferry — Project Overview

## What this project does

CLI tool that migrates Hive tables to Apache Iceberg format using PySpark.
Reads from a Hive metastore (Thrift), writes to an Iceberg catalog (Hadoop, Nessie, AWS Glue, or S3 Tables).

## Owns

- Hive-to-Iceberg migration logic (`src/hive_to_iceberg/`)
- YAML-driven configuration for source, target, storage, Spark, and migration behavior
- Docker Compose local dev stack (Hive metastore, MinIO, Presto, PostgreSQL)
- Sample data loading scripts (`scripts/`)

## Does NOT own

- The Hive metastore itself (external dependency)
- Iceberg catalog backends (AWS Glue, S3 Tables, Nessie are external services)
- PySpark / Iceberg runtime (consumed as Maven packages)

## Critical constraints

- **CRITICAL:** Target is always Iceberg. Do not introduce alternative target formats. The platform standardizes on Iceberg as the open table format.
- **CRITICAL:** Source is currently Hive-only but the architecture should evolve to support pluggable sources (JDBC, Delta Lake, Parquet files, CSV, etc.). Keep source-specific logic isolated.
- **NEVER:** Hardcode AWS credentials in config files or source code. Use IAM roles or environment variables for AWS deployments.

## Key components

| Component | Path | Role |
|-----------|------|------|
| CLI | `src/hive_to_iceberg/cli.py` | Click entry point, argument parsing, summary output |
| Config | `src/hive_to_iceberg/config.py` | YAML parsing into typed dataclasses |
| Migration engine | `src/hive_to_iceberg/migrate.py` | SparkSession setup, table read/write, row validation |
| Sample data loader | `scripts/load_sample_data.py` | Downloads NYC taxi parquet, loads into Hive |
| YAML config validator | `scripts/validate_yaml.py` | Validates config files structure and values |

## Toolchain

- **Python 3.11–3.12** with `uv` as package manager
- **PySpark 3.5** with Iceberg 1.7.1 runtime
- **Hatchling** build backend
- Run commands via `uv run`
