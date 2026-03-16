# Hive-to-Iceberg Ferry

Migrate your Hive tables to [Apache Iceberg](https://iceberg.apache.org/) with a single command. Point it at a Hive metastore, pick a target catalog, and let it handle the rest.

**Supports:** AWS Glue Catalog · AWS S3 Tables · Hadoop · Nessie — with S3 and MinIO storage backends.

## Why?

Apache Iceberg is rapidly becoming the standard open table format for analytics. But if you have years of data sitting in Hive tables, the migration path isn't always obvious. Hive-to-Iceberg Ferry bridges that gap:

- **Zero-copy schema migration** — reads from Hive, writes to Iceberg using Spark's DataFrameWriterV2 API
- **Multiple catalog backends** — AWS Glue, S3 Tables, Hadoop, and Nessie out of the box
- **Row-count validation** — every table is verified after migration
- **Batteries-included local dev** — Docker Compose stack with Hive metastore, MinIO, Presto, and PostgreSQL
- **Simple YAML config** — one file controls source, target, storage, Spark tuning, and migration behavior

## Quick Start

### Prerequisites

- Python 3.11–3.12
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- Docker & Docker Compose (for local development)
- Java 11+ (required by PySpark)

### 1. Install

```bash
uv sync
```

### 2. Start local infrastructure

```bash
docker compose up -d
```

This spins up PostgreSQL (Hive metastore backend), Apache Hive 4.0.1, MinIO (S3-compatible storage), and Presto.

### 3. Load sample data

```bash
uv run python scripts/load_sample_data.py
```

Downloads NYC taxi trip parquet files and loads them into Hive. Use `--rows N` to limit row count (default: 10,000; `0` for all).

### 4. Run the migration

```bash
uv run hive-to-iceberg -c config.yaml
```

Override specific tables with `-t`:

```bash
uv run hive-to-iceberg -c config.yaml -t nyctaxi.yellow_tripdata
```

Add `-v` for debug logging.

## Configuration

All settings live in a single YAML file. See [`config.yaml`](config.yaml) for a local MinIO/Hadoop example and [`config.aws.yaml`](config.aws.yaml) for AWS Glue and S3 Tables templates.

```yaml
source:
  metastore_uri: thrift://localhost:9083
  database: nyctaxi

target:
  catalog_type: hadoop  # hadoop | nessie | glue | s3_tables
  catalog_name: iceberg
  database: nyctaxi

storage:
  endpoint: http://localhost:9000
  access_key: minioadmin
  secret_key: minioadmin

migration:
  write_mode: create  # create | replace | append
  repartition: null
  partition_by: []
```

### Catalog types

| Type | Backend | Use case |
|------|---------|----------|
| `hadoop` | S3A warehouse path | Local dev with MinIO, simple S3 deployments |
| `nessie` | Nessie REST catalog | Git-like branching for data lakes |
| `glue` | AWS Glue Data Catalog | AWS-native, integrates with Athena/Redshift Spectrum |
| `s3_tables` | AWS S3 Tables | Native S3 table format with automatic compaction |

## Project Structure

```
├── src/hive_to_iceberg/
│   ├── cli.py            # Click CLI entry point
│   ├── config.py         # YAML config parsing with dataclasses
│   └── migrate.py        # Spark session setup & migration logic
├── scripts/
│   └── load_sample_data.py
├── docker/
│   ├── hive/             # Hive metastore config (hive-site.xml, core-site.xml)
│   └── presto/           # Presto catalog config
├── config.yaml           # Local dev config (MinIO + Hadoop catalog)
├── config.aws.yaml       # AWS config template (Glue / S3 Tables)
└── docker-compose.yml    # Full local stack
```

## Docker Compose Stack

| Service | Port | Purpose |
|---------|------|---------|
| PostgreSQL 16 | 5432 | Hive metastore backend |
| Hive 4.0.1 | 9083 | Thrift metastore server |
| MinIO | 9000 / 9001 | S3-compatible object storage + console |
| Presto | 8080 | Query engine for validation |

## How It Works

1. Reads the YAML config and connects to the Hive metastore via Thrift
2. Builds a SparkSession with Iceberg extensions and the target catalog configured
3. For each table: reads the Hive DataFrame, optionally repartitions, and writes to Iceberg using `DataFrameWriterV2`
4. Validates row counts between source and target
5. Prints a migration summary with success/failure counts

## License

MIT
