# Iceberg Ferry

Migrate your data to [Apache Iceberg](https://iceberg.apache.org/) from any supported source with a single command. Pluggable source architecture makes it easy to add new data origins — Hive today, JDBC or Parquet files tomorrow.

**Sources:** Hive · Parquet (more coming)
**Targets:** Iceberg via AWS Glue Catalog · AWS S3 Tables · Hadoop · Nessie

## Why?

Apache Iceberg is rapidly becoming the standard open table format for analytics. But migrating existing data isn't always straightforward. Iceberg Ferry bridges that gap:

- **Pluggable sources** — built-in Hive and Parquet support; add new sources by implementing a simple Python ABC
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
  type: hive              # hive | parquet
  database: nyctaxi
  hive:
    metastore_uri: thrift://localhost:9083

target:
  catalog_type: hadoop    # hadoop | nessie | glue | s3_tables
  catalog_name: iceberg
  database: nyctaxi

storage:
  endpoint: http://localhost:9000
  access_key: minioadmin
  secret_key: minioadmin

migration:
  write_mode: create      # create | replace | append
  repartition: null
  partition_by: []
```

### Source types

| Type | Input | `tables` entries |
|------|-------|------------------|
| `hive` | Hive metastore via Thrift | `database.table_name` |
| `parquet` | Parquet files on local/S3 | File paths (e.g. `s3a://bucket/file.parquet`) |

### Target catalog types

| Type | Backend | Use case |
|------|---------|----------|
| `hadoop` | S3A warehouse path | Local dev with MinIO, simple S3 deployments |
| `nessie` | Nessie REST catalog | Git-like branching for data lakes |
| `glue` | AWS Glue Data Catalog | AWS-native, integrates with Athena/Redshift Spectrum |
| `s3_tables` | AWS S3 Tables | Native S3 table format with automatic compaction |

## Adding a New Source

1. Create `src/hive_to_iceberg/sources/your_source.py`
2. Subclass `Source` from `sources.base` and implement three methods:
   - `configure_spark()` — add source-specific Spark config and Maven packages
   - `resolve_table()` — return a display name for a table reference
   - `read_table()` — return a Spark DataFrame for a table reference
3. Register it in `sources/registry.py`

See `sources/parquet.py` for a minimal example.

## Project Structure

```
├── src/hive_to_iceberg/
│   ├── cli.py            # Click CLI entry point
│   ├── config.py         # YAML config parsing with dataclasses
│   ├── migrate.py        # Spark session setup & Iceberg write logic
│   └── sources/          # Pluggable source implementations
│       ├── base.py       # Source ABC
│       ├── registry.py   # Source type -> implementation mapping
│       ├── hive.py       # Hive metastore source
│       └── parquet.py    # Parquet file source
├── scripts/
│   ├── load_sample_data.py
│   └── validate_yaml.py
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

1. Reads the YAML config and instantiates the configured source
2. Builds a SparkSession with Iceberg extensions, source config, and target catalog
3. For each table: reads a DataFrame from the source, optionally repartitions, and writes to Iceberg using `DataFrameWriterV2`
4. Validates row counts between source and target
5. Prints a migration summary with success/failure counts

## License

MIT
