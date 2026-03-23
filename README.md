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
- Java 17 (recommended) — PySpark 3.4 requires JDK 8/11/17; PySpark 3.5+ also supports JDK 21

### 1. Install

```bash
uv sync
```

### 2. Run the full flow

```bash
make all                    # setup + load + migrate + verify (Hadoop catalog)
```

Or step by step:

```bash
make setup                  # start Docker stack (Hive 3.1.3 default)
make setup PROFILE=hive4    # or Hive 4.0.1 for PySpark 4.x
make load                   # download NYC taxi data, load into Hive
make migrate                # migrate Hive -> Iceberg (Hadoop catalog)
make verify                 # run SQL checks against migrated tables
```

### Other catalog targets

```bash
make all-nessie             # migrate + verify using Nessie catalog
make migrate-s3tables       # migrate to AWS S3 Tables
```

### Teardown

```bash
make down                   # stop Docker stack
make clean                  # stop and remove volumes
```

### Direct CLI usage

For finer control, use the CLI directly:

```bash
uv run hive-to-iceberg -c config.yaml -t nyctaxi.yellow_tripdata -v
uv run python scripts/verify_migration.py -c config.yaml -q tests/checks.sql
```

## Register Mode (add_files — no data rewrite)

If your data is already on S3 as Parquet files (e.g., produced by an external export pipeline), you can create Iceberg tables **without rewriting any data**. The `register` mode uses Iceberg's `add_files` stored procedure to build only the metadata layer (manifests, snapshots, `metadata.json`) on top of your existing files.

This is ideal when:
- An external system (Hyperloop, Airflow, custom ETL) writes Parquet to S3
- Files use Hive-style partition layout (`ds=2024-01-15/`)
- You want to query them as Iceberg tables without copying data

### Quick Start (register flow)

```bash
# 1. Start local infrastructure
make setup

# 2. Write sample raw Parquet to MinIO (simulates external export)
make load-raw

# 3. Register as Iceberg tables (metadata-only, no data rewrite)
make register

# 4. Verify
make verify-register
```

Or run the full flow in one command: `make all-register`

### How it works

1. `load_raw_parquet.py` writes NYC taxi data as Hive-style partitioned Parquet to `s3a://raw-parquet/`, with the partition column (`ds`) encoded only in directory names and dropped from data files — simulating a typical external export pipeline
2. The migration tool infers the schema from the Parquet files (including partition columns from directory structure), creates an Iceberg table, and calls `add_files` to register the files
3. Iceberg reads Parquet footer statistics (min/max/null counts) and writes them into manifests — query performance (predicate pushdown, partition pruning) is equivalent to a native Iceberg write

### Configuration

```yaml
source:
  type: parquet

target:
  catalog_type: glue    # or hadoop for local dev
  # ...

tables:
  - s3a://raw-parquet/yellow_tripdata
  - s3a://raw-parquet/green_tripdata

migration:
  write_mode: register
  register_partition_columns: [ds]
```

See [`config.register.yaml`](config.register.yaml) for a complete local example.

### Glue vs S3 Tables for register mode

| Catalog | Works with add_files? | Notes |
|---------|----------------------|-------|
| Glue | Yes | Recommended for registering external Parquet |
| S3 Tables | No | Requires data to be written through its API |
| Hadoop | Yes | Good for local dev with MinIO |
| Nessie | Yes | Good for local dev with branching |

## Configuration

All settings live in a single YAML file. See [`config.yaml`](config.yaml) for a local MinIO/Hadoop example, [`config.aws.yaml`](config.aws.yaml) for AWS, and [`config.custom.yaml`](config.custom.yaml) for environments with internal Spark/Hive forks.

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
  write_mode: create      # create | replace | append | register
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

## Custom / Internal Environments

If your environment uses internal forks of Spark, Hive, or custom storage formats, see [`config.custom.yaml`](config.custom.yaml) for a detailed template. Key customization points:

| What | How |
|------|-----|
| Internal Maven repos | `spark.jars.repositories` in `extra_config` |
| Custom JARs (local paths) | `spark.jars` in `extra_config` |
| Hive metastore fork | `spark.sql.hive.metastore.version` + `jars.path` in `extra_config` |
| Custom storage format (ORC/Parquet fork) | `spark.sql.hive.convertMetastoreOrc: "false"` in `extra_config`, or create a custom source |
| Custom SerDe | Spark reads through Hive — if your Spark fork can `spark.table()` your tables, it works |

**Minimum Spark version:** 3.4 (requires `DataFrameWriterV2` API for Iceberg writes).

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
│   ├── load_sample_data.py     # Load sample data into Hive
│   ├── load_raw_parquet.py     # Write raw partitioned Parquet to MinIO
│   ├── verify_migration.py     # SQL-based post-migration verification
│   ├── validate_yaml.py
│   └── setup_docker.sh         # Docker stack bootstrap with profile support
├── tests/
│   ├── checks.sql              # SQL verification checks (editable)
│   └── checks-register.sql    # Checks for register (add_files) flow
├── docker/
│   ├── hive/             # Hive metastore config (hive-site.xml, core-site.xml)
│   └── presto/           # Presto catalog config
├── config.yaml           # Local dev config (MinIO + Hadoop catalog)
├── config.aws.yaml       # AWS config template (Glue / S3 Tables)
├── config.register.yaml  # Register mode config (add_files, no rewrite)
└── docker-compose.yml    # Full local stack
```

## Docker Compose Stack

The stack uses [Docker Compose profiles](https://docs.docker.com/compose/profiles/) to support multiple Hive versions:

| Profile | Hive image | PySpark compatibility |
|---------|------------|----------------------|
| `hive3` (default) | `apache/hive:3.1.3` | PySpark 3.5.x |
| `hive4` | `apache/hive:4.0.1` | PySpark 4.x |

Maven coordinates for Iceberg and Hadoop are auto-detected based on the installed PySpark version — no config changes needed.

| Service | Port | Purpose |
|---------|------|---------|
| PostgreSQL 16 | 5432 | Hive metastore backend |
| Hive metastore | 9083 | Thrift metastore server (version depends on profile) |
| MinIO | 9000 / 9001 | S3-compatible object storage + console |
| Presto | 8080 | Query engine for validation |

Each profile uses its own PostgreSQL volume (`postgres-data-hive3` / `postgres-data-hive4`), so switching profiles doesn't require clearing data. Just stop one and start the other:
```bash
make down
make setup PROFILE=hive4
```

## How It Works

**Standard mode** (`write_mode: create | replace | append`):
1. Reads the YAML config and instantiates the configured source
2. Builds a SparkSession with Iceberg extensions, source config, and target catalog
3. For each table: reads a DataFrame from the source, optionally repartitions, and writes to Iceberg using `DataFrameWriterV2`
4. Validates row counts between source and target
5. Prints a migration summary with success/failure counts

**Register mode** (`write_mode: register`):
1. Infers schema from existing Parquet files (including partition columns from directory structure)
2. Creates the Iceberg table if it doesn't exist
3. Calls `add_files` stored procedure to register existing files as Iceberg metadata — no data is copied or rewritten
4. Validates row counts and prints a summary

## License

MIT
