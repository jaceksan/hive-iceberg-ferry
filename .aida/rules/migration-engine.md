---
description: Migration engine internals — how sources are read and written to Iceberg catalogs
globs:
  - src/hive_to_iceberg/migrate.py
  - src/hive_to_iceberg/config.py
alwaysApply: false
---

# Migration Engine

## Architecture

The migration has three phases:
1. **Spark session setup** — configures Hive source access, Iceberg catalog, storage, and Maven packages
2. **Per-table migration** — reads source DataFrame, optionally repartitions, writes via `DataFrameWriterV2`
3. **Validation** — compares source and target row counts

## Catalog configuration pattern

Target catalogs follow a discriminated-union pattern:
- `TargetConfig.catalog_type` selects the backend (`hadoop`, `nessie`, `glue`, `s3_tables`)
- Each type has its own dataclass with type-specific fields
- `build_spark_session()` applies catalog-specific Spark config and Maven packages

## Critical constraints

- **CRITICAL:** When adding a new Iceberg catalog type, add a new dataclass in `config.py`, a new branch in `build_spark_session()`, and document it in `config.aws.yaml`.
- **CRITICAL:** Always validate row counts after migration. Never skip the count check.
- **NEVER:** Remove or weaken the row-count validation — it's the primary correctness gate.

## Common traps

| Mistake | Symptom | Fix |
|---------|---------|-----|
| Missing Maven package | `ClassNotFoundException` at runtime | Add to `packages` list in `build_spark_session()` |
| Wrong S3A config for MinIO | `AccessDenied` or connection refused | Check `storage` section — needs `path_style_access: true` and correct endpoint |
| `create` mode on existing table | `TableAlreadyExistsException` | Use `replace` or `append` write mode |

## Future direction: pluggable sources

Current source is Hive-only. To add new sources (JDBC, Delta, Parquet, CSV):
1. Add `source.type` discriminator to `SourceConfig` (like target's `catalog_type`)
2. Extract source-specific Spark setup from `build_spark_session()`
3. Replace `spark.table()` in `migrate_table()` with source-type-specific read logic
