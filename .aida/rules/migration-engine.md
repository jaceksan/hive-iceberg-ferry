---
description: Migration engine internals — pluggable source architecture and Iceberg write path
globs:
  - src/hive_to_iceberg/migrate.py
  - src/hive_to_iceberg/config.py
  - src/hive_to_iceberg/sources/**
alwaysApply: false
---

# Migration Engine

## Architecture

The migration has three phases:
1. **Spark session setup** — source configures its Spark needs, then Iceberg catalog and storage are configured
2. **Per-table migration** — source reads a DataFrame, engine optionally repartitions, writes via `DataFrameWriterV2`
3. **Validation** — compares source and target row counts

## Pluggable source pattern

Sources follow an ABC pattern in `sources/base.py`:
- `configure_spark(builder, packages)` — source-specific Spark config and Maven JARs
- `resolve_table(table_ref)` — human-readable display name for a table reference
- `read_table(spark, table_ref)` — returns a DataFrame

The registry in `sources/registry.py` maps `config.source.type` to the correct implementation.

## How to add a new source

1. Create `src/hive_to_iceberg/sources/your_source.py` — subclass `Source`
2. Add type-specific config dataclass in `config.py` if needed (e.g. `JdbcSourceConfig`)
3. Add the new type to `SourceConfig` as an optional field
4. Register it in `sources/registry.py`
5. Add `source.type` value to `scripts/validate_yaml.py`
6. Document in `config.aws.yaml` and `README.md`

## Catalog configuration pattern

Target catalogs follow a discriminated-union pattern:
- `TargetConfig.catalog_type` selects the backend (`hadoop`, `nessie`, `glue`, `s3_tables`)
- Each type has its own dataclass with type-specific fields
- `build_spark_session()` applies catalog-specific Spark config and Maven packages

## Critical constraints

- **CRITICAL:** When adding a new Iceberg catalog type, add a new dataclass in `config.py`, a new branch in `build_spark_session()`, and document it in `config.aws.yaml`.
- **CRITICAL:** Always validate row counts after migration. Never skip the count check.
- **NEVER:** Remove or weaken the row-count validation — it's the primary correctness gate.
- **NEVER:** Put source-specific logic in `migrate.py` — it belongs in `sources/`.

## Common traps

| Mistake | Symptom | Fix |
|---------|---------|-----|
| Missing Maven package | `ClassNotFoundException` at runtime | Add to `packages` in source's `configure_spark()` or `build_spark_session()` |
| Wrong S3A config for MinIO | `AccessDenied` or connection refused | Check `storage` section — needs `path_style_access: true` and correct endpoint |
| `create` mode on existing table | `TableAlreadyExistsException` | Use `replace` or `append` write mode |
| Source logic in migrate.py | Breaks pluggability | Move to `sources/` subclass |
