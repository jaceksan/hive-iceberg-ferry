from __future__ import annotations

import logging

import pyspark
from pyspark.sql import SparkSession

from .config import Config
from .sources import Source, get_source

logger = logging.getLogger(__name__)

# --- Version-dependent Maven coordinates ---

_SPARK_VERSION = tuple(int(x) for x in pyspark.__version__.split(".")[:2])


def _iceberg_packages() -> tuple[str, str, str, str]:
    """Return (iceberg_spark, hadoop_aws, iceberg_aws, s3_tables_catalog)
    matched to the installed PySpark version."""
    major, minor = _SPARK_VERSION

    if major >= 4:
        return (
            "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.8.1",
            "org.apache.hadoop:hadoop-aws:3.4.1",
            "org.apache.iceberg:iceberg-aws-bundle:1.8.1",
            "software.amazon.s3tables:s3-tables-catalog-for-iceberg:0.1.8",
        )
    if (major, minor) >= (3, 5):
        return (
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "org.apache.iceberg:iceberg-aws-bundle:1.7.1",
            "software.amazon.s3tables:s3-tables-catalog-for-iceberg:0.1.8",
        )
    # Spark 3.4
    return (
        "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.7.1",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "org.apache.iceberg:iceberg-aws-bundle:1.7.1",
        "software.amazon.s3tables:s3-tables-catalog-for-iceberg:0.1.8",
    )


def build_spark_session(config: Config, source: Source) -> SparkSession:
    """Build a SparkSession with source and Iceberg target catalogs."""
    iceberg_spark, hadoop_aws, iceberg_aws, s3_tables_catalog = _iceberg_packages()

    logger.info(
        "PySpark %s detected, using Iceberg runtime: %s",
        pyspark.__version__, iceberg_spark,
    )

    builder = (
        SparkSession.builder
        .appName(config.spark.app_name)
        .master(config.spark.master)
        .config("spark.driver.memory", config.spark.driver_memory)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
    )

    packages: list[str] = [iceberg_spark, hadoop_aws]

    # Source-specific configuration
    builder = source.configure_spark(builder, packages)

    # --- Iceberg catalog configuration ---
    cat = config.target.catalog_name
    cat_type = config.target.catalog_type

    if cat_type == "hadoop":
        cfg = config.target.hadoop
        builder = (
            builder
            .config(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{cat}.type", "hadoop")
            .config(f"spark.sql.catalog.{cat}.warehouse", cfg.warehouse)
        )

    elif cat_type == "nessie":
        cfg = config.target.nessie
        packages.append(iceberg_aws)
        builder = (
            builder
            .config(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{cat}.type", "rest")
            .config(f"spark.sql.catalog.{cat}.uri", cfg.uri)
            .config(f"spark.sql.catalog.{cat}.warehouse", cfg.warehouse)
        )

    elif cat_type == "glue":
        cfg = config.target.glue
        packages.append(iceberg_aws)
        builder = (
            builder
            .config(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
            .config(
                f"spark.sql.catalog.{cat}.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog",
            )
            .config(f"spark.sql.catalog.{cat}.warehouse", cfg.warehouse)
            .config(
                f"spark.sql.catalog.{cat}.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
            )
            .config(f"spark.sql.catalog.{cat}.region", cfg.region)
        )

    elif cat_type == "s3_tables":
        cfg = config.target.s3_tables
        packages.extend([iceberg_aws, s3_tables_catalog])
        builder = (
            builder
            .config(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
            .config(
                f"spark.sql.catalog.{cat}.catalog-impl",
                "software.amazon.s3tables.iceberg.S3TablesCatalog",
            )
            .config(f"spark.sql.catalog.{cat}.warehouse", cfg.warehouse)
            .config(f"spark.sql.catalog.{cat}.region", cfg.region)
        )

    else:
        raise ValueError(f"Unknown catalog type: {cat_type}")

    # S3/MinIO storage overrides
    if config.storage and config.storage.endpoint:
        # Use per-bucket config when mixing local MinIO with real AWS.
        # Per-bucket: spark.hadoop.fs.s3a.bucket.<name>.<property>
        # Global fallback for endpoints that are clearly non-AWS (local dev).
        bucket = config.storage.bucket
        if bucket:
            prefix = f"spark.hadoop.fs.s3a.bucket.{bucket}"
            builder = (
                builder
                .config(f"{prefix}.endpoint", config.storage.endpoint)
                .config(f"{prefix}.access.key", config.storage.access_key)
                .config(f"{prefix}.secret.key", config.storage.secret_key)
                .config(f"{prefix}.path.style.access",
                        str(config.storage.path_style_access).lower())
            )
        else:
            builder = (
                builder
                .config("spark.hadoop.fs.s3a.endpoint", config.storage.endpoint)
                .config("spark.hadoop.fs.s3a.access.key", config.storage.access_key)
                .config("spark.hadoop.fs.s3a.secret.key", config.storage.secret_key)
                .config(
                    "spark.hadoop.fs.s3a.path.style.access",
                    str(config.storage.path_style_access).lower(),
                )
            )
        builder = builder.config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )

    # Merge extra packages and config from YAML
    packages.extend(config.spark.extra_packages)
    builder = builder.config("spark.jars.packages", ",".join(packages))
    for k, v in config.spark.extra_config.items():
        builder = builder.config(k, v)

    return builder.getOrCreate()


def _register_table(
    spark: SparkSession,
    config: Config,
    table_ref: str,
    full_target: str,
) -> dict:
    """Register existing Parquet files as an Iceberg table using add_files.

    This avoids rewriting data — only Iceberg metadata (manifests, snapshots,
    metadata.json) is created.  The Parquet files must already exist on S3 in
    Hive-style partition layout (e.g. ds=2024-01-15/).
    """
    cat = config.target.catalog_name
    partition_cols = config.migration.register_partition_columns

    # Read the Parquet directory to infer schema (Spark reads the partition
    # columns from directory names and adds them to the schema automatically).
    inferred = spark.read.parquet(table_ref)
    data_schema = inferred.schema
    logger.info("  Inferred schema: %s", data_schema.simpleString())

    # Ensure target namespace exists
    target_db = config.target.database
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.{target_db}")

    # Build CREATE TABLE with partition spec
    col_defs = ", ".join(
        f"`{f.name}` {f.dataType.simpleString()}" for f in data_schema.fields
    )
    partition_clause = ""
    if partition_cols:
        partition_clause = f" PARTITIONED BY ({', '.join(f'`{c}`' for c in partition_cols)})"

    create_sql = (
        f"CREATE TABLE IF NOT EXISTS {full_target} ({col_defs})"
        f" USING iceberg{partition_clause}"
    )
    logger.info("  Creating table: %s", create_sql)
    spark.sql(create_sql)

    # Build the source_table reference for add_files.
    # Backtick-quoted format tells Spark to read raw Parquet from this path.
    source_table = f"`parquet`.`{table_ref}`"

    add_files_sql = (
        f"CALL {cat}.system.add_files("
        f"table => '{full_target}', "
        f"source_table => \"{source_table}\")"
    )
    logger.info("  Registering files: %s", add_files_sql)
    spark.sql(add_files_sql)

    tgt_count = spark.table(full_target).count()
    logger.info("  Registered rows: %d", tgt_count)

    return {
        "source": table_ref,
        "target": full_target,
        "source_rows": tgt_count,  # no separate source count for register mode
        "target_rows": tgt_count,
    }


def migrate_table(
    spark: SparkSession,
    source: Source,
    config: Config,
    table_ref: str,
    target_db: str,
) -> dict:
    """Migrate a single table to the Iceberg target catalog."""
    cat = config.target.catalog_name
    src_name = source.resolve_table(table_ref)

    # For catalog sources, derive the target table name from the last segment
    target_table_name = src_name.rsplit(".", 1)[-1]
    full_target = f"{cat}.{target_db}.{target_table_name}"

    logger.info("Migrating %s -> %s", src_name, full_target)

    mode = config.migration.write_mode

    # Register mode: use add_files instead of rewriting data
    if mode == "register":
        return _register_table(spark, config, table_ref, full_target)

    df = source.read_table(spark, table_ref)
    src_count = df.count()
    logger.info("  Source rows: %d", src_count)

    if config.migration.repartition:
        df = df.repartition(config.migration.repartition)

    # Ensure target namespace exists
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.{target_db}")

    # Write using DataFrameWriterV2
    writer = df.writeTo(full_target).using("iceberg")

    if config.migration.partition_by:
        from pyspark.sql.functions import col
        writer = writer.partitionedBy(*[col(c) for c in config.migration.partition_by])

    if mode == "create":
        writer.create()
    elif mode == "replace":
        writer.createOrReplace()
    elif mode == "append":
        writer.append()
    else:
        raise ValueError(f"Unknown write_mode: {mode}")

    tgt_count = spark.table(full_target).count()
    logger.info("  Target rows: %d", tgt_count)

    return {
        "source": src_name,
        "target": full_target,
        "source_rows": src_count,
        "target_rows": tgt_count,
    }


def run_migration(config: Config, tables: list[str] | None = None) -> list[dict]:
    """Run the full migration for all configured tables."""
    tables = tables or config.tables
    if not tables:
        logger.warning("No tables to migrate")
        return []

    source = get_source(config.source)
    target_db = config.target.database
    spark = build_spark_session(config, source)

    results = []
    for table in tables:
        try:
            result = migrate_table(spark, source, config, table, target_db)
            result["status"] = "success"
        except Exception as e:
            logger.error("Failed to migrate %s: %s", table, e, exc_info=True)
            result = {"source": table, "status": "failed", "error": str(e)}
        results.append(result)

    spark.stop()
    return results
