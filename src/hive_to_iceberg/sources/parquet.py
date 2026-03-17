from __future__ import annotations

from pathlib import PurePosixPath

from pyspark.sql import DataFrame, SparkSession

from .base import Source


class ParquetSource(Source):
    """Reads tables from Parquet files on local or S3 storage.

    Each ``table_ref`` is a file path (local or ``s3a://``).
    The target table name is derived from the file stem.
    """

    def configure_spark(
        self,
        builder: SparkSession.Builder,
        packages: list[str],
    ) -> SparkSession.Builder:
        # Parquet is built into Spark — no extra config needed.
        return builder

    def resolve_table(self, table_ref: str) -> str:
        return PurePosixPath(table_ref).stem

    def read_table(
        self,
        spark: SparkSession,
        table_ref: str,
    ) -> DataFrame:
        return spark.read.parquet(table_ref)
