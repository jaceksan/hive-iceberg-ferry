from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from ..config import HiveSourceConfig
from .base import Source


class HiveSource(Source):
    """Reads tables from a Hive metastore via Thrift."""

    def __init__(self, cfg: HiveSourceConfig) -> None:
        self._cfg = cfg

    def configure_spark(
        self,
        builder: SparkSession.Builder,
        packages: list[str],
    ) -> SparkSession.Builder:
        builder = builder.enableHiveSupport()
        builder = builder.config(
            "spark.hadoop.hive.metastore.uris", self._cfg.metastore_uri,
        )
        return builder

    def resolve_table(self, table_ref: str) -> str:
        if "." in table_ref:
            return table_ref
        return f"{self._cfg.database}.{table_ref}"

    def read_table(
        self,
        spark: SparkSession,
        table_ref: str,
    ) -> DataFrame:
        return spark.table(self.resolve_table(table_ref))
