from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession


class Source(ABC):
    """Base class for all migration sources.

    To add a new source type:
    1. Create a new module in this package (e.g. ``jdbc.py``)
    2. Subclass ``Source`` and implement the three abstract methods
    3. Register it in ``registry.py``
    """

    @abstractmethod
    def configure_spark(
        self,
        builder: SparkSession.Builder,
        packages: list[str],
    ) -> SparkSession.Builder:
        """Apply source-specific Spark configuration.

        Modify *builder* in place (add Hive support, set metastore URIs, etc.)
        and append any required Maven coordinates to *packages*.
        Return the builder.
        """

    @abstractmethod
    def resolve_table(self, table_ref: str) -> str:
        """Return a human-readable display name for *table_ref*.

        For catalog sources this is typically ``database.table``.
        For file sources it may be the file stem.
        """

    @abstractmethod
    def read_table(
        self,
        spark: SparkSession,
        table_ref: str,
    ) -> DataFrame:
        """Read the table identified by *table_ref* and return a DataFrame."""
