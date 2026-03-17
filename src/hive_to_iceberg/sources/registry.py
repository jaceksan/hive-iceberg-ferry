from __future__ import annotations

from ..config import SourceConfig
from .base import Source


def get_source(config: SourceConfig) -> Source:
    """Instantiate the correct Source implementation based on config.type."""
    source_type = config.type

    if source_type == "hive":
        from .hive import HiveSource
        if config.hive is None:
            raise ValueError("source.hive config is required when source.type is 'hive'")
        return HiveSource(config.hive)

    if source_type == "parquet":
        from .parquet import ParquetSource
        return ParquetSource()

    raise ValueError(
        f"Unknown source type: '{source_type}'. "
        f"Supported types: hive, parquet"
    )
