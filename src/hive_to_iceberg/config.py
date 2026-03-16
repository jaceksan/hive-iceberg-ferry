from __future__ import annotations

import yaml
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class SourceConfig:
    metastore_uri: str
    database: str = "default"


@dataclass
class HadoopCatalogConfig:
    warehouse: str = ""


@dataclass
class NessieCatalogConfig:
    uri: str = ""
    warehouse: str = ""


@dataclass
class GlueCatalogConfig:
    warehouse: str = ""
    region: str = "us-east-1"


@dataclass
class S3TablesCatalogConfig:
    warehouse: str = ""
    region: str = "us-east-1"


@dataclass
class TargetConfig:
    catalog_type: str  # hadoop, nessie, glue, s3_tables
    catalog_name: str = "iceberg"
    database: str = "default"
    hadoop: Optional[HadoopCatalogConfig] = None
    nessie: Optional[NessieCatalogConfig] = None
    glue: Optional[GlueCatalogConfig] = None
    s3_tables: Optional[S3TablesCatalogConfig] = None


@dataclass
class StorageConfig:
    endpoint: Optional[str] = None
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    path_style_access: bool = True


@dataclass
class SparkConfig:
    master: str = "local[*]"
    app_name: str = "hive-to-iceberg"
    driver_memory: str = "2g"
    extra_packages: list[str] = field(default_factory=list)
    extra_config: dict[str, str] = field(default_factory=dict)


@dataclass
class MigrationConfig:
    write_mode: str = "create"
    repartition: Optional[int] = None
    partition_by: list[str] = field(default_factory=list)


@dataclass
class Config:
    source: SourceConfig
    target: TargetConfig
    storage: Optional[StorageConfig] = None
    spark: SparkConfig = field(default_factory=SparkConfig)
    tables: list[str] = field(default_factory=list)
    migration: MigrationConfig = field(default_factory=MigrationConfig)


def _parse_nested(cls, raw: dict | None):
    if raw is None:
        return None
    return cls(**raw)


def load_config(path: str) -> Config:
    with open(path) as f:
        raw = yaml.safe_load(f)

    source = SourceConfig(**raw["source"])

    t = raw["target"]
    target = TargetConfig(
        catalog_type=t["catalog_type"],
        catalog_name=t.get("catalog_name", "iceberg"),
        database=t.get("database", "default"),
        hadoop=_parse_nested(HadoopCatalogConfig, t.get("hadoop")),
        nessie=_parse_nested(NessieCatalogConfig, t.get("nessie")),
        glue=_parse_nested(GlueCatalogConfig, t.get("glue")),
        s3_tables=_parse_nested(S3TablesCatalogConfig, t.get("s3_tables")),
    )

    storage = _parse_nested(StorageConfig, raw.get("storage"))
    spark = SparkConfig(**raw.get("spark", {}))
    migration = MigrationConfig(**raw.get("migration", {}))

    return Config(
        source=source,
        target=target,
        storage=storage,
        spark=spark,
        tables=raw.get("tables", []),
        migration=migration,
    )
