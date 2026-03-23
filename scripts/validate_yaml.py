"""Validate that project YAML config files are well-formed."""
from __future__ import annotations

import sys
from pathlib import Path

import yaml

CONFIG_FILES = [
    "config.yaml",
    "config.aws.yaml",
    "config.custom.yaml",
    "config.nessie.yaml",
    "config.s3tables.yaml",
    "config.register.yaml",
]

REQUIRED_TOP_LEVEL_KEYS = {"source", "target"}


def validate_file(path: Path) -> list[str]:
    errors: list[str] = []
    if not path.exists():
        errors.append(f"{path}: file not found")
        return errors
    try:
        with open(path) as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as exc:
        errors.append(f"{path}: invalid YAML — {exc}")
        return errors

    if not isinstance(data, dict):
        errors.append(f"{path}: expected a mapping at top level")
        return errors

    for key in REQUIRED_TOP_LEVEL_KEYS:
        if key not in data:
            errors.append(f"{path}: missing required key '{key}'")

    source = data.get("source")
    if isinstance(source, dict):
        src_type = source.get("type", "hive")
        valid_source_types = {"hive", "parquet"}
        if src_type not in valid_source_types:
            errors.append(
                f"{path}: source.type '{src_type}' not in {valid_source_types}"
            )
        if src_type == "hive":
            # Accept both nested hive.metastore_uri and legacy flat metastore_uri
            hive_cfg = source.get("hive")
            has_nested = isinstance(hive_cfg, dict) and "metastore_uri" in hive_cfg
            has_flat = "metastore_uri" in source
            if not has_nested and not has_flat:
                errors.append(
                    f"{path}: hive source requires 'hive.metastore_uri' or 'metastore_uri'"
                )

    target = data.get("target")
    if isinstance(target, dict):
        cat_type = target.get("catalog_type")
        valid_catalog_types = {"hadoop", "nessie", "glue", "s3_tables"}
        if cat_type and cat_type not in valid_catalog_types:
            errors.append(
                f"{path}: target.catalog_type '{cat_type}' not in {valid_catalog_types}"
            )

    return errors


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    all_errors: list[str] = []
    for name in CONFIG_FILES:
        all_errors.extend(validate_file(root / name))

    if all_errors:
        for err in all_errors:
            print(f"ERROR: {err}", file=sys.stderr)
        return 1

    print(f"OK: {len(CONFIG_FILES)} config files validated")
    return 0


if __name__ == "__main__":
    sys.exit(main())
