"""Validate that project YAML config files are well-formed."""
from __future__ import annotations

import sys
from pathlib import Path

import yaml

CONFIG_FILES = [
    "config.yaml",
    "config.aws.yaml",
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

    target = data.get("target")
    if isinstance(target, dict):
        cat_type = target.get("catalog_type")
        valid_types = {"hadoop", "nessie", "glue", "s3_tables"}
        if cat_type and cat_type not in valid_types:
            errors.append(
                f"{path}: target.catalog_type '{cat_type}' not in {valid_types}"
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
