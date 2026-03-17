#!/usr/bin/env python3
"""Verify migrated Iceberg tables by running SQL checks.

Reads the project config to build a SparkSession with the same catalog
setup used during migration, then executes SQL statements from a file.

Each statement can be:
  - A bare query   → results are printed
  - An ASSERT      → row must return a truthy first column, or the check fails

ASSERT syntax (line prefix):
    -- ASSERT: description of the check
    SELECT (hive_count = iceberg_count) AS ok FROM ...

Usage:
    uv run python scripts/verify_migration.py
    uv run python scripts/verify_migration.py -c config.yaml -q tests/checks.sql
    uv run python scripts/verify_migration.py -v
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from hive_to_iceberg.config import load_config
from hive_to_iceberg.migrate import build_spark_session
from hive_to_iceberg.sources import get_source

logger = logging.getLogger(__name__)

DEFAULT_CHECKS = Path(__file__).resolve().parent.parent / "tests" / "checks.sql"


def parse_sql_file(path: Path) -> list[dict]:
    """Parse a SQL file into a list of executable checks.

    Returns a list of dicts with keys:
      - sql: the SQL statement
      - assert_msg: assertion description (None for plain queries)
    """
    checks: list[dict] = []
    lines: list[str] = []
    assert_msg: str | None = None

    for raw_line in path.read_text().splitlines():
        stripped = raw_line.strip()

        # Skip blank lines and plain comments
        if not stripped:
            if lines:
                checks.append({"sql": "\n".join(lines), "assert_msg": assert_msg})
                lines = []
                assert_msg = None
            continue

        if stripped.startswith("-- ASSERT:"):
            assert_msg = stripped.removeprefix("-- ASSERT:").strip()
            continue

        if stripped.startswith("--"):
            continue

        lines.append(raw_line)

    if lines:
        checks.append({"sql": "\n".join(lines), "assert_msg": assert_msg})

    return checks


def run_checks(config_path: str, sql_path: Path, verbose: bool) -> int:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )

    if not sql_path.exists():
        logger.error("SQL checks file not found: %s", sql_path)
        return 1

    checks = parse_sql_file(sql_path)
    if not checks:
        logger.warning("No SQL checks found in %s", sql_path)
        return 0

    config = load_config(config_path)
    source = get_source(config.source)
    spark = build_spark_session(config, source)

    passed = 0
    failed = 0

    for i, check in enumerate(checks, 1):
        sql = check["sql"]
        assert_msg = check["assert_msg"]
        label = assert_msg or f"Query #{i}"

        logger.info("Running: %s", label)
        logger.debug("SQL:\n%s", sql)

        try:
            result = spark.sql(sql)
            rows = result.collect()

            if assert_msg is not None:
                if not rows:
                    print(f"  FAIL  {label}: query returned no rows")
                    failed += 1
                    continue

                first_value = rows[0][0]
                if first_value:
                    print(f"  OK    {label}")
                    passed += 1
                else:
                    result.show(truncate=False)
                    print(f"  FAIL  {label}: assertion returned {first_value}")
                    failed += 1
            else:
                print(f"  ---   {label}")
                result.show(truncate=False)
                passed += 1

        except Exception as exc:
            print(f"  FAIL  {label}: {exc}")
            failed += 1

    spark.stop()

    print()
    print("=" * 60)
    print(f"Verification: {passed} passed, {failed} failed")
    print("=" * 60)

    return 1 if failed else 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify migrated Iceberg tables")
    parser.add_argument(
        "-c", "--config",
        default="config.yaml",
        help="Path to YAML config file (default: config.yaml)",
    )
    parser.add_argument(
        "-q", "--queries",
        default=str(DEFAULT_CHECKS),
        help=f"Path to SQL checks file (default: {DEFAULT_CHECKS})",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Debug logging")
    args = parser.parse_args()
    sys.exit(run_checks(args.config, Path(args.queries), args.verbose))


if __name__ == "__main__":
    main()
