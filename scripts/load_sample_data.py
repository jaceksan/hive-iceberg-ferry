#!/usr/bin/env python3
"""Download sample NYC taxi data and load it into Hive tables via PySpark.

Usage:
    uv run python scripts/load_sample_data.py            # default: 10 000 rows
    uv run python scripts/load_sample_data.py --rows 0   # all rows (full file)
"""
from __future__ import annotations

import argparse
import os
import urllib.request
from pathlib import Path

from pyspark.sql import SparkSession

DATASETS = {
    "yellow_tripdata": (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        "yellow_tripdata_2024-01.parquet"
    ),
    "green_tripdata": (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        "green_tripdata_2024-01.parquet"
    ),
}

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DATABASE = "nyctaxi"


def download_datasets() -> dict[str, Path]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    paths = {}
    for name, url in DATASETS.items():
        dest = DATA_DIR / f"{name}.parquet"
        if not dest.exists():
            print(f"Downloading {name} ...")
            urllib.request.urlretrieve(url, dest)
            size_mb = dest.stat().st_size / 1024 / 1024
            print(f"  -> {dest}  ({size_mb:.1f} MB)")
        else:
            print(f"  {name} already downloaded")
        paths[name] = dest
    return paths


def _find_ivy_jars(*artifacts: str) -> list[str]:
    """Find downloaded Maven JARs in the Ivy cache."""
    ivy_jars = Path.home() / ".ivy2" / "jars"
    found = []
    for art in artifacts:
        matches = list(ivy_jars.glob(f"*{art}*"))
        if matches:
            found.append(str(matches[0]))
    return found


def build_spark() -> SparkSession:
    # First pass: resolve Maven packages so JARs land in Ivy cache.
    # We need hadoop-aws on the driver classpath for Hive's S3A support.
    packages = "org.apache.hadoop:hadoop-aws:3.3.4"

    # Check if JARs are already cached; if so, add them to driver classpath
    # so Spark's embedded Hive client can resolve S3AFileSystem.
    extra_cp = _find_ivy_jars("hadoop-aws", "aws-java-sdk-bundle")
    extra_cp_str = ":".join(extra_cp) if extra_cp else ""

    builder = (
        SparkSession.builder
        .appName("load-sample-data")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "s3a://hive-warehouse/")
        .config("spark.hadoop.hive.metastore.uris", "thrift://localhost:9083")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config("spark.jars.packages", packages)
    )

    if extra_cp_str:
        builder = builder.config("spark.driver.extraClassPath", extra_cp_str)

    return builder.enableHiveSupport().getOrCreate()


def load_to_hive(max_rows: int) -> None:
    paths = download_datasets()
    spark = build_spark()

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")

    for name, parquet_path in paths.items():
        full_name = f"{DATABASE}.{name}"
        print(f"Loading {full_name} ...")

        df = spark.read.parquet(str(parquet_path))
        if max_rows > 0:
            df = df.limit(max_rows)

        df.write.mode("overwrite").saveAsTable(full_name)
        count = spark.table(full_name).count()
        print(f"  -> {count} rows in {full_name}")

    spark.stop()
    print("Done.")


def main():
    parser = argparse.ArgumentParser(description="Load sample data into Hive")
    parser.add_argument(
        "--rows",
        type=int,
        default=10_000,
        help="Max rows per table (0 = all rows). Default: 10000",
    )
    args = parser.parse_args()
    load_to_hive(args.rows)


if __name__ == "__main__":
    main()
