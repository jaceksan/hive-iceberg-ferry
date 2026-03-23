#!/usr/bin/env python3
"""Write sample NYC taxi data as raw Hive-style partitioned Parquet to MinIO.

This simulates an external export pipeline (like Meta's Hyperloop) that produces
Parquet files partitioned by a date column (ds), where ds is encoded only in the
directory structure and dropped from the data files themselves.

Output layout on MinIO (s3a://raw-parquet/):
    yellow_tripdata/ds=2024-01-15/part-00000-*.parquet
    yellow_tripdata/ds=2024-01-16/part-00000-*.parquet
    green_tripdata/ds=2024-01-15/part-00000-*.parquet
    ...

Usage:
    uv run python scripts/load_raw_parquet.py              # default: 10 000 rows
    uv run python scripts/load_raw_parquet.py --rows 0     # all rows
    uv run python scripts/load_raw_parquet.py --partitions 3
"""
from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Same datasets as load_sample_data.py
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
OUTPUT_BUCKET = "s3a://raw-parquet"

# Synthetic ds values to simulate daily partitions
DEFAULT_DS_VALUES = ["2024-01-15", "2024-01-16"]


def download_datasets() -> dict[str, Path]:
    import urllib.request

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


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("load-raw-parquet")
        .master("local[*]")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4",
        )
        .getOrCreate()
    )


def write_raw_parquet(max_rows: int, num_partitions: int) -> None:
    paths = download_datasets()
    spark = build_spark()

    ds_values = DEFAULT_DS_VALUES[:num_partitions]

    for name, parquet_path in paths.items():
        print(f"Writing {name} as partitioned Parquet ...")

        df = spark.read.parquet(str(parquet_path))
        if max_rows > 0:
            df = df.limit(max_rows)

        total_rows = 0
        for ds_val in ds_values:
            # Add ds column, then write partitioned by ds.
            # Spark's partitionBy removes ds from the data files and encodes
            # it only in the directory structure — exactly like Meta's export.
            partitioned = df.withColumn("ds", lit(ds_val))
            output_path = f"{OUTPUT_BUCKET}/{name}"

            partitioned.write.mode("append").partitionBy("ds").parquet(output_path)

            count = partitioned.count()
            total_rows += count
            print(f"  ds={ds_val}: {count} rows")

        print(f"  -> {output_path}  (total: {total_rows} rows)")

    spark.stop()
    print("Done.")


def main():
    parser = argparse.ArgumentParser(
        description="Write sample data as raw partitioned Parquet to MinIO",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=10_000,
        help="Max rows per table per partition (0 = all rows). Default: 10000",
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=2,
        choices=range(1, len(DEFAULT_DS_VALUES) + 1),
        help=f"Number of ds partitions to create (max {len(DEFAULT_DS_VALUES)}). Default: 2",
    )
    args = parser.parse_args()
    write_raw_parquet(args.rows, args.partitions)


if __name__ == "__main__":
    main()
