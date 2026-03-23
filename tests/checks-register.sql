-- Verification checks for register (add_files) flow
-- Run with: uv run python scripts/verify_migration.py -c config.register.yaml -q tests/checks-register.sql

-- ============================================================
-- Non-empty table assertions
-- ============================================================

-- ASSERT: yellow_tripdata Iceberg table is not empty
SELECT count(*) > 0 AS ok FROM iceberg.nyctaxi.yellow_tripdata

-- ASSERT: green_tripdata Iceberg table is not empty
SELECT count(*) > 0 AS ok FROM iceberg.nyctaxi.green_tripdata

-- ============================================================
-- Partition column assertions (ds must exist and have values)
-- ============================================================

-- ASSERT: yellow_tripdata has ds partition column with expected values
SELECT count(DISTINCT ds) > 0 AS ok FROM iceberg.nyctaxi.yellow_tripdata

-- ASSERT: green_tripdata has ds partition column with expected values
SELECT count(DISTINCT ds) > 0 AS ok FROM iceberg.nyctaxi.green_tripdata

-- ============================================================
-- Partition pruning check (query a specific ds value)
-- ============================================================

-- ASSERT: yellow_tripdata partition ds=2024-01-15 has rows
SELECT count(*) > 0 AS ok FROM iceberg.nyctaxi.yellow_tripdata WHERE ds = '2024-01-15'

-- ASSERT: green_tripdata partition ds=2024-01-15 has rows
SELECT count(*) > 0 AS ok FROM iceberg.nyctaxi.green_tripdata WHERE ds = '2024-01-15'

-- ============================================================
-- Data sample (informational)
-- ============================================================

SELECT 'yellow_tripdata' AS tbl, count(*) AS rows, count(DISTINCT ds) AS partitions FROM iceberg.nyctaxi.yellow_tripdata UNION ALL SELECT 'green_tripdata' AS tbl, count(*) AS rows, count(DISTINCT ds) AS partitions FROM iceberg.nyctaxi.green_tripdata
