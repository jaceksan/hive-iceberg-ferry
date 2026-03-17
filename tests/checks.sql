-- Verification checks for Hive -> Iceberg migration
-- Run with: uv run python scripts/verify_migration.py

-- ============================================================
-- Row count assertions (source vs target must match)
-- ============================================================

-- ASSERT: yellow_tripdata row counts match between Hive and Iceberg
SELECT (SELECT count(*) FROM nyctaxi.yellow_tripdata) = (SELECT count(*) FROM iceberg.nyctaxi.yellow_tripdata) AS ok

-- ASSERT: green_tripdata row counts match between Hive and Iceberg
SELECT (SELECT count(*) FROM nyctaxi.green_tripdata) = (SELECT count(*) FROM iceberg.nyctaxi.green_tripdata) AS ok

-- ============================================================
-- Non-empty table assertions
-- ============================================================

-- ASSERT: yellow_tripdata Iceberg table is not empty
SELECT count(*) > 0 AS ok FROM iceberg.nyctaxi.yellow_tripdata

-- ASSERT: green_tripdata Iceberg table is not empty
SELECT count(*) > 0 AS ok FROM iceberg.nyctaxi.green_tripdata

-- ============================================================
-- Data sample (informational, not asserted)
-- ============================================================

SELECT 'yellow_tripdata' AS tbl, count(*) AS iceberg_rows FROM iceberg.nyctaxi.yellow_tripdata UNION ALL SELECT 'green_tripdata' AS tbl, count(*) AS iceberg_rows FROM iceberg.nyctaxi.green_tripdata
