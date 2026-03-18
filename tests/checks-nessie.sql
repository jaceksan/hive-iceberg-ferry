-- Verification checks for Hive -> Iceberg migration (Nessie catalog)
-- Run with: uv run python scripts/verify_migration.py -c config.nessie.yaml -q tests/checks-nessie.sql

-- ASSERT: yellow_tripdata row counts match between Hive and Nessie/Iceberg
SELECT (SELECT count(*) FROM nyctaxi.yellow_tripdata) = (SELECT count(*) FROM nessie.nyctaxi.yellow_tripdata) AS ok

-- ASSERT: green_tripdata row counts match between Hive and Nessie/Iceberg
SELECT (SELECT count(*) FROM nyctaxi.green_tripdata) = (SELECT count(*) FROM nessie.nyctaxi.green_tripdata) AS ok

-- ASSERT: yellow_tripdata Nessie/Iceberg table is not empty
SELECT count(*) > 0 AS ok FROM nessie.nyctaxi.yellow_tripdata

-- ASSERT: green_tripdata Nessie/Iceberg table is not empty
SELECT count(*) > 0 AS ok FROM nessie.nyctaxi.green_tripdata

-- Data sample (informational)
SELECT 'yellow_tripdata' AS tbl, count(*) AS iceberg_rows FROM nessie.nyctaxi.yellow_tripdata UNION ALL SELECT 'green_tripdata' AS tbl, count(*) AS iceberg_rows FROM nessie.nyctaxi.green_tripdata
