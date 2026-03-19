-- Verification checks for Hive -> S3 Tables migration
-- Run with: uv run python scripts/verify_migration.py -c config.s3tables.yaml -q tests/checks-s3tables.sql
--
-- NOTE: Update the catalog and namespace below to match your config.s3tables.yaml
--   s3tablescatalog  -> target.catalog_name
--   my_namespace     -> target.database

-- ASSERT: yellow_tripdata S3 Tables table is not empty
SELECT count(*) > 0 AS ok FROM s3tablescatalog.my_namespace.yellow_tripdata

-- ASSERT: green_tripdata S3 Tables table is not empty
SELECT count(*) > 0 AS ok FROM s3tablescatalog.my_namespace.green_tripdata

-- ASSERT: yellow_tripdata row counts match between Hive and S3 Tables
SELECT (SELECT count(*) FROM nyctaxi.yellow_tripdata) = (SELECT count(*) FROM s3tablescatalog.my_namespace.yellow_tripdata) AS ok

-- ASSERT: green_tripdata row counts match between Hive and S3 Tables
SELECT (SELECT count(*) FROM nyctaxi.green_tripdata) = (SELECT count(*) FROM s3tablescatalog.my_namespace.green_tripdata) AS ok

-- Data sample (informational)
SELECT 'yellow_tripdata' AS tbl, count(*) AS rows FROM s3tablescatalog.my_namespace.yellow_tripdata UNION ALL SELECT 'green_tripdata' AS tbl, count(*) AS rows FROM s3tablescatalog.my_namespace.green_tripdata
