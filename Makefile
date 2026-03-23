# Iceberg Ferry — local development targets
#
# Usage:
#   make setup                  # Start Docker stack (hive3 default)
#   make setup PROFILE=hive4    # Start with Hive 4 profile
#   make load                   # Load sample data into Hive
#   make migrate                # Migrate Hive -> Iceberg (Hadoop catalog)
#   make verify                 # Verify migration results
#   make all                    # Full flow: setup + load + migrate + verify
#
#   make migrate-nessie         # Migrate Hive -> Iceberg (Nessie catalog)
#   make verify-nessie          # Verify Nessie migration
#   make all-nessie             # Full Nessie flow (assumes setup + load done)
#
#   make migrate-s3tables       # Migrate Hive -> AWS S3 Tables
#
#   make load-raw               # Write raw partitioned Parquet to MinIO
#   make register               # Register Parquet as Iceberg (add_files, no rewrite)
#   make verify-register        # Verify registered tables
#   make all-register           # Full register flow: setup + load-raw + register + verify
#
#   make down                   # Stop Docker stack
#   make clean                  # Stop stack and remove volumes

PROFILE ?= hive3

.PHONY: setup down clean load \
        migrate verify all \
        migrate-nessie verify-nessie all-nessie \
        migrate-s3tables verify-s3tables all-s3tables \
        load-raw register verify-register all-register \
        validate-yaml

# --- Infrastructure ---

setup:
	./scripts/setup_docker.sh --profile $(PROFILE)

down:
	docker compose --profile $(PROFILE) down

clean:
	docker compose --profile $(PROFILE) down -v

# --- Data loading ---

load:
	uv run python scripts/load_sample_data.py

# --- Hadoop catalog (default local) ---

migrate:
	uv run hive-to-iceberg -c config.yaml -v

verify:
	uv run python scripts/verify_migration.py

all: setup load migrate verify

# --- Nessie catalog ---

migrate-nessie:
	uv run hive-to-iceberg -c config.nessie.yaml -v

verify-nessie:
	uv run python scripts/verify_migration.py -c config.nessie.yaml -q tests/checks-nessie.sql

all-nessie: migrate-nessie verify-nessie

# --- AWS S3 Tables ---

migrate-s3tables:
	uv run hive-to-iceberg -c config.s3tables.yaml -v

verify-s3tables:
	uv run python scripts/verify_migration.py -c config.s3tables.yaml -q tests/checks-s3tables.sql

all-s3tables: migrate-s3tables verify-s3tables

# --- Register (add_files — metadata-only, no data rewrite) ---

load-raw:
	uv run python scripts/load_raw_parquet.py

register:
	uv run hive-to-iceberg -c config.register.yaml -v

verify-register:
	uv run python scripts/verify_migration.py -c config.register.yaml -q tests/checks-register.sql

all-register: setup load-raw register verify-register

# --- Validation ---

validate-yaml:
	uv run python scripts/validate_yaml.py
