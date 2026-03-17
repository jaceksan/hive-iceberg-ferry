#!/usr/bin/env bash
# Download external dependencies and start the Docker Compose stack.
#
# Usage:
#   ./scripts/setup_docker.sh              # default: hive3 profile
#   ./scripts/setup_docker.sh --profile hive3
#   ./scripts/setup_docker.sh --profile hive4
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose.yml"

PROFILE="hive3"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile) PROFILE="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [[ "$PROFILE" != "hive3" && "$PROFILE" != "hive4" ]]; then
    echo "ERROR: profile must be 'hive3' or 'hive4' (got '$PROFILE')"
    exit 1
fi

PG_JDBC_VERSION="42.7.4"
PG_JDBC_JAR="postgresql-${PG_JDBC_VERSION}.jar"
PG_JDBC_DIR="${ROOT_DIR}/docker/hive/lib"
PG_JDBC_PATH="${PG_JDBC_DIR}/${PG_JDBC_JAR}"
PG_JDBC_URL="https://jdbc.postgresql.org/download/${PG_JDBC_JAR}"

LIB_DIR="${ROOT_DIR}/docker/hive/lib"
mkdir -p "$LIB_DIR"

# Download JARs if missing
download_jar() {
    local name="$1" url="$2"
    if [ ! -f "$LIB_DIR/$name" ]; then
        echo "Downloading $name..."
        curl -sL -o "$LIB_DIR/$name" "$url"
        echo "  -> $LIB_DIR/$name"
    fi
}

download_jar "$PG_JDBC_JAR" "$PG_JDBC_URL"
download_jar "hadoop-aws-3.1.0.jar" \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.0/hadoop-aws-3.1.0.jar"
download_jar "aws-java-sdk-bundle-1.11.271.jar" \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar"

echo "All JARs present."

echo "Starting Docker Compose stack with profile: $PROFILE"
docker compose -f "$COMPOSE_FILE" --profile "$PROFILE" up -d

# Determine the metastore service name for this profile
if [[ "$PROFILE" == "hive4" ]]; then
    HIVE_SERVICE="hive-metastore-v4"
else
    HIVE_SERVICE="hive-metastore"
fi

echo "Waiting for Hive metastore on port 9083..."
for i in $(seq 1 30); do
    if nc -z localhost 9083 2>/dev/null; then
        echo "Hive metastore is ready."
        exit 0
    fi
    sleep 2
done

echo "WARNING: Hive metastore did not become ready within 60s. Check: docker compose --profile $PROFILE logs $HIVE_SERVICE"
