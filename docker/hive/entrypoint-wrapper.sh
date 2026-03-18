#!/bin/bash
# Wrapper entrypoint that safely handles schema init on both fresh and existing databases.
set -e

export HIVE_CONF_DIR=$HIVE_HOME/conf
export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Xmx1G $SERVICE_OPTS"

# Check if schema already exists; init only if needed
if $HIVE_HOME/bin/schematool -dbType "$DB_DRIVER" -info >/dev/null 2>&1; then
    echo "Hive metastore schema already exists, skipping init."
else
    echo "Initializing Hive metastore schema..."
    $HIVE_HOME/bin/schematool -dbType "$DB_DRIVER" -initSchema
    echo "Schema initialized."
fi

export METASTORE_PORT=${METASTORE_PORT:-9083}
exec $HIVE_HOME/bin/hive --skiphadoopversion --skiphbasecp --service metastore
