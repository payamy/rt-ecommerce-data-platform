#!/usr/bin/env bash
set -euo pipefail

export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS:-} ${SERVICE_OPTS:-}"
# Hive 3's stock entrypoint initializes unconditionally. Check the persisted
# schema first so restarting the container does not try to create it again.
if ! /opt/hive/bin/schematool -dbType postgres -info; then
    /opt/hive/bin/schematool -dbType postgres -initSchema
fi
exec /opt/hive/bin/hive --service metastore -p 9083
