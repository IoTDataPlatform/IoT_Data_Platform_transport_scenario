#!/bin/bash
set -e

echo "[flink-start] Starting Flink JobManager..."
/docker-entrypoint.sh jobmanager &
JM_PID=$!

echo "[flink-start] Waiting for JobManager RPC on localhost:6123..."
for i in {1..60}; do
  if (echo > /dev/tcp/localhost/6123) 2>/dev/null; then
    echo "[flink-start] JobManager RPC is up."
    break
  fi
  echo "[flink-start] JM not ready yet, retry $i..."
  sleep 2
done

# 1) positions_enricher
echo "[flink-start] Submitting SQL job from /opt/flink/jobs/positions_enricher.sql..."
/opt/flink/bin/sql-client.sh \
  -Dexecution.target=remote \
  -Dexecution.remote.host=localhost \
  -Dexecution.remote.port=6123 \
  -Dexecution.attached=false \
  -f /opt/flink/jobs/positions_enricher.sql

# 2) arrivals_delay
echo "[flink-start] Submitting SQL job from /opt/flink/jobs/arrivals_delay.sql..."
/opt/flink/bin/sql-client.sh \
  -Dexecution.target=remote \
  -Dexecution.remote.host=localhost \
  -Dexecution.remote.port=6123 \
  -Dexecution.attached=false \
  -f /opt/flink/jobs/arrivals_delay.sql

echo "[flink-start] SQL jobs submitted. Waiting for JobManager to exit..."
wait "$JM_PID"
