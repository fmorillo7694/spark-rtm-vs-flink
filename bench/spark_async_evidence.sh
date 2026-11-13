#!/usr/bin/env bash
# Capture evidence of whether Spark async progress tracking is active for the running query.
# Run while the query is still up (before teardown).
#
#   1. jstack the driver JVM: async tracking runs a single-thread executor named
#      "async-log-write" and the query uses AsyncProgressTrackingMicroBatchExecution
#      (Spark 4.3 StreamingQueryManager.useAsyncProgressTracking picks it).
#   2. List the checkpoint dir's offsets/ and commits/ logs (batch ids + mtimes).
#
# Usage: spark_async_evidence.sh DRIVER_CONTAINER OUTFILE
set -uo pipefail
C="$1"; OUT="$2"

{
  echo "# async progress tracking evidence  container=$C  at=$(date -u +%FT%TZ)"
  PID=$(docker exec "$C" bash -c 'jps -l 2>/dev/null | awk "/SparkSubmit/{print \$1; exit}"')
  echo "driver SparkSubmit pid=$PID"
  if [ -n "$PID" ]; then
    DUMP=$(docker exec -u "$(docker exec "$C" stat -c %u /proc/"$PID")" "$C" jstack "$PID" 2>&1)
    echo "async-log-write threads: $(grep -c '"async-log-write' <<<"$DUMP")"
    echo "AsyncProgressTrackingMicroBatchExecution frames: $(grep -c 'AsyncProgressTrackingMicroBatchExecution' <<<"$DUMP")"
    echo "plain MicroBatchExecution frames: $(grep -c 'runtime.MicroBatchExecution\.' <<<"$DUMP")"
    echo "--- thread names matching async|stream execution"
    grep -E '^"(async-log-write|stream execution thread)' <<<"$DUMP" | cut -c1-160
  fi
  CK=$(docker exec "$C" bash -c 'ls -td /tmp/spark-checkpoints/*/ 2>/dev/null | head -1')
  echo "--- checkpoint dir: $CK"
  for d in offsets commits; do
    echo "[$d]"
    docker exec "$C" bash -c "cd '$CK/$d' 2>/dev/null && ls -l --time-style=+%H:%M:%S | grep -v '^total'"
  done
  echo "--- latest commit log entry"
  docker exec "$C" bash -c "cd '$CK/commits' 2>/dev/null && cat \"\$(ls | sort -n | tail -1)\""
  echo
} > "$OUT" 2>&1
sed -n '1,6p' "$OUT"
