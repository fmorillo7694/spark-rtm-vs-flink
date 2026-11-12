#!/usr/bin/env bash
# Submit a stateful Flink job to the session cluster (runs inside jobmanager), detached.
#
# Env knobs:
#   API=datastream|sql          (default datastream)
#   WORKLOAD                    datastream: s1-window|s1-update|s2|s3   sql: s1|s2
#   DELIVERY                    AT_LEAST_ONCE|EXACTLY_ONCE (mapped to the SQL option values)
#   STARTING_OFFSETS=latest|earliest
#   BUFFER_TIMEOUT_MS=0 (datastream; 100 = Flink default)
#   SECOND_TOPIC (datastream s4: reference topic; s5: payments topic, INPUT_TOPIC = orders)
#   JOIN_BOUND_S=10 (s5)
#   CHECKPOINT_MS=60000  WATERMARK_S=5 WINDOW_S=10 SESSION_GAP_MS=5000 SESSION_TIMEOUT_MS (= gap)
# The SQL job's INSERT ... await() keeps the client alive: callers run this in the background.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_INTERNAL:-kafka:9092}"
INPUT="${INPUT_TOPIC:-st-input}"
OUTPUT="${OUTPUT_TOPIC:-st-output}"
PARALLELISM="${TOPIC_PARTITIONS:-6}"
API="${API:-datastream}"
WORKLOAD="${WORKLOAD:-s1-window}"
DELIVERY="${DELIVERY:-AT_LEAST_ONCE}"
START="${STARTING_OFFSETS:-latest}"
CKPT_MS="${CHECKPOINT_MS:-60000}"
CKPT_DIR="${CHECKPOINT_DIR:-file:///tmp/flink-checkpoints/${API}-${WORKLOAD}-$(date +%s)}"
WM="${WATERMARK_S:-5}"; WIN="${WINDOW_S:-10}"; GAP="${SESSION_GAP_MS:-5000}"
TMO="${SESSION_TIMEOUT_MS:-$GAP}"
BT="${BUFFER_TIMEOUT_MS:-0}"   # 0 = per-record flush, 100 = Flink default

case "$API" in
  datastream)
    exec /opt/flink/bin/flink run --detached -c bench.StatefulDataStream \
      /workspace/flink/java-datastream/target/flink-stateless.jar \
      "$WORKLOAD" "$BOOTSTRAP" "$INPUT" "$OUTPUT" "$PARALLELISM" "$CKPT_MS" "$CKPT_DIR" \
      "$DELIVERY" "$START" "$WM" "$WIN" "$GAP" "$TMO" "$BT" "${SECOND_TOPIC:-}" "${JOIN_BOUND_S:-10}" ;;
  sql)
    case "$DELIVERY" in AT_LEAST_ONCE) D=at-least-once ;; EXACTLY_ONCE) D=exactly-once ;; *) D="$DELIVERY" ;; esac
    exec /opt/flink/bin/flink run --detached -c bench.StatefulSql \
      /workspace/flink/java-sql/target/flink-sql.jar \
      "$WORKLOAD" "$BOOTSTRAP" "$INPUT" "$OUTPUT" "$PARALLELISM" "$CKPT_MS" "$CKPT_DIR" \
      "$D" "$START" "$WM" "$WIN" "$BT" ;;
  *) echo "unknown API=$API (datastream|sql)" >&2; exit 1 ;;
esac
