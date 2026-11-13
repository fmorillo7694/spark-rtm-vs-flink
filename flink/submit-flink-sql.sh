#!/usr/bin/env bash
# Submit the Flink SQL (Table API) pipeline to the session cluster (runs inside jobmanager).
# Schema-based json format + keyed-by-user_id output — the JSON-path-comparable variant.
# Detached so the caller can drive the producer/consumer; returns the JobID.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_INTERNAL:-kafka:9092}"
INPUT="${INPUT_TOPIC:-input-events}"
OUTPUT="${OUTPUT_TOPIC:-output-events}"
PARALLELISM="${TOPIC_PARTITIONS:-3}"
CHECKPOINT_MS="${CHECKPOINT_MS:-0}"
JAR=/workspace/flink/java-sql/target/flink-sql.jar

exec /opt/flink/bin/flink run --detached \
  -c bench.SqlPipeline \
  "$JAR" \
  "$BOOTSTRAP" "$INPUT" "$OUTPUT" "$PARALLELISM" "$CHECKPOINT_MS"
