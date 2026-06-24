#!/usr/bin/env bash
# Submit the Java DataStream pipeline to the Flink session cluster (runs inside jobmanager).
# Detached so the caller can drive the producer/consumer; returns the JobID.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_INTERNAL:-kafka:9092}"
INPUT="${INPUT_TOPIC:-input-events}"
OUTPUT="${OUTPUT_TOPIC:-output-events}"
PARALLELISM="${TOPIC_PARTITIONS:-3}"
JAR=/workspace/flink/java-datastream/target/flink-stateless.jar

# CHECKPOINT_MS (default 0 = off, for the latency benchmark). Set >0 for fault tolerance.
CHECKPOINT_MS="${CHECKPOINT_MS:-0}"

exec /opt/flink/bin/flink run --detached \
  -c bench.StatelessPipeline \
  "$JAR" \
  "$BOOTSTRAP" "$INPUT" "$OUTPUT" "$PARALLELISM" "$CHECKPOINT_MS"
