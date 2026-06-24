#!/usr/bin/env bash
# Submit the PyFlink Table API pipeline to the session cluster (runs inside jobmanager).
# Detached so the caller can drive the producer/consumer.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_INTERNAL:-kafka:9092}"
INPUT="${INPUT_TOPIC:-input-events}"
OUTPUT="${OUTPUT_TOPIC:-output-events}"
PARALLELISM="${TOPIC_PARTITIONS:-3}"
PYJOB=/workspace/flink/pyflink/stateless_pipeline.py

exec /opt/flink/bin/flink run --detached \
  --python "$PYJOB" \
  "$BOOTSTRAP" "$INPUT" "$OUTPUT" "$PARALLELISM"
