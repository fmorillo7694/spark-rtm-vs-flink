#!/usr/bin/env bash
# Submit the stateful Java pipeline (bench.StatefulPipelineJava) to the standalone cluster
# (runs inside spark-master). Every state-store setting is explicit so the run records what
# it used (RTM would apply the first three as soft defaults anyway); see STATEFUL_CONFIG.md.
#
# Env knobs:
#   WORKLOAD=s1|s2|s3          (default s1)
#   MODE=rtm|microbatch        (default rtm)
#   STARTING_OFFSETS=latest|earliest   (earliest = correctness replay)
#   RTM_TRIGGER="60 seconds"   RTM batch duration = checkpoint cadence
#   WATERMARK_S=5 WINDOW_S=10 SESSION_GAP_MS=5000 SESSION_TIMEOUT_MS (default = gap)
#   SECOND_TOPIC               s4: reference topic, s5: payments topic (INPUT_TOPIC = orders)
#   MB_TRIGGER                 micro-batch trigger, e.g. "1 second" ("" = Spark default)
#   JOIN_BOUND_S=10            s5 time bound
#   SHUFFLE_MAX_WAIT_MS        spark.shuffle.streaming.networkBufferMaxWaitTimeMs (internal conf,
#                              Spark default 50 ms: how long a partly-filled streaming-shuffle
#                              buffer waits before flushing; the analogue of Flink's
#                              execution.buffer-timeout). Unset = Spark default.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_INTERNAL:-kafka:9092}"
INPUT="${INPUT_TOPIC:-st-input}"
OUTPUT="${OUTPUT_TOPIC:-st-output}"
WORKLOAD="${WORKLOAD:-s1}"
MODE="${MODE:-rtm}"
TRIGGER="${RTM_TRIGGER:-60 seconds}"
START="${STARTING_OFFSETS:-latest}"
SCALA_BIN="${SPARK_SCALA_BINARY:-2.13}"
SPARK_VER="${SPARK_VERSION:-4.3.0}"
SLOTS="${SPARK_SLOTS:-12}"
CKPT=/tmp/spark-checkpoints/stateful-${WORKLOAD}-${MODE}-$(date +%s)
JAR=/workspace/spark/java-rtm/target/java-rtm.jar
EXTRA=()
[ -n "${SHUFFLE_MAX_WAIT_MS:-}" ] && EXTRA+=(--conf "spark.shuffle.streaming.networkBufferMaxWaitTimeMs=${SHUFFLE_MAX_WAIT_MS}")
# EXTRA_CONFS: space-separated key=value Spark confs (e.g. the dynamic-allocation smoke in SCALING_PLAN.md)
for kv in ${EXTRA_CONFS:-}; do EXTRA+=(--conf "$kv"); done

export HOME=/opt/spark

exec /opt/spark/bin/spark-submit \
  --master "spark://spark-master:7077" \
  --deploy-mode client \
  --name "stateful-${WORKLOAD}-${MODE}" \
  --packages "org.apache.spark:spark-sql-kafka-0-10_${SCALA_BIN}:${SPARK_VER}" \
  --conf spark.jars.ivy=/opt/spark/.ivy2 \
  --conf spark.cores.max="$SLOTS" \
  --conf spark.executor.cores="$SLOTS" \
  --conf spark.executor.memory=2560m \
  --conf spark.executor.memoryOverhead=512m \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.sql.shuffle.partitions=6 \
  --conf spark.sql.streaming.stateStore.providerClass=org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider \
  --conf spark.sql.streaming.stateStore.checkpointFormatVersion=2 \
  --conf spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled=true \
  ${EXTRA[@]+"${EXTRA[@]}"} \
  --class bench.StatefulPipelineJava \
  "$JAR" \
  "$WORKLOAD" "$MODE" "$BOOTSTRAP" "$INPUT" "$OUTPUT" "$CKPT" "$TRIGGER" "$START" \
  "${WATERMARK_S:-5}" "${WINDOW_S:-10}" "${SESSION_GAP_MS:-5000}" \
  "${SESSION_TIMEOUT_MS:-${SESSION_GAP_MS:-5000}}" \
  "${SECOND_TOPIC:-}" "${MB_TRIGGER:-}" "${JOIN_BOUND_S:-10}"
