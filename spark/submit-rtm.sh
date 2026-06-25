#!/usr/bin/env bash
# Submit the Scala RTM pipeline to the standalone cluster (runs inside spark-master).
# Real-time Mode: Trigger.RealTime, outputMode=update, checkpoint required, AQE off.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_INTERNAL:-kafka:9092}"
INPUT="${INPUT_TOPIC:-input-events}"
OUTPUT="${OUTPUT_TOPIC:-output-events}"
TRIGGER="${RTM_TRIGGER:-5 seconds}"
SCALA_BIN="${SPARK_SCALA_BINARY:-2.13}"
SPARK_VER="${SPARK_VERSION:-4.1.2}"
JAR=/workspace/spark/scala-rtm/target/scala-${SCALA_BIN}/scala-rtm_${SCALA_BIN}-0.1.0.jar
CKPT=/tmp/spark-checkpoints/rtm-$(date +%s)

export HOME=/opt/spark

exec /opt/spark/bin/spark-submit \
  --master "spark://spark-master:7077" \
  --deploy-mode client \
  --name rtm-bench \
  --packages "org.apache.spark:spark-sql-kafka-0-10_${SCALA_BIN}:${SPARK_VER}" \
  --conf spark.jars.ivy=/opt/spark/.ivy2 \
  --conf spark.cores.max=6 \
  --conf spark.executor.cores=6 \
  --conf spark.executor.memory=2560m \
  --conf spark.executor.memoryOverhead=512m \
  --conf spark.sql.adaptive.enabled=false \
  --class RtmPipeline \
  "$JAR" \
  rtm "$BOOTSTRAP" "$INPUT" "$OUTPUT" "$CKPT" "$TRIGGER"
