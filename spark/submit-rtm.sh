#!/usr/bin/env bash
# Submit the RTM pipeline to the standalone cluster (runs inside spark-master).
# Real-time Mode: Trigger.RealTime, outputMode=update, checkpoint required, AQE off.
#
# Env knobs:
#   RTM_LANG=scala|java          which compiled pipeline to run (default scala)
#   ASYNC_PROGRESS=auto|false    auto = Spark 4.3 default (async progress tracking is ON for
#                                stateless RTM); false = control arm, writer option
#                                asyncProgressTrackingEnabled=false (synchronous commits)
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_INTERNAL:-kafka:9092}"
INPUT="${INPUT_TOPIC:-input-events}"
OUTPUT="${OUTPUT_TOPIC:-output-events}"
TRIGGER="${RTM_TRIGGER:-5 seconds}"
SCALA_BIN="${SPARK_SCALA_BINARY:-2.13}"
SPARK_VER="${SPARK_VERSION:-4.3.0}"
RTM_LANG="${RTM_LANG:-scala}"
ASYNC_PROGRESS="${ASYNC_PROGRESS:-auto}"
CKPT=/tmp/spark-checkpoints/rtm-${RTM_LANG}-${ASYNC_PROGRESS}-$(date +%s)

case "$RTM_LANG" in
  scala) JAR=/workspace/spark/scala-rtm/target/scala-${SCALA_BIN}/scala-rtm_${SCALA_BIN}-0.1.0.jar; CLASS=RtmPipeline ;;
  java)  JAR=/workspace/spark/java-rtm/target/java-rtm.jar; CLASS=bench.RtmPipelineJava ;;
  *) echo "unknown RTM_LANG=$RTM_LANG (scala|java)" >&2; exit 1 ;;
esac

export HOME=/opt/spark

exec /opt/spark/bin/spark-submit \
  --master "spark://spark-master:7077" \
  --deploy-mode client \
  --name "rtm-bench-${RTM_LANG}-${ASYNC_PROGRESS}" \
  --packages "org.apache.spark:spark-sql-kafka-0-10_${SCALA_BIN}:${SPARK_VER}" \
  --conf spark.jars.ivy=/opt/spark/.ivy2 \
  --conf spark.cores.max=6 \
  --conf spark.executor.cores=6 \
  --conf spark.executor.memory=2560m \
  --conf spark.executor.memoryOverhead=512m \
  --conf spark.sql.adaptive.enabled=false \
  --class "$CLASS" \
  "$JAR" \
  rtm "$BOOTSTRAP" "$INPUT" "$OUTPUT" "$CKPT" "$TRIGGER" "$ASYNC_PROGRESS"
