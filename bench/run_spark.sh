#!/usr/bin/env bash
# Full Spark benchmark run: Kafka + Spark up -> build -> submit (RTM or micro-batch) ->
# produce + consume + sample docker stats -> tear Spark down. Kafka is left up (shared).
#
# Usage: bench/run_spark.sh [rtm|microbatch]   (default rtm)
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a

MODE="${1:-rtm}"
ENGINE="spark-${MODE}"
PY=.venv/bin/python
mkdir -p results

echo "==> [1/7] Kafka up"
docker compose -f docker-compose.kafka.yml up -d >/dev/null
# wait for healthy
until [ "$(docker inspect -f '{{.State.Health.Status}}' kafka 2>/dev/null)" = "healthy" ]; do sleep 2; done

echo "==> [2/7] Build Scala jar"
( cd spark/scala-rtm && sbt -batch package >/tmp/sbt_build.log 2>&1 ) && echo "    jar OK"

echo "==> [3/7] Spark cluster up"
docker compose -f docker-compose.spark.yml up -d >/dev/null
until [ "$(docker inspect -f '{{.State.Health.Status}}' spark-master 2>/dev/null)" = "healthy" ]; do sleep 2; done
# writable dirs for ivy + checkpoints on every node (survives recreation)
for c in spark-master spark-worker-1; do
  docker exec -u root "$c" bash -c 'mkdir -p /opt/spark/.ivy2 /tmp/spark-checkpoints && chmod -R 777 /opt/spark/.ivy2 /tmp/spark-checkpoints'
done
# wait for the worker (6 cores) to register
until [ "$(curl -s http://localhost:8080/json/ | $PY -c 'import sys,json;print(len(json.load(sys.stdin)["workers"]))' 2>/dev/null)" = "1" ]; do sleep 2; done
echo "    1 worker registered (6 cores)"

echo "==> [4/7] Submit $MODE job"
docker exec -e KAFKA_BOOTSTRAP_INTERNAL="$KAFKA_BOOTSTRAP_INTERNAL" -e INPUT_TOPIC="$INPUT_TOPIC" \
  -e OUTPUT_TOPIC="$OUTPUT_TOPIC" -e RTM_TRIGGER="$RTM_TRIGGER" -e SPARK_SCALA_BINARY="$SPARK_SCALA_BINARY" \
  -e SPARK_VERSION="$SPARK_VERSION" \
  spark-master bash "/workspace/spark/submit-${MODE}.sh" >/tmp/${ENGINE}_submit.log 2>&1 &
echo "    waiting 40s for query to spin up..."; sleep 40
if ! grep -q 'RtmPipeline]' /tmp/${ENGINE}_submit.log; then
  echo "!! query did not start; tail:"; tail -15 /tmp/${ENGINE}_submit.log; exit 1
fi

echo "==> [5/7] Sample docker stats"
bash bench/collect_stats.sh "results/${ENGINE}_stats.csv" kafka spark-master spark-worker-1 &
STATS_PID=$!

echo "==> [6/7] Produce ${RUN_SECONDS}s @ ${TARGET_RATE}/s + measure latency"
$PY common/latency_consumer.py --engine "$ENGINE" --warmup "$WARMUP_SECONDS" --seconds "$RUN_SECONDS" >/tmp/${ENGINE}_cons.log 2>&1 &
CONS=$!
sleep 2
$PY common/producer.py --rate "$TARGET_RATE" --seconds "$RUN_SECONDS" >/tmp/${ENGINE}_prod.log 2>&1
wait $CONS
kill $STATS_PID 2>/dev/null || true

echo "==> [7/7] Tear down Spark (Kafka stays up)"
docker exec spark-master bash -c "pkill -f RtmPipeline; pkill -f spark-submit" 2>/dev/null || true
docker compose -f docker-compose.spark.yml down >/dev/null 2>&1

echo "==> DONE. Latency summary:"
sed -n '/LATENCY SUMMARY/,$p' /tmp/${ENGINE}_cons.log | head -32
