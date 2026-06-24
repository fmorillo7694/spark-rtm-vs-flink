#!/usr/bin/env bash
# Full measured PySpark RTM run (Python-Spark corner of the 2x2). Driver = jupyter
# container (needs python3 installed); executors run no Python (out_ts via reflect()).
#
# Usage: bench/run_pyspark.sh
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a

ENGINE="pyspark-rtm"
PY=.venv/bin/python
mkdir -p results

echo "==> [1/6] Kafka up"
docker compose -f docker-compose.kafka.yml up -d >/dev/null
until [ "$(docker inspect -f '{{.State.Health.Status}}' kafka 2>/dev/null)" = "healthy" ]; do sleep 2; done

echo "==> [2/6] Spark cluster up"
docker compose -f docker-compose.spark.yml up -d >/dev/null
until [ "$(docker inspect -f '{{.State.Health.Status}}' spark-master 2>/dev/null)" = "healthy" ]; do sleep 2; done
for c in spark-master spark-worker-1 spark-worker-2 spark-jupyter; do
  docker exec -u root "$c" bash -c 'mkdir -p /opt/spark/.ivy2 /tmp/spark-checkpoints && chmod -R 777 /opt/spark/.ivy2 /tmp/spark-checkpoints'
done
until [ "$(curl -s http://localhost:8080/json/ | $PY -c 'import sys,json;print(len(json.load(sys.stdin)["workers"]))' 2>/dev/null)" = "2" ]; do sleep 2; done
echo "    2 workers registered"

echo "==> [3/6] Install python3 on driver (jupyter)"
docker exec -u root spark-jupyter bash -c 'command -v python3 >/dev/null || (apt-get update -qq >/dev/null 2>&1 && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq python3 >/dev/null 2>&1); ln -sf /usr/bin/python3 /usr/local/bin/python3; python3 --version'

echo "==> [4/6] Submit measured PySpark RTM job"
docker exec -e KAFKA_BOOTSTRAP_INTERNAL="$KAFKA_BOOTSTRAP_INTERNAL" -e INPUT_TOPIC="$INPUT_TOPIC" \
  -e OUTPUT_TOPIC="$OUTPUT_TOPIC" -e RTM_TRIGGER="$RTM_TRIGGER" -e SPARK_MASTER_URL="spark://spark-master:7077" \
  -e HOME=/opt/spark -e PYSPARK_PYTHON=python3 -e PYSPARK_DRIVER_PYTHON=python3 \
  spark-jupyter /opt/spark/bin/spark-submit \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2" \
  --conf spark.jars.ivy=/opt/spark/.ivy2 \
  /workspace/spark/pyspark-bridge/rtm_pyspark_measured.py >/tmp/${ENGINE}_submit.log 2>&1 &
echo "    waiting 45s for query to spin up..."; sleep 45
if ! grep -q 'STARTED RTM' /tmp/${ENGINE}_submit.log; then
  echo "!! query did not start; tail:"; tail -20 /tmp/${ENGINE}_submit.log; exit 1
fi

echo "==> [5/6] Sample stats + produce ${RUN_SECONDS}s @ ${TARGET_RATE}/s + measure latency"
bash bench/collect_stats.sh "results/${ENGINE}_stats.csv" kafka spark-master spark-worker-1 spark-worker-2 spark-jupyter &
STATS_PID=$!
$PY common/latency_consumer.py --engine "$ENGINE" --warmup "$WARMUP_SECONDS" --seconds "$RUN_SECONDS" >/tmp/${ENGINE}_cons.log 2>&1 &
CONS=$!
sleep 2
$PY common/producer.py --rate "$TARGET_RATE" --seconds "$RUN_SECONDS" >/tmp/${ENGINE}_prod.log 2>&1
wait $CONS
kill $STATS_PID 2>/dev/null || true

echo "==> [6/6] Tear down Spark (Kafka stays up)"
docker exec spark-jupyter bash -c "pkill -f rtm_pyspark_measured; pkill -f spark-submit" 2>/dev/null || true
docker compose -f docker-compose.spark.yml down >/dev/null 2>&1

echo "==> DONE. Latency summary:"
sed -n '/LATENCY SUMMARY/,$p' /tmp/${ENGINE}_cons.log | head -32
