#!/usr/bin/env bash
# RIGOROUS Spark RTM chaos test. Fixes the flaw in the first attempt:
#   - cluster has EXACTLY 3 cores total (1 worker x 3 cores) so all 3 partition-reader
#     tasks MUST run on the one executor we kill -> a real, unavoidable disruption.
#   - persistent driver log (not overwritten), captured across the kill.
#   - records executor list + throughput before/during/after.
# This forces RTM to actually lose the data path, not an idle spare executor.
#
# Usage: bench/chaos_spark_rigorous.sh
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a
PY=.venv/bin/python
ENGINE=spark-rtm-chaos-rigorous
DRIVERLOG=/tmp/${ENGINE}_driver.log
mkdir -p results

echo "==> Kafka + Spark up (single worker, 3 cores = no spare capacity)"
docker compose -f docker-compose.kafka.yml up -d >/dev/null
until [ "$(docker inspect -f '{{.State.Health.Status}}' kafka 2>/dev/null)" = healthy ]; do sleep 2; done
docker compose -f docker-compose.spark.yml up -d spark-master spark-worker-1 >/dev/null
until [ "$(docker inspect -f '{{.State.Health.Status}}' spark-master 2>/dev/null)" = healthy ]; do sleep 2; done
for c in spark-master spark-worker-1; do
  docker exec -u root "$c" bash -c 'mkdir -p /opt/spark/.ivy2 /tmp/spark-checkpoints && chmod -R 777 /opt/spark/.ivy2 /tmp/spark-checkpoints' 2>/dev/null || true
done
until [ "$(curl -s http://localhost:8080/json/ | $PY -c 'import sys,json;print(len(json.load(sys.stdin)["workers"]))' 2>/dev/null)" -ge 1 ]; do sleep 2; done

CKPT=/tmp/spark-checkpoints/chaos-$(date +%s)
echo "==> Submit Java RTM with checkpoint=$CKPT (3 cores, all on worker-1)"
docker exec -e HOME=/opt/spark spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2 --conf spark.jars.ivy=/opt/spark/.ivy2 \
  --conf spark.cores.max=3 --conf spark.executor.cores=3 --conf spark.task.maxFailures=10 \
  --class bench.RtmPipelineJava /workspace/spark/java-rtm/target/java-rtm.jar \
  rtm $KAFKA_BOOTSTRAP_INTERNAL $INPUT_TOPIC $OUTPUT_TOPIC "$CKPT" "5 seconds" >"$DRIVERLOG" 2>&1 &
echo "   waiting 45s for query"; sleep 45
grep -qE 'RtmPipeline' "$DRIVERLOG" && echo "   query up" || { echo "   FAILED to start"; tail -10 "$DRIVERLOG"; exit 1; }

echo "==> executors BEFORE kill:"
curl -s http://localhost:8080/json/ | $PY -c 'import sys,json;d=json.load(sys.stdin);print("   workers:",len(d["workers"]),"coresused:",d["coresused"])'

echo "==> consumer + producer (100s); kill worker-1 at ~45s, restart it at ~50s"
$PY common/latency_consumer.py --engine "$ENGINE" --warmup 0 --seconds 100 >/tmp/chaos_cons_rig.log 2>&1 &
CONS=$!; sleep 2
$PY common/producer.py --rate 10000 --seconds 95 >/tmp/chaos_prod_rig.log 2>&1 &
PROD=$!
sleep 45
echo "   !!! KILL spark-worker-1 (hosts ALL 3 reader tasks) at $(date +%H:%M:%S)"
docker kill spark-worker-1 >/dev/null 2>&1 || true
sleep 5
echo "   restart worker-1 at $(date +%H:%M:%S)"
docker compose -f docker-compose.spark.yml up -d spark-worker-1 >/dev/null 2>&1 || true
docker exec -u root spark-worker-1 bash -c 'chmod -R 777 /opt/spark/.ivy2 /tmp/spark-checkpoints' 2>/dev/null || true
wait $PROD; wait $CONS

echo "==> Recovery analysis"
$PY bench/analyze_chaos.py "$ENGINE"
echo "==> Did the QUERY survive, or did spark-submit exit (= query failed)?"
if kill -0 $(pgrep -f 'spark-submit.*RtmPipelineJava' | head -1) 2>/dev/null; then echo "   spark-submit STILL RUNNING -> query survived"; else echo "   spark-submit EXITED -> query FAILED/terminated"; fi
echo "==> driver log around the kill:"
grep -iE 'Lost executor|StreamingQueryException|Query .* terminated|reschedul|RUNNING|ERROR|Aborting|restarted|FAILED' "$DRIVERLOG" | tail -20

docker exec spark-master bash -c "pkill -f RtmPipelineJava; pkill -f spark-submit" 2>/dev/null || true
docker compose -f docker-compose.spark.yml down >/dev/null 2>&1
