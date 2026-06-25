#!/usr/bin/env bash
# Chaos test for Flink (Java or PyFlink): start the job, begin load, kill a taskmanager
# mid-stream, and measure recovery downtime + duplicates from the consumer CSV.
#
# Usage: bench/chaos_flink.sh <java|py>
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a
PY=.venv/bin/python
MODE="${1:-java}"
[ "$MODE" = py ] && COMPOSE=docker-compose.pyflink.yml ENGINE=pyflink-chaos || COMPOSE=docker-compose.flink.yml ENGINE=flink-chaos
mkdir -p results

echo "==> Kafka + Flink ($MODE) up"
docker compose -f docker-compose.kafka.yml up -d >/dev/null
until [ "$(docker inspect -f '{{.State.Health.Status}}' kafka 2>/dev/null)" = healthy ]; do sleep 2; done
docker compose -f $COMPOSE up -d >/dev/null
until [ "$(docker inspect -f '{{.State.Health.Status}}' flink-jobmanager 2>/dev/null)" = healthy ]; do sleep 2; done
until [ "$(curl -s http://localhost:8081/overview | $PY -c 'import sys,json;print(json.load(sys.stdin)["slots-total"])' 2>/dev/null)" -ge 3 ] 2>/dev/null; do sleep 2; done

echo "==> Submit job"
if [ "$MODE" = py ]; then
  docker exec -e KAFKA_BOOTSTRAP_INTERNAL=$KAFKA_BOOTSTRAP_INTERNAL -e INPUT_TOPIC=$INPUT_TOPIC -e OUTPUT_TOPIC=$OUTPUT_TOPIC -e TOPIC_PARTITIONS=$TOPIC_PARTITIONS flink-jobmanager bash /workspace/flink/submit-pyflink.sh >/tmp/chaos_submit.log 2>&1
else
  docker exec -e KAFKA_BOOTSTRAP_INTERNAL=$KAFKA_BOOTSTRAP_INTERNAL -e INPUT_TOPIC=$INPUT_TOPIC -e OUTPUT_TOPIC=$OUTPUT_TOPIC -e TOPIC_PARTITIONS=$TOPIC_PARTITIONS -e CHECKPOINT_MS=10000 flink-jobmanager bash /workspace/flink/submit-flink.sh >/tmp/chaos_submit.log 2>&1
fi
JOBID=$(grep -oE 'JobID [a-f0-9]+' /tmp/chaos_submit.log | awk '{print $2}'); echo "   JobID=$JOBID"
sleep 12

echo "==> Start consumer + producer (90s), kill a taskmanager at ~40s"
$PY common/latency_consumer.py --engine "$ENGINE" --warmup 0 --seconds 90 >/tmp/chaos_cons.log 2>&1 &
CONS=$!; sleep 2
$PY common/producer.py --rate 10000 --seconds 85 >/tmp/chaos_prod.log 2>&1 &
PROD=$!
sleep 40
echo "   !!! KILLING flink-taskmanager at $(date +%H:%M:%S)"
docker kill flink-taskmanager >/dev/null 2>&1 || true
# Flink restart strategy will reschedule; bring the TM back so slots are restored
sleep 5; docker compose -f $COMPOSE up -d taskmanager >/dev/null 2>&1 || true
wait $PROD; wait $CONS

echo "==> Recovery analysis"
$PY bench/analyze_chaos.py "$ENGINE"
echo "==> Job state after chaos:"; curl -s http://localhost:8081/jobs/overview | $PY -c 'import sys,json;[print(" ",j["name"][:30],j["state"]) for j in json.load(sys.stdin)["jobs"]]' 2>/dev/null || true
[ -n "$JOBID" ] && docker exec flink-jobmanager /opt/flink/bin/flink cancel "$JOBID" >/dev/null 2>&1 || true
docker compose -f $COMPOSE down >/dev/null 2>&1
