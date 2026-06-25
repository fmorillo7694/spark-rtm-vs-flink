#!/usr/bin/env bash
# Full PyFlink benchmark run (Python-Flink corner of the 2x2). Uses the pyflink-bench
# image (Flink 2.2 + python + apache-flink) so the Table API UDF runs on the taskmanager.
#
# Usage: bench/run_pyflink.sh
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a

ENGINE="pyflink"
PY=.venv/bin/python
mkdir -p results

echo "==> [1/6] Kafka up"
docker compose -f docker-compose.kafka.yml up -d >/dev/null
until [ "$(docker inspect -f '{{.State.Health.Status}}' kafka 2>/dev/null)" = "healthy" ]; do sleep 2; done

echo "==> [2/6] PyFlink cluster up (image: pyflink-bench:2.2.0)"
docker image inspect pyflink-bench:2.2.0 >/dev/null 2>&1 || { echo "!! build first: docker build -t pyflink-bench:2.2.0 flink/pyflink/"; exit 1; }
docker compose -f docker-compose.pyflink.yml up -d >/dev/null
until [ "$(docker inspect -f '{{.State.Health.Status}}' flink-jobmanager 2>/dev/null)" = "healthy" ]; do sleep 2; done
until [ "$(curl -s http://localhost:8081/overview | $PY -c 'import sys,json;print(json.load(sys.stdin)["slots-total"])' 2>/dev/null)" -ge 6 ] 2>/dev/null; do sleep 2; done
echo "    taskmanager slots ready"

echo "==> [3/6] Submit PyFlink job"
# The Table API INSERT ... .wait() keeps the `flink run --python` client alive, so run the
# submit in the BACKGROUND and discover the JobID + RUNNING state from the REST API.
docker exec -e KAFKA_BOOTSTRAP_INTERNAL="$KAFKA_BOOTSTRAP_INTERNAL" -e INPUT_TOPIC="$INPUT_TOPIC" \
  -e OUTPUT_TOPIC="$OUTPUT_TOPIC" -e TOPIC_PARTITIONS="$TOPIC_PARTITIONS" \
  -e CHECKPOINT_MS="${FLINK_CHECKPOINT_MS:-0}" \
  flink-jobmanager bash /workspace/flink/submit-pyflink.sh >/tmp/pyflink_submit.log 2>&1 &
SUBMIT_PID=$!
STATE=UNKNOWN
for i in $(seq 1 40); do
  STATE=$(curl -s http://localhost:8081/jobs/overview | $PY -c 'import sys,json;j=json.load(sys.stdin)["jobs"];print(j[0]["state"] if j else "NONE")' 2>/dev/null || echo UNKNOWN)
  JOBID=$(curl -s http://localhost:8081/jobs/overview | $PY -c 'import sys,json;j=json.load(sys.stdin)["jobs"];print(j[0]["jid"] if j else "")' 2>/dev/null || echo "")
  [ "$STATE" = "RUNNING" ] && break
  sleep 2
done
echo "    JobID=$JOBID state=$STATE"
[ "$STATE" = "RUNNING" ] || { echo "!! job not RUNNING ($STATE)"; tail -20 /tmp/pyflink_submit.log; exit 1; }

echo "==> [4/6] Sample docker stats"
bash bench/collect_stats.sh "results/${ENGINE}_stats.csv" kafka flink-jobmanager flink-taskmanager &
STATS_PID=$!

echo "==> [5/6] Produce ${RUN_SECONDS}s @ ${TARGET_RATE}/s + measure latency"
$PY common/latency_consumer.py --engine "$ENGINE" --warmup "$WARMUP_SECONDS" --seconds "$RUN_SECONDS" >/tmp/${ENGINE}_cons.log 2>&1 &
CONS=$!
sleep 2
$PY common/producer.py --rate "$TARGET_RATE" --seconds "$RUN_SECONDS" >/tmp/${ENGINE}_prod.log 2>&1
wait $CONS
kill $STATS_PID 2>/dev/null || true

echo "==> [6/6] Cancel job + tear down (Kafka stays up)"
[ -n "$JOBID" ] && docker exec flink-jobmanager /opt/flink/bin/flink cancel "$JOBID" >/dev/null 2>&1 || true
kill "$SUBMIT_PID" 2>/dev/null || true   # the backgrounded blocking submit client
docker compose -f docker-compose.pyflink.yml down >/dev/null 2>&1

echo "==> DONE. Latency summary:"
sed -n '/LATENCY SUMMARY/,$p' /tmp/${ENGINE}_cons.log | head -32
