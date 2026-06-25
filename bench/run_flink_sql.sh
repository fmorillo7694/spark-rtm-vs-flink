#!/usr/bin/env bash
# Full Flink SQL benchmark run: Kafka + Flink up -> build -> submit the Table API/SQL job
# (schema-based json format, keyed by user_id) -> produce + consume + sample docker stats
# -> cancel job -> tear Flink down. Kafka is left up (shared). This is the JSON-path-
# comparable Flink variant (the DataStream/Jackson job is bench/run_flink.sh).
#
# Usage: bench/run_flink_sql.sh
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a

ENGINE="flink-sql"
PY=.venv/bin/python
mkdir -p results

echo "==> [1/7] Kafka up"
docker compose -f docker-compose.kafka.yml up -d >/dev/null
until [ "$(docker inspect -f '{{.State.Health.Status}}' kafka 2>/dev/null)" = "healthy" ]; do sleep 2; done

echo "==> [2/7] Build Flink SQL jar"
( cd flink/java-sql && mvn -q -B package >/tmp/mvn_sql_build.log 2>&1 ) && echo "    jar OK"

echo "==> [3/7] Flink cluster up"
docker compose -f docker-compose.flink.yml up -d >/dev/null
until [ "$(docker inspect -f '{{.State.Health.Status}}' flink-jobmanager 2>/dev/null)" = "healthy" ]; do sleep 2; done
until [ "$(curl -s http://localhost:8081/overview | $PY -c 'import sys,json;print(json.load(sys.stdin)["slots-total"])' 2>/dev/null)" -ge 6 ] 2>/dev/null; do sleep 2; done
echo "    taskmanager slots ready"

echo "==> [4/7] Submit job"
# The Table API INSERT ... .await() keeps the `flink run` client alive, so run the submit
# in the BACKGROUND and discover the JobID from the REST API (unlike the DataStream job,
# whose env.execute() returns immediately under --detached).
docker exec -e KAFKA_BOOTSTRAP_INTERNAL="$KAFKA_BOOTSTRAP_INTERNAL" -e INPUT_TOPIC="$INPUT_TOPIC" \
  -e OUTPUT_TOPIC="$OUTPUT_TOPIC" -e TOPIC_PARTITIONS="$TOPIC_PARTITIONS" \
  -e CHECKPOINT_MS="${FLINK_CHECKPOINT_MS:-0}" \
  flink-jobmanager bash /workspace/flink/submit-flink-sql.sh >/tmp/flink_sql_submit.log 2>&1 &
SUBMIT_PID=$!
# Poll the REST API until the job shows RUNNING (up to ~60s).
STATE=UNKNOWN
for i in $(seq 1 30); do
  STATE=$(curl -s http://localhost:8081/jobs/overview | $PY -c 'import sys,json;j=json.load(sys.stdin)["jobs"];print(j[0]["state"] if j else "NONE")' 2>/dev/null || echo UNKNOWN)
  JOBID=$(curl -s http://localhost:8081/jobs/overview | $PY -c 'import sys,json;j=json.load(sys.stdin)["jobs"];print(j[0]["jid"] if j else "")' 2>/dev/null || echo "")
  [ "$STATE" = "RUNNING" ] && break
  sleep 2
done
echo "    JobID=$JOBID state=$STATE"
[ "$STATE" = "RUNNING" ] || { echo "!! job not RUNNING ($STATE)"; tail -15 /tmp/flink_sql_submit.log; exit 1; }

echo "==> [5/7] Sample docker stats"
bash bench/collect_stats.sh "results/${ENGINE}_stats.csv" kafka flink-jobmanager flink-taskmanager &
STATS_PID=$!

echo "==> [6/7] Produce ${RUN_SECONDS}s @ ${TARGET_RATE}/s + measure latency"
$PY common/latency_consumer.py --engine "$ENGINE" --warmup "$WARMUP_SECONDS" --seconds "$RUN_SECONDS" >/tmp/${ENGINE}_cons.log 2>&1 &
CONS=$!
sleep 2
$PY common/producer.py --rate "$TARGET_RATE" --seconds "$RUN_SECONDS" >/tmp/${ENGINE}_prod.log 2>&1
wait $CONS
kill $STATS_PID 2>/dev/null || true

echo "==> [7/7] Cancel job + tear down Flink (Kafka stays up)"
[ -n "$JOBID" ] && docker exec flink-jobmanager /opt/flink/bin/flink cancel "$JOBID" >/dev/null 2>&1 || true
kill "$SUBMIT_PID" 2>/dev/null || true   # the backgrounded blocking submit client
docker compose -f docker-compose.flink.yml down >/dev/null 2>&1

echo "==> DONE. Latency summary:"
sed -n '/LATENCY SUMMARY/,$p' /tmp/${ENGINE}_cons.log | head -32
