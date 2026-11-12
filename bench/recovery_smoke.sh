#!/usr/bin/env bash
# Local smoke of the recovery-test instrumentation (Flink s1-update): live input with the
# producer's --truth-sample, consumer with --hist --truth-sample, the TaskManager container
# killed at KILL_AT_S and started again, then bench/recovery_check.py on the artifacts. Checks
# that the whole path (TRUTHJSON / MAXJSON / RATEJSON / kill.txt -> recovery.json) works; the
# numbers are a laptop smoke, not a result.
#
# Env: RATE (2000) SECONDS_ (120) KILL_AT_S (45) CHECKPOINT_MS (10000) RESULTS_DIR
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"
set -a; . ./.env; set +a
OUT="${RESULTS_DIR:-results/local-stateful}"; D="$OUT/recovery-smoke/flink-ds-s1-update-kill"
rm -rf "$D"; mkdir -p "$D" "$OUT/logs"
PY=.venv/bin/python
RATE="${RATE:-2000}"; SECS="${SECONDS_:-120}"; KILL_AT="${KILL_AT_S:-45}"
K() { docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "$@"; }
recreate() {
  K --delete --if-exists --topic "$1" >/dev/null 2>&1
  for _ in $(seq 1 30); do K --list 2>/dev/null | grep -qx "$1" || break; sleep 1; done
  K --create --topic "$1" --partitions "$TOPIC_PARTITIONS" --replication-factor 1 >/dev/null
}
running_job() { curl -s http://localhost:8081/jobs/overview | $PY -c 'import sys,json
j=[x for x in json.load(sys.stdin)["jobs"] if x["state"]=="RUNNING"];print(j[0]["jid"] if j else "")' 2>/dev/null; }
offsets() { docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$1" --time -1 | awk -F: '{s+=$3} END {print s+0}'; }

docker compose -f docker-compose.kafka.yml up -d kafka >/dev/null
until [ "$(docker inspect -f '{{.State.Health.Status}}' kafka 2>/dev/null)" = "healthy" ]; do sleep 2; done
docker compose -f docker-compose.flink.yml up -d >/dev/null
until [ "$(curl -s http://localhost:8081/overview | $PY -c 'import sys,json;print(json.load(sys.stdin)["slots-total"])' 2>/dev/null)" -ge 6 ] 2>/dev/null; do sleep 2; done
for c in flink-jobmanager flink-taskmanager; do docker exec -u root "$c" chmod 777 /tmp/flink-checkpoints; done
recreate st-rec-in; recreate st-rec-out
docker exec -e KAFKA_BOOTSTRAP_INTERNAL="$KAFKA_BOOTSTRAP_INTERNAL" -e INPUT_TOPIC=st-rec-in -e OUTPUT_TOPIC=st-rec-out \
  -e TOPIC_PARTITIONS="$TOPIC_PARTITIONS" -e API=datastream -e WORKLOAD=s1-update -e STARTING_OFFSETS=latest \
  -e CHECKPOINT_MS="${CHECKPOINT_MS:-10000}" flink-jobmanager bash /workspace/flink/submit-flink-stateful.sh \
  >"$OUT/logs/recovery-smoke_submit.log" 2>&1 &
for _ in $(seq 1 45); do [ -n "$(running_job)" ] && break; sleep 2; done
sleep 10
$PY common/latency_consumer.py --engine recovery-smoke --topic st-rec-out --warmup 10 --seconds $((SECS + 30)) \
  --outdir "$D" --hist --no-raw --truth-sample 100 >"$D/consumer.raw" 2>&1 &
CONS=$!
sleep 2
$PY common/producer.py --topic st-rec-in --rate "$RATE" --seconds "$SECS" --seed 3 --keys 20000 \
  --truth-sample 100 >"$D/producer.log" 2>&1 &
PROD=$!
sleep "$KILL_AT"
echo "$(date +%s.%N | cut -c1-14) flink-taskmanager" >"$D/kill.txt"
docker kill flink-taskmanager >/dev/null; sleep 2; docker start flink-taskmanager >/dev/null
echo "killed + restarted TaskManager at $(cut -d' ' -f1 "$D/kill.txt")"
wait $PROD; wait $CONS
grep '^TRUTHJSON ' "$D/producer.log" | sed 's/^TRUTHJSON //' >"$D/truth.jsonl"
grep '^RATEJSON ' "$D/consumer.raw" | sed 's/^RATEJSON //' >"$D/rate.json"
grep '^MAXJSON ' "$D/consumer.raw" | sed 's/^MAXJSON //' >"$D/maxcnt.json"
cp "$D/recovery-smoke_summary.json" "$D/summary.json"
printf '{"input_records":%s,"output_records":%s}\n' "$(offsets st-rec-in)" "$(offsets st-rec-out)" >"$D/verify.json"
JOB=$(running_job); [ -n "$JOB" ] && curl -s -X PATCH "http://localhost:8081/jobs/$JOB?mode=cancel" >/dev/null
sleep 5; pkill -f submit-flink-stateful 2>/dev/null
docker compose -f docker-compose.flink.yml down >/dev/null 2>&1
rm -f "$D/consumer.raw"
$PY bench/recovery_check.py "$D"
