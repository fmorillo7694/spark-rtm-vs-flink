#!/usr/bin/env bash
# Dedup stall test (local, Flink): does a processing stall longer than the dedup retention let
# duplicates through? Per arm: fresh topics, job reads latest, producer writes live input with
# 10 % duplicates (re-sent 0-3 s later), and STALL_AT_S seconds in the TaskManager container is
# frozen with `docker pause` for STALL_S seconds (default 10 s > the 5 s TTL). Afterwards the
# input and output topics are captured and diffed by bench/stateful_check.py (s2: every
# distinct event_id exactly once).
#
# Expectation: processing-time TTL arms (ds:s2, sql:s2) leak duplicates whose original was
# processed before the stall and whose copy after it; event-time arms (ds:s2-et, sql:s2-rt)
# and the 60 s TTL arm (sql:s2-ttl60) leak none.
#
# Usage: bench/stall_test.sh [ARM ...]   (default: ds:s2 ds:s2-et sql:s2 sql:s2-rt sql:s2-ttl60)
# Env: RATE (2000) SECONDS_ (70) STALL_AT_S (25) STALL_S (10) RESULTS_DIR (results/local-stateful)
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"
set -a; . ./.env; set +a
ARMS=("$@"); [ ${#ARMS[@]} -gt 0 ] || ARMS=(ds:s2 ds:s2-et sql:s2 sql:s2-rt sql:s2-ttl60)
OUT="${RESULTS_DIR:-results/local-stateful}"; CAP="$OUT/captures/stall"; DST="$OUT/stall"
mkdir -p "$CAP" "$DST" "$OUT/logs"
PY=.venv/bin/python
RATE="${RATE:-2000}"; SECS="${SECONDS_:-70}"; STALL_AT="${STALL_AT_S:-25}"; STALL="${STALL_S:-10}"
log() { echo "[$(date +%T)] $*"; }
K() { docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "$@"; }
recreate() {
  K --delete --if-exists --topic "$1" >/dev/null 2>&1
  for _ in $(seq 1 30); do K --list 2>/dev/null | grep -qx "$1" || break; sleep 1; done
  K --create --topic "$1" --partitions "$TOPIC_PARTITIONS" --replication-factor 1 >/dev/null
}
running_job() { curl -s http://localhost:8081/jobs/overview | $PY -c 'import sys,json
j=[x for x in json.load(sys.stdin)["jobs"] if x["state"]=="RUNNING"];print(j[0]["jid"] if j else "")' 2>/dev/null; }

docker compose -f docker-compose.kafka.yml up -d kafka >/dev/null
until [ "$(docker inspect -f '{{.State.Health.Status}}' kafka 2>/dev/null)" = "healthy" ]; do sleep 2; done
docker compose -f docker-compose.flink.yml up -d >/dev/null
until [ "$(curl -s http://localhost:8081/overview | $PY -c 'import sys,json;print(json.load(sys.stdin)["slots-total"])' 2>/dev/null)" -ge 6 ] 2>/dev/null; do sleep 2; done
for c in flink-jobmanager flink-taskmanager; do docker exec -u root "$c" chmod 777 /tmp/flink-checkpoints; done

rc=0
for arm in "${ARMS[@]}"; do
  api=datastream; [ "${arm%%:*}" = sql ] && api=sql
  wl=${arm#*:}; label="flink-${arm/:/-}-stall"
  log "== $label (stall ${STALL}s at t+${STALL_AT}s)"
  recreate st-stall-in; recreate st-stall-out
  docker exec -e KAFKA_BOOTSTRAP_INTERNAL="$KAFKA_BOOTSTRAP_INTERNAL" -e INPUT_TOPIC=st-stall-in \
    -e OUTPUT_TOPIC=st-stall-out -e TOPIC_PARTITIONS="$TOPIC_PARTITIONS" -e API="$api" -e WORKLOAD="$wl" \
    -e STARTING_OFFSETS=latest -e CHECKPOINT_MS=60000 \
    flink-jobmanager bash /workspace/flink/submit-flink-stateful.sh >"$OUT/logs/${label}_submit.log" 2>&1 &
  JOBID=""; for _ in $(seq 1 45); do JOBID=$(running_job); [ -n "$JOBID" ] && break; sleep 2; done
  [ -n "$JOBID" ] || { log "!! not RUNNING"; tail -5 "$OUT/logs/${label}_submit.log"; rc=1; continue; }
  sleep 10
  $PY common/producer.py --topic st-stall-in --rate "$RATE" --seconds "$SECS" --seed 7 --keys 5000 \
    --dup-rate 0.1 --dup-max-delay-ms 3000 --tail-heartbeat-s 20 >"$OUT/logs/${label}_prod.log" 2>&1 &
  PROD=$!
  sleep "$STALL_AT"; docker pause flink-taskmanager >/dev/null; log "   TaskManager paused"
  sleep "$STALL"; docker unpause flink-taskmanager >/dev/null; log "   TaskManager resumed"
  wait $PROD; tail -1 "$OUT/logs/${label}_prod.log" | sed 's/^/   /'
  $PY common/topic_capture.py --topic st-stall-in --out "$CAP/${label}-input.jsonl" --idle-seconds 5 | tail -1
  $PY common/topic_capture.py --topic st-stall-out --out "$CAP/${label}.jsonl" --idle-seconds 25 | tail -1
  curl -s -X PATCH "http://localhost:8081/jobs/$JOBID?mode=cancel" >/dev/null
  for _ in $(seq 1 30); do [ -z "$(running_job)" ] && break; sleep 2; done
  pkill -f "submit-flink-stateful" 2>/dev/null
  $PY bench/stateful_check.py --workload s2 --input "$CAP/${label}-input.jsonl" \
    --out "$DST/${label}.json" "$label=$CAP/${label}.jsonl" | tee "$DST/${label}.txt"
done
docker compose -f docker-compose.flink.yml down >/dev/null 2>&1
log "DONE"
exit $rc
