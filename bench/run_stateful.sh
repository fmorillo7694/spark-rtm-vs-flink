#!/usr/bin/env bash
# Local stateful runs, ONE engine family at a time (its cluster stays up across its arms).
# Two phases per arm:
#   replay  correctness: read the shared deterministic input topic from EARLIEST into a fresh
#           output topic, capture the output (common/topic_capture.py) for stateful_check.py.
#           Needs bench/stateful_input.sh to have produced $CORR_INPUT_TOPIC first.
#   smoke   short latency smoke at a low pinned rate (labels are SMOKE ONLY, not results).
#
# Usage: bench/run_stateful.sh spark|flink [ARM ...]
#   spark arms: rtm:s1 rtm:s2 rtm:s3 microbatch:s1                 (default: all four)
#   flink arms: ds:s1-window ds:s1-update ds:s2 ds:s3 sql:s1 sql:s2 ds-eo:s2   (default: all)
#               ds-eo = DataStream with the EXACTLY_ONCE transactional Kafka sink; its
#               consumers read with isolation.level=read_committed.
#               ds-bt100:<wl> / sql-bt100:<wl> (opt-in) = Flink's default 100 ms buffer timeout.
#               Session B workloads (opt-in): ds:s2-et (event-time dedup), sql:s2-rt (rowtime
#               dedup), sql:s2-ttl60, ds:s3-lt and spark rtm:s3-lt (1 s-bucket light timer).
#               Session C joins (opt-in, run bench/join_input.sh first): spark rtm:s4,
#               microbatch:s5, mb1s:s5 (1 s trigger); flink ds:s4, ds-bt100:s4, ds:s5, ds-bt100:s5.
# Env: PHASE=replay|smoke|both (both), RESULTS_DIR (results/local-stateful),
#      SMOKE_SECONDS (120) SMOKE_RATE (3000) WARMUP (20) KEYS (20000)
#      S2_DUP_RATE (0.1, dedup smokes) DUP_RATE (0, s1/s3 smokes: a late duplicate would be
#      measured against the window's / session's max ts, inflating the latency anchor)
#      HOT_KEY_FRACTION (0.1) SEED (42) CORR_INPUT_TOPIC (st-corr-input)
#      KEEP_UP=1 leaves the engine cluster running at the end.
#      SHUFFLE_MAX_WAIT_MS (spark only) sets spark.shuffle.streaming.networkBufferMaxWaitTimeMs;
#      labels get a -sw<N> suffix.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"
set -a; . ./.env; set +a

FAMILY="${1:?usage: run_stateful.sh spark|flink [ARM ...]}"; shift
ARMS=("$@")
if [ ${#ARMS[@]} -eq 0 ]; then
  case "$FAMILY" in
    spark) ARMS=(rtm:s1 rtm:s2 rtm:s3 microbatch:s1) ;;
    flink) ARMS=(ds:s1-window ds:s1-update ds:s2 ds:s3 sql:s1 sql:s2 ds-eo:s2) ;;
    *) echo "unknown family $FAMILY"; exit 1 ;;
  esac
fi
PHASE="${PHASE:-both}"
OUT="${RESULTS_DIR:-results/local-stateful}"
LOGDIR="$OUT/logs"; CAPDIR="$OUT/captures"
mkdir -p "$OUT/smoke" "$LOGDIR" "$CAPDIR"
PY=.venv/bin/python
CORR_IN="${CORR_INPUT_TOPIC:-st-corr-input}"
# Join workloads (bench/join_input.sh loads these): s4 reference table, s5 replay inputs
REF_TOPIC="${REF_TOPIC:-st-ref}"; CORR_ORDERS="${CORR_ORDERS_TOPIC:-st-corr-orders}"
CORR_PAY="${CORR_PAYMENTS_TOPIC:-st-corr-payments}"
SMOKE_SECONDS="${SMOKE_SECONDS:-120}"; SMOKE_RATE="${SMOKE_RATE:-3000}"; WARMUP="${WARMUP:-20}"
GEN_ARGS=(--seed "${SEED:-42}" --keys "${KEYS:-20000}"
          --hot-key-fraction "${HOT_KEY_FRACTION:-0.1}" --hot-keys 10 --dup-max-delay-ms 3000)

log() { echo "[$(date +%T)] $*"; }

topic_recreate() {
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --delete --if-exists --topic "$1" >/dev/null 2>&1
  for _ in $(seq 1 30); do
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null \
      | grep -qx "$1" || break
    sleep 1
  done
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
    --topic "$1" --partitions "$TOPIC_PARTITIONS" --replication-factor 1 >/dev/null
}

kafka_up() {
  docker compose -f docker-compose.kafka.yml up -d kafka >/dev/null
  until [ "$(docker inspect -f '{{.State.Health.Status}}' kafka 2>/dev/null)" = "healthy" ]; do sleep 2; done
}

# ---------------------------------------------------------------- Spark
spark_up() {
  SPARK_WORKER_CORES=12 docker compose -f docker-compose.spark.yml up -d spark-master spark-worker-1 >/dev/null
  until [ "$(docker inspect -f '{{.State.Health.Status}}' spark-master 2>/dev/null)" = "healthy" ]; do sleep 2; done
  for c in spark-master spark-worker-1; do
    docker exec -u root "$c" bash -c 'mkdir -p /opt/spark/.ivy2 /tmp/spark-checkpoints && chmod -R 777 /opt/spark/.ivy2 /tmp/spark-checkpoints'
  done
  until [ "$(curl -s http://localhost:8080/json/ | $PY -c 'import sys,json;w=json.load(sys.stdin)["workers"];print(sum(x["cores"] for x in w))' 2>/dev/null)" = "12" ]; do sleep 2; done
  log "spark up: 1 worker, 12 slots"
}
spark_down() { docker compose -f docker-compose.spark.yml down >/dev/null 2>&1; }
spark_start() {  # label mode workload in out start [extra env...]
  local label=$1 mode=$2 wl=$3 in=$4 outt=$5 start=$6; shift 6
  docker exec -e KAFKA_BOOTSTRAP_INTERNAL="$KAFKA_BOOTSTRAP_INTERNAL" -e INPUT_TOPIC="$in" \
    -e OUTPUT_TOPIC="$outt" -e WORKLOAD="$wl" -e MODE="$mode" -e STARTING_OFFSETS="$start" \
    -e SPARK_VERSION="$SPARK_VERSION" -e SPARK_SCALA_BINARY="$SPARK_SCALA_BINARY" \
    -e SHUFFLE_MAX_WAIT_MS="${SHUFFLE_MAX_WAIT_MS:-}" "$@" \
    spark-master bash /workspace/spark/submit-stateful.sh >"$LOGDIR/${label}_submit.log" 2>&1 &
  for _ in $(seq 1 60); do
    grep -q '\[StatefulPipelineJava\]' "$LOGDIR/${label}_submit.log" && { grep '\[StatefulPipelineJava\]' "$LOGDIR/${label}_submit.log"; return 0; }
    grep -qE 'Exception in thread|Error: ' "$LOGDIR/${label}_submit.log" && break
    sleep 2
  done
  log "!! $label did not start"; tail -25 "$LOGDIR/${label}_submit.log"; return 1
}
spark_evidence() {  # label  (query still running)
  local label=$1
  local f="$OUT/smoke/${label}_evidence.txt"
  {
    echo "# $label spark evidence at $(date -u +%FT%TZ)"
    PID=$(docker exec spark-master bash -c 'jps -l 2>/dev/null | awk "/SparkSubmit/{print \$1; exit}"')
    if [ -n "$PID" ]; then
      DUMP=$(docker exec -u "$(docker exec spark-master stat -c %u /proc/"$PID")" spark-master jstack "$PID" 2>&1)
      echo "async-log-write threads: $(grep -c '"async-log-write' <<<"$DUMP")"
      echo "AsyncProgressTrackingMicroBatchExecution frames: $(grep -c 'AsyncProgressTrackingMicroBatchExecution' <<<"$DUMP")"
    fi
    APP=$(docker exec spark-master curl -s localhost:4040/api/v1/applications | $PY -c 'import sys,json;print(json.load(sys.stdin)[0]["id"])' 2>/dev/null)
    echo "--- active stages (pipelined shuffle => source stage and stateful stage RUNNING together)"
    docker exec spark-master curl -s "localhost:4040/api/v1/applications/$APP/stages?status=active" \
      | $PY -c 'import sys,json
for s in json.load(sys.stdin): print("stage", s["stageId"], s["status"], "tasks", s["numTasks"], "active", s["numActiveTasks"], s["name"][:90])' 2>&1
    echo "--- driver log: pipelined / state store / commit lines"
    grep -i -E 'pipelin|RocksDBStateStoreProvider|changelog|checkpointFormatVersion|asyncProgress' "$LOGDIR/${label}-smoke_submit.log" | cut -c1-220 | sort | uniq -c | sort -rn | head -15
    CK=$(docker exec spark-master bash -c 'ls -td /tmp/spark-checkpoints/stateful-*/ 2>/dev/null | head -1')
    echo "--- checkpoint $CK"
    docker exec spark-master bash -c "ls '$CK' '$CK/state/0' 2>/dev/null | head -20; echo '[commits]'; ls '$CK/commits' 2>/dev/null; echo '[latest commit]'; cd '$CK/commits' 2>/dev/null && cat \"\$(ls | grep -v crc | sort -n | tail -1)\"; echo; echo '[state files, partition 0]'; find '$CK/state/0' -type f 2>/dev/null | head -10"
  } >"$f" 2>&1
  head -6 "$f" | sed 's/^/    /'
}
spark_stop() {
  docker exec spark-master bash -c 'pkill -f StatefulPipelineJava' >/dev/null 2>&1
  for _ in $(seq 1 30); do
    n=$(curl -s http://localhost:8080/json/ | $PY -c 'import sys,json;print(len(json.load(sys.stdin)["activeapps"]))' 2>/dev/null || echo 1)
    [ "$n" = 0 ] && break; sleep 2
  done
}

# ---------------------------------------------------------------- Flink
flink_up() {
  docker compose -f docker-compose.flink.yml up -d >/dev/null
  until [ "$(docker inspect -f '{{.State.Health.Status}}' flink-jobmanager 2>/dev/null)" = "healthy" ]; do sleep 2; done
  until [ "$(curl -s http://localhost:8081/overview | $PY -c 'import sys,json;print(json.load(sys.stdin)["slots-total"])' 2>/dev/null)" -ge 6 ] 2>/dev/null; do sleep 2; done
  for c in flink-jobmanager flink-taskmanager; do docker exec -u root "$c" chmod 777 /tmp/flink-checkpoints; done
  log "flink up: 1 TM, 6 slots"
}
flink_down() { docker compose -f docker-compose.flink.yml down >/dev/null 2>&1; }
running_job() { curl -s http://localhost:8081/jobs/overview | $PY -c 'import sys,json
j=[x for x in json.load(sys.stdin)["jobs"] if x["state"]=="RUNNING"];print(j[0]["jid"] if j else "")' 2>/dev/null; }
flink_start() {  # label api workload delivery in out start [extra env...]
  local label=$1 api=$2 wl=$3 del=$4 in=$5 outt=$6 start=$7; shift 7
  docker exec -e KAFKA_BOOTSTRAP_INTERNAL="$KAFKA_BOOTSTRAP_INTERNAL" -e INPUT_TOPIC="$in" \
    -e OUTPUT_TOPIC="$outt" -e TOPIC_PARTITIONS="$TOPIC_PARTITIONS" -e API="$api" \
    -e WORKLOAD="$wl" -e DELIVERY="$del" -e STARTING_OFFSETS="$start" "$@" \
    flink-jobmanager bash /workspace/flink/submit-flink-stateful.sh >"$LOGDIR/${label}_submit.log" 2>&1 &
  JOBID=""
  for _ in $(seq 1 45); do
    JOBID=$(running_job)
    [ -n "$JOBID" ] && break; sleep 2
  done
  [ -n "$JOBID" ] || { log "!! $label not RUNNING"; tail -25 "$LOGDIR/${label}_submit.log"; return 1; }
  # wait for every task to be RUNNING (not just the job)
  for _ in $(seq 1 30); do
    curl -s "http://localhost:8081/jobs/$JOBID" | $PY -c 'import sys,json
v=json.load(sys.stdin)["vertices"];sys.exit(0 if all(x["status"]=="RUNNING" for x in v) else 1)' && break
    sleep 2
  done
  log "$label JobID=$JOBID"
}
flink_evidence() {
  local label=$1
  local f="$OUT/smoke/${label}_evidence.txt"
  {
    echo "# $label flink evidence at $(date -u +%FT%TZ) job=$JOBID"
    echo "--- checkpoint config"; curl -s "http://localhost:8081/jobs/$JOBID/checkpoints/config"; echo
    echo "--- checkpoint counts"; curl -s "http://localhost:8081/jobs/$JOBID/checkpoints" | $PY -c 'import sys,json
d=json.load(sys.stdin);print(d["counts"]);l=d.get("latest",{}).get("completed") or {}
print("latest completed: id",l.get("id"),"state_size",l.get("state_size"),"checkpointed_size",l.get("checkpointed_size"),"duration_ms",l.get("end_to_end_duration"))
for h in d.get("history",[]): print("  ckpt",h["id"],h["status"],"trigger",h.get("trigger_timestamp"),"dur_ms",h.get("end_to_end_duration"),"size",h.get("checkpointed_size"),(h.get("failure_message") or "")[:200])' 2>&1
    echo "--- JM log: checkpoint failures"
    docker logs flink-jobmanager 2>&1 | grep -E "$JOBID|Checkpoint" | grep -i -E 'fail|declin|expired' | tail -5 | cut -c1-300
    echo "--- job vertices"; curl -s "http://localhost:8081/jobs/$JOBID" | $PY -c 'import sys,json
for v in json.load(sys.stdin)["vertices"]: print(v["parallelism"], v["status"], v["name"][:150])' 2>&1
    echo "--- TM log: state backend"
    docker logs flink-taskmanager 2>&1 | grep -i -E 'StateBackend|rocksdb' | grep -v -i 'DEBUG' | cut -c1-220 | sort | uniq -c | sort -rn | head -6
  } >"$f" 2>&1
  head -4 "$f" | cut -c1-300 | sed 's/^/    /'
}
flink_stop() {
  # cancel EVERY non-terminal job: a job that never reached RUNNING (no JOBID) keeps restarting
  # and holding slots otherwise, and starves every later arm
  local j
  for j in $(curl -s http://localhost:8081/jobs/overview | $PY -c 'import sys,json
print(" ".join(x["jid"] for x in json.load(sys.stdin)["jobs"] if x["state"] not in ("CANCELED","FAILED","FINISHED")))' 2>/dev/null); do
    curl -s -X PATCH "http://localhost:8081/jobs/$j?mode=cancel" >/dev/null
  done
  for _ in $(seq 1 30); do
    [ -z "$(curl -s http://localhost:8081/jobs/overview | $PY -c 'import sys,json
print("".join(x["jid"] for x in json.load(sys.stdin)["jobs"] if x["state"] not in ("CANCELED","FAILED","FINISHED")))' 2>/dev/null)" ] && break
    sleep 2
  done
  pkill -f "submit-flink-stateful" 2>/dev/null; sleep 2
}

# ---------------------------------------------------------------- arm plumbing
arm_label() { echo "${FAMILY}-${1/:/-}${SHUFFLE_MAX_WAIT_MS:+-sw${SHUFFLE_MAX_WAIT_MS}}"; }
join_env() {  # workload phase -> -e args for the join workloads' second topic
  case "$1:$2" in
    s4:*) echo "-e SECOND_TOPIC=$REF_TOPIC" ;;
    s5:replay) echo "-e SECOND_TOPIC=$CORR_PAY" ;;
    s5:smoke) echo "-e SECOND_TOPIC=st-payments" ;;
  esac
}
arm_start() {  # arm in out start
  local arm=$1 in=$2 outt=$3 start=$4 label
  local kind=${arm%%:*} wl=${arm#*:}
  label=$(arm_label "$arm")-$PH
  local -a jenv
  read -r -a jenv <<<"$(join_env "$wl" "$PH")"
  if [ "$FAMILY" = spark ]; then
    local extra=(); [ "$PH" = replay ] && extra=(-e RTM_TRIGGER="10 seconds" -e SESSION_TIMEOUT_MS=30000)
    local mode=$kind
    # mb1s = micro-batch with a 1 s processing-time trigger (s5); microbatch = Spark's default trigger
    [ "$kind" = mb1s ] && { mode=microbatch; extra+=(-e MB_TRIGGER="1 second"); }
    spark_start "$label" "$mode" "$wl" "$in" "$outt" "$start" ${extra[@]+"${extra[@]}"} ${jenv[@]+"${jenv[@]}"}
  else
    local api=datastream del=AT_LEAST_ONCE extra=(${jenv[@]+"${jenv[@]}"})
    case "$kind" in sql|sql-bt100) api=sql ;; esac
    [ "$kind" = ds-eo ] && del=EXACTLY_ONCE
    [ "$PH" = replay ] && extra+=(-e CHECKPOINT_MS=10000 -e SESSION_TIMEOUT_MS=30000)
    case "$kind" in *-bt100) extra+=(-e BUFFER_TIMEOUT_MS=100) ;; esac   # Flink default buffer timeout
    flink_start "$label" "$api" "$wl" "$del" "$in" "$outt" "$start" ${extra[@]+"${extra[@]}"}
  fi
}
arm_stop() { if [ "$FAMILY" = spark ]; then spark_stop; else flink_stop; fi; }
isolation_of() { [ "${1%%:*}" = ds-eo ] && echo read_committed || echo read_uncommitted; }

replay() {
  local arm=$1 label outt idle=25
  PH=replay; label=$(arm_label "$arm")
  outt="st-out-${label}"
  case "${arm#*:}" in s3*) idle=60 ;; esac
  local in="$CORR_IN"; [ "${arm#*:}" = s5 ] && in="$CORR_ORDERS"
  log "== replay $label  ($in earliest -> $outt)"
  topic_recreate "$outt"
  arm_start "$arm" "$in" "$outt" earliest || { arm_stop; return 1; }
  $PY common/topic_capture.py --topic "$outt" --out "$CAPDIR/${label}.jsonl" --idle-seconds "$idle" \
    --max-seconds 420 --isolation "$(isolation_of "$arm")" 2>&1 | tail -1
  arm_stop
}

smoke() {
  local arm=$1 label stats
  PH=smoke; label=$(arm_label "$arm")
  log "== smoke $label  (${SMOKE_RATE}/s x ${SMOKE_SECONDS}s, SMOKE ONLY)"
  topic_recreate st-input; topic_recreate st-output
  local pay=()
  if [ "${arm#*:}" = s5 ]; then
    topic_recreate st-payments
    # SMOKE_RATE = orders/s; ~0.85 payments per order follow, so input records/s ~ 1.85x
    pay=(--payments-topic st-payments)
  fi
  arm_start "$arm" st-input st-output latest || { arm_stop; return 1; }
  sleep 10
  if [ "$FAMILY" = spark ]; then
    bash bench/collect_stats.sh "$OUT/smoke/${label}_stats.csv" kafka spark-master spark-worker-1 & stats=$!
  else
    bash bench/collect_stats.sh "$OUT/smoke/${label}_stats.csv" kafka flink-jobmanager flink-taskmanager & stats=$!
  fi
  $PY common/latency_consumer.py --engine "${label}-SMOKE" --topic st-output --warmup "$WARMUP" \
    --seconds "$SMOKE_SECONDS" --outdir "$OUT/smoke" --isolation "$(isolation_of "$arm")" \
    >"$LOGDIR/${label}_cons.log" 2>&1 & local cons=$!
  sleep 2
  local dup="${DUP_RATE:-0}"; case "${arm#*:}" in s2*) dup="${S2_DUP_RATE:-0.1}" ;; esac
  $PY common/producer.py --topic st-input --rate "$SMOKE_RATE" --seconds "$SMOKE_SECONDS" "${GEN_ARGS[@]}" --dup-rate "$dup" \
    ${pay[@]+"${pay[@]}"} >"$LOGDIR/${label}_prod.log" 2>&1
  wait $cons
  if [ "$FAMILY" = spark ]; then spark_evidence "$label"; else flink_evidence "$label"; fi
  kill $stats 2>/dev/null
  gzip -f "$OUT/smoke/${label}-SMOKE_latency.csv" 2>/dev/null   # local only (gitignored)
  grep -o '"kept_records": [0-9]*' "$OUT/smoke/${label}-SMOKE_summary.json" 2>/dev/null | sed 's/^/    /'
  tail -1 "$LOGDIR/${label}_prod.log" | sed 's/^/    /'
  arm_stop
}

kafka_up
if [ "$FAMILY" = spark ]; then spark_up; else flink_up; flink_stop; fi   # flink_stop: clear jobs left on a KEEP_UP cluster
for arm in "${ARMS[@]}"; do
  [ "$PHASE" = smoke ] || replay "$arm"
  [ "$PHASE" = replay ] || smoke "$arm"
done
[ "${KEEP_UP:-0}" = 1 ] || { if [ "$FAMILY" = spark ]; then spark_down; else flink_down; fi; }
log "DONE $FAMILY ${ARMS[*]}"
