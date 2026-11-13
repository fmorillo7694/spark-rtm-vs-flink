#!/usr/bin/env bash
# Produce the shared deterministic correctness input ONCE into $CORR_INPUT_TOPIC (6 partitions)
# and capture it to results/local-stateful/captures/input.jsonl. Every engine then replays this
# exact topic from earliest (bench/run_stateful.sh PHASE=replay), so all engines see identical
# bytes in identical per-partition order. Content is seeded; ts is the produce wall clock.
#
# Env: CORR_RATE (2000) CORR_SECONDS (60) CORR_KEYS (5000) DUP_RATE (0.1)
#      HOT_KEY_FRACTION (0.1) SEED (42) CORR_INPUT_TOPIC (st-corr-input) RESULTS_DIR
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"
set -a; . ./.env; set +a
T="${CORR_INPUT_TOPIC:-st-corr-input}"
OUT="${RESULTS_DIR:-results/local-stateful}"
mkdir -p "$OUT/captures"
PY=.venv/bin/python

docker compose -f docker-compose.kafka.yml up -d kafka >/dev/null
until [ "$(docker inspect -f '{{.State.Health.Status}}' kafka 2>/dev/null)" = "healthy" ]; do sleep 2; done
K() { docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "$@"; }
K --delete --if-exists --topic "$T" >/dev/null 2>&1 || true
for _ in $(seq 1 30); do K --list | grep -qx "$T" || break; sleep 1; done
K --create --topic "$T" --partitions "$TOPIC_PARTITIONS" --replication-factor 1 >/dev/null

$PY common/producer.py --topic "$T" --rate "${CORR_RATE:-2000}" --seconds "${CORR_SECONDS:-60}" \
  --seed "${SEED:-42}" --keys "${CORR_KEYS:-5000}" --dup-rate "${DUP_RATE:-0.1}" \
  --hot-key-fraction "${HOT_KEY_FRACTION:-0.1}" --hot-keys 10 --dup-max-delay-ms 3000 \
  --tail-heartbeat-s 20 | tail -1
$PY common/topic_capture.py --topic "$T" --out "$OUT/captures/input.jsonl" --idle-seconds 5
