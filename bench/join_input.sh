#!/usr/bin/env bash
# Inputs for the session C join workloads, produced ONCE and shared by every engine:
#   - $REF_TOPIC (compacted, 6 partitions): the S4 static reference table, REF_ROWS users
#     (common/joindata.py rules: user_id % 50 has no row). Used by both the replays and smokes.
#   - $CORR_ORDERS_TOPIC + $CORR_PAYMENTS_TOPIC: the S5 correctness input (seeded orders, ~80 %
#     with a payment 0-9.5 s later, ~5 % with a payment 11-14 s later = outside the 10 s bound),
#     captured to results/local-stateful/captures/{orders,payments}.jsonl.
# The S4 replay reuses the shared S1-S3 input ($CORR_INPUT_TOPIC, bench/stateful_input.sh).
#
# Env: REF_ROWS (1000000) CORR_RATE (2000 orders/s) CORR_SECONDS (60) CORR_KEYS (5000) SEED (42)
#      REF_TOPIC (st-ref) CORR_ORDERS_TOPIC (st-corr-orders) CORR_PAYMENTS_TOPIC (st-corr-payments)
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"
set -a; . ./.env; set +a
OUT="${RESULTS_DIR:-results/local-stateful}"
mkdir -p "$OUT/captures"
PY=.venv/bin/python
REF="${REF_TOPIC:-st-ref}"; ORD="${CORR_ORDERS_TOPIC:-st-corr-orders}"; PAY="${CORR_PAYMENTS_TOPIC:-st-corr-payments}"

docker compose -f docker-compose.kafka.yml up -d kafka >/dev/null
until [ "$(docker inspect -f '{{.State.Health.Status}}' kafka 2>/dev/null)" = "healthy" ]; do sleep 2; done
K() { docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "$@"; }
fresh() {  # topic [extra create args]
  local t=$1; shift
  K --delete --if-exists --topic "$t" >/dev/null 2>&1 || true
  for _ in $(seq 1 30); do K --list | grep -qx "$t" || break; sleep 1; done
  K --create --topic "$t" --partitions "$TOPIC_PARTITIONS" --replication-factor 1 "$@" >/dev/null
}

fresh "$REF" --config cleanup.policy=compact
$PY common/producer.py --topic "$REF" --ref-rows "${REF_ROWS:-1000000}" --ref-miss-mod 50 | tail -1

fresh "$ORD"; fresh "$PAY"
$PY common/producer.py --topic "$ORD" --payments-topic "$PAY" --rate "${CORR_RATE:-2000}" \
  --seconds "${CORR_SECONDS:-60}" --seed "${SEED:-42}" --keys "${CORR_KEYS:-5000}" | tail -1
$PY common/topic_capture.py --topic "$ORD" --out "$OUT/captures/orders.jsonl" --idle-seconds 5
$PY common/topic_capture.py --topic "$PAY" --out "$OUT/captures/payments.jsonl" --idle-seconds 5
