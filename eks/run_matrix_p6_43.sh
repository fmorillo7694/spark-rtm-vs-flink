#!/usr/bin/env bash
# EKS p6 matrix for Spark 4.3.0 / Flink 2.3.0, strictly one engine at a time.
# Robust to a flaky operator laptop connection:
#   - before each run, wait up to 10 min for AWS STS + the cluster API to answer;
#   - a run that fails on connectivity is retried (once the link is back), not skipped;
#   - two failed runs in a row stop the loop and leave the cluster idle;
#   - no run starts once the cluster has been up BUDGET_H hours (idle time counts).
# A run counts as done only when its summary has >= 9.9M kept records.
#
# Usage: eks/run_matrix_p6_43.sh
# Env: PLAN_ITEMS="engine:trigger_s:run ..." (default: full plan), BUDGET_H (9.1),
#      CLUSTER_START=<epoch>, RESULTS_DIR (results/eks-4.3)
set -uo pipefail
cd "$(dirname "$0")/.."
export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH" AWS_REGION=us-east-1
export KUBECONFIG="$PWD/eks/kubeconfig"
BUDGET_H="${BUDGET_H:-9.1}"; CLUSTER_START="${CLUSTER_START:-$(date +%s)}"
export RESULTS_DIR="${RESULTS_DIR:-results/eks-4.3}"
mkdir -p "$RESULTS_DIR/logs"

if [ -n "${PLAN_ITEMS:-}" ]; then read -r -a PLAN <<<"$PLAN_ITEMS"; else
  PLAN=()
  for e in spark-rtm-java spark-rtm-scala pyspark-rtm spark-rtm-syncckpt flink flink-sql pyflink; do
    for i in 1 2 3; do PLAN+=("$e:60:$i"); done; done
  for e in spark-rtm-java spark-rtm-syncckpt flink; do PLAN+=("$e:5:1"); done
fi

hours() { awk "BEGIN{printf \"%.2f\", ($(date +%s)-$CLUSTER_START)/3600}"; }
over_budget() { awk "BEGIN{exit !($(hours) >= $BUDGET_H)}"; }
online() {
  aws sts get-caller-identity --cli-connect-timeout 10 --cli-read-timeout 10 >/dev/null 2>&1 &&
  kubectl --request-timeout=15s get --raw /readyz >/dev/null 2>&1
}
wait_online() {  # up to 10 min
  for w in $(seq 1 40); do online && return 0; echo "    offline (try $w/40), waiting 15s"; sleep 15; done
  return 1
}
label_of() { local e=$1 t=$2 i=$3 l="${1}-p6"; [ "$t" != 60 ] && l="$l-ckpt${t}s"; echo "$l-r$i"; }
done_ok() { python3 -c "import json,sys;sys.exit(0 if json.load(open(sys.argv[1]))['kept_records']>=9900000 else 1)" \
  "$RESULTS_DIR/$1_summary.json" 2>/dev/null; }
cleanup() {
  kubectl --request-timeout=30s -n kafka delete job latency-consumer producer --ignore-not-found --wait=false >/dev/null 2>&1
  kubectl --request-timeout=60s -n spark delete sparkapplication --all --ignore-not-found >/dev/null 2>&1
  kubectl --request-timeout=60s -n flink delete flinkdeployment --all --ignore-not-found >/dev/null 2>&1
}

FAILS=0
for item in "${PLAN[@]}"; do
  IFS=: read -r e trig i <<<"$item"
  lab=$(label_of "$e" "$trig" "$i")
  done_ok "$lab" && { echo "-- $lab already done, skipping"; continue; }
  attempt=0
  while true; do
    over_budget && { echo "!! BUDGET: cluster up $(hours)h >= ${BUDGET_H}h, stopping before $lab"; echo "MATRIX STOPPED (budget)"; exit 3; }
    wait_online || { echo "!! OFFLINE >10 min before $lab, stopping; cluster left idle"; echo "MATRIX STOPPED (offline)"; exit 4; }
    attempt=$((attempt+1))
    echo "######## $lab attempt=$attempt start=$(date +%T) cluster_h=$(hours)"
    cleanup
    RUN_ID="$i" TRIGGER_S="$trig" bash eks/04b_run_p6.sh "$e" > "$RESULTS_DIR/logs/${lab}.a${attempt}.log" 2>&1
    rc=$?
    echo "    rc=$rc  $(grep -E 'SAVED|NO SUMMARY|!!' "$RESULTS_DIR/logs/${lab}.a${attempt}.log" | tail -1)"
    if done_ok "$lab"; then FAILS=0; break; fi
    FAILS=$((FAILS+1))
    [ $FAILS -ge 2 ] && { echo "!! 2 failed runs in a row (last $lab), stopping; cluster left idle"; cleanup; echo "MATRIX STOPPED (2 failures)"; exit 5; }
    if online; then echo "    failed with the link up; moving on"; break; fi
    echo "    connectivity failure; will retry $lab"
  done
done
echo "MATRIX DONE $(date +%T) cluster_h=$(hours)"
