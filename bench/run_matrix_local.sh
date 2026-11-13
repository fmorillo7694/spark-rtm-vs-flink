#!/usr/bin/env bash
# Local p6 matrix: every engine x RUNS runs, strictly ONE engine at a time (they share the
# Kafka output topic, so concurrent runs contaminate each other). Each run also records a
# Docker-VM-vs-host clock probe every 10s (bench/clock_probe.py) next to its summary.
#
# Usage: bench/run_matrix_local.sh [engine ...]
#   engines: spark-rtm-java spark-rtm-scala pyspark-rtm spark-microbatch spark-rtm-syncckpt
#            flink flink-sql pyflink   (default: all, in that order)
# Env: RUNS (default 3), RESULTS_DIR (default results/local-4.3)
set -uo pipefail
cd "$(dirname "$0")/.."
export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"
RUNS="${RUNS:-3}"
export RESULTS_DIR="${RESULTS_DIR:-results/local-4.3}"
export LOG_DIR="$RESULTS_DIR/logs"
mkdir -p "$LOG_DIR"
ENGINES=("$@")
[ ${#ENGINES[@]} -gt 0 ] || ENGINES=(spark-rtm-java spark-rtm-scala pyspark-rtm spark-microbatch \
                                       spark-rtm-syncckpt flink flink-sql pyflink)

runner() {
  case "$1" in
    spark-rtm-java)     echo "bench/run_spark.sh rtm-java" ;;
    spark-rtm-scala)    echo "bench/run_spark.sh rtm" ;;
    spark-rtm-syncckpt) echo "bench/run_spark.sh rtm-syncckpt" ;;
    spark-microbatch)   echo "bench/run_spark.sh microbatch" ;;
    pyspark-rtm)        echo "bench/run_pyspark.sh" ;;
    flink)              echo "bench/run_flink.sh" ;;
    flink-sql)          echo "bench/run_flink_sql.sh" ;;
    pyflink)            echo "bench/run_pyflink.sh" ;;
    *) return 1 ;;
  esac
}

for e in "${ENGINES[@]}"; do
  cmd=$(runner "$e") || { echo "unknown engine $e"; exit 1; }
  for i in $(seq 1 "$RUNS"); do
    label="${e}-r${i}"
    echo "################ $label  start=$(date +%T)"
    ( while true; do .venv/bin/python bench/clock_probe.py --n 100 >> "$RESULTS_DIR/${label}_clock.jsonl" 2>/dev/null; sleep 10; done ) &
    PROBE=$!
    RUN_ID="$i" bash $cmd > "$LOG_DIR/${label}_runner.log" 2>&1
    rc=$?
    kill "$PROBE" 2>/dev/null; wait "$PROBE" 2>/dev/null
    [ -f "$RESULTS_DIR/${label}_latency.csv" ] && gzip -f "$RESULTS_DIR/${label}_latency.csv"
    echo "    rc=$rc  end=$(date +%T)  $(grep -o '"kept_records": [0-9]*' "$RESULTS_DIR/${label}_summary.json" 2>/dev/null)"
    sleep 10   # let the previous stack's containers fully exit before the next one starts
  done
done
.venv/bin/python bench/clock_probe.py --stop
echo "MATRIX DONE $(date +%T)"
