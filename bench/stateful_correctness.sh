#!/usr/bin/env bash
# Diff every captured replay output against the ground truth of the shared input.
# Writes results/local-stateful/correctness/<workload>.{json,txt}. Exit 1 on any mismatch.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
OUT="${RESULTS_DIR:-results/local-stateful}"
CAP="$OUT/captures"; DST="$OUT/correctness"
mkdir -p "$DST"
PY=.venv/bin/python
rc=0
specs() {  # workload -> NAME=file for every capture present
  local wl=$1 f n
  for f in "$CAP"/*.jsonl; do
    n=$(basename "$f" .jsonl)
    [ "$n" = input ] && continue
    case "$wl:$n" in
      s1:*-s1|s1:*-s1-window|s1:*-s1-update) echo "$n=$f" ;;
      s2:*-s2|s2:*-s2-et|s2:*-s2-rt|s2:*-s2-ttl60) echo "$n=$f" ;;
      s3:*-s3|s3:*-s3-lt) echo "$n=$f" ;;
      s4:*-s4) echo "$n=$f" ;;
      s5:*-s5) echo "$n=$f" ;;
    esac
  done
}
for wl in s1 s2 s3 s4 s5; do
  S=$(specs "$wl")
  [ -n "$S" ] || { echo "[$wl] no captures"; continue; }
  in="$CAP/input.jsonl"; extra=()
  [ "$wl" = s4 ] && extra=(--ref-rows "${REF_ROWS:-1000000}" --ref-miss-mod 50)
  [ "$wl" = s5 ] && { in="$CAP/orders.jsonl"; extra=(--payments "$CAP/payments.jsonl" --join-bound-s 10); }
  # shellcheck disable=SC2086
  $PY bench/stateful_check.py --workload "$wl" --input "$in" --window-s 10 --gap-ms 5000 \
    ${extra[@]+"${extra[@]}"} --out "$DST/$wl.json" $S | tee "$DST/$wl.txt" || rc=1
done
exit $rc
