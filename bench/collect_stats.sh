#!/usr/bin/env bash
# Sample `docker stats` ~1 Hz for the given containers into a CSV until killed.
# Columns: epoch,container,cpu_pct,mem_used_mb,mem_pct
#
# Usage: collect_stats.sh OUTFILE CONTAINER [CONTAINER...]
set -euo pipefail

OUT="$1"; shift
CONTAINERS=("$@")

echo "epoch,container,cpu_pct,mem_used_mb,mem_pct" > "$OUT"

while true; do
  ts=$(date +%s)
  # --no-stream takes one snapshot; parse cpu%, mem usage, mem%.
  docker stats --no-stream --format '{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}' "${CONTAINERS[@]}" 2>/dev/null \
  | while IFS=$'\t' read -r name cpu mem mempct; do
      cpu_v="${cpu/\%/}"
      # MemUsage looks like "123.4MiB / 2GiB" -> take the used part, normalize to MB.
      used="${mem%% /*}"
      num="${used//[^0-9.]/}"
      if [[ "$used" == *GiB* ]]; then mb=$(awk "BEGIN{printf \"%.1f\", $num*1024}");
      elif [[ "$used" == *MiB* ]]; then mb="$num";
      elif [[ "$used" == *KiB* ]]; then mb=$(awk "BEGIN{printf \"%.3f\", $num/1024}");
      else mb="$num"; fi
      echo "$ts,$name,$cpu_v,$mb,${mempct/\%/}" >> "$OUT"
    done
  sleep 1
done
