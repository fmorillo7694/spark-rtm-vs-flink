#!/usr/bin/env bash
# After a self-teardown, S3 holds the only copy of a cluster's results. Check it from the bucket
# listing alone (no download): every run directory has the files bench/analyze_final_run.py and the
# report need, the sampler files are non-empty, and the runner log was uploaded.
#
#   every run   summary.json rate.json hist.json consumer.log placement.txt ckpt_s3.json,
#               cadvisor.jsonl + jvm.jsonl non-empty
#   stateful    verify.json (run label not ending in -p6-rN)
#   Flink       rocksdb.jsonl non-empty (label starts with flink-)
#   (final run 2026-11-11: its stateless consumer, v43, wrote no rate.json; v43d does)
#   prefix      logs/runner.log, matrix_status.jsonl; EXPECT run directories (default 11)
#
# Usage: S3_PREFIX=final30/a [EXPECT=11] bash eks/stateful/verify_s3_results.sh   (exit 1 on any gap)
set -euo pipefail
export AWS_REGION="${AWS_REGION:-us-east-1}" PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"
PFX="${S3_PREFIX:?set S3_PREFIX, e.g. final30/a}"
EXPECT="${EXPECT:-11}"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
B="rtm-flink-bench-ckpt-${ACCOUNT}"
LS="$(aws s3 ls --recursive "s3://$B/$PFX/" | awk '{print $3, $4}')"   # "<size> <key>"
size() { awk -v k="$PFX/$1" '$2==k{print $1}' <<<"$LS"; }

gaps=0
gap() { echo "GAP $*"; gaps=$((gaps + 1)); }
runs=$(awk -v p="$PFX/runs/" 'index($2,p)==1{k=substr($2,length(p)+1); sub(/\/.*/,"",k); print k}' <<<"$LS" | sort -u)
n=0
for r in $runs; do
  n=$((n + 1))
  need="summary.json rate.json hist.json consumer.log placement.txt ckpt_s3.json"
  case "$r" in *-p6-r[0-9]*) ;; *) need="$need verify.json" ;; esac
  for f in $need; do [ -n "$(size "runs/$r/$f")" ] || gap "$r/$f missing"; done
  nonempty="cadvisor.jsonl jvm.jsonl"
  case "$r" in flink-*|pyflink-*) nonempty="$nonempty rocksdb.jsonl" ;; esac
  for f in $nonempty; do s=$(size "runs/$r/$f"); [ -n "$s" ] && [ "$s" -gt 0 ] || gap "$r/$f missing or empty"; done
done
[ "$n" -ge "$EXPECT" ] || gap "$n run directories, expected $EXPECT"
for f in logs/runner.log matrix_status.jsonl; do [ -n "$(size "$f")" ] || gap "$f missing"; done
echo "s3://$B/$PFX: $n runs, $(wc -l <<<"$LS" | tr -d ' ') objects, $gaps gaps"
[ "$gaps" = 0 ]
