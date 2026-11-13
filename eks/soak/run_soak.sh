#!/usr/bin/env bash
# 4-hour concurrent soak: Java Spark RTM vs Java Flink DataStream, both at once on the same
# input, 6 partitions / 7 cores each, 60s durable S3 checkpoints. The soak consumers emit a
# latency snapshot every 30 min; this script ALSO samples kubectl-top CPU/mem for both engines
# every 30 min into a resource time-series. Total ~4h. Designed to run detached (nohup/&).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; cd "$HERE/../.."
export KUBECONFIG="$HERE/../kubeconfig" AWS_PROFILE="${AWS_PROFILE:-default}" AWS_REGION=us-east-1
ACCOUNT="${ACCOUNT:-$(aws sts get-caller-identity --query Account --output text)}"
source eks/.registry.env   # REGISTRY + TAG
RES=results/eks/soak; mkdir -p "$RES"
echo "==> soak start $(date) | REGISTRY=$REGISTRY TAG=$TAG ACCOUNT=$ACCOUNT"

render() { sed -e "s#__REGISTRY__#$REGISTRY#g" -e "s#__TAG__#$TAG#g" -e "s#__ACCOUNT__#$ACCOUNT#g" "$1"; }

echo "==> [1/5] clean slate + topics"
kubectl delete sparkapplication --all -n spark --ignore-not-found >/dev/null 2>&1
kubectl delete flinkdeployment --all -n flink --ignore-not-found >/dev/null 2>&1
kubectl delete job soak-producer soak-consumer-spark soak-consumer-flink -n kafka --ignore-not-found >/dev/null 2>&1
kubectl delete kafkatopic input-events output-spark output-flink -n kafka --ignore-not-found >/dev/null 2>&1
sleep 8
kubectl apply -f eks/soak/topics.yaml >/dev/null 2>&1; sleep 12

echo "==> [2/5] deploy BOTH engines (load image tag must have soak_consumer; spark/flink at __TAG__)"
render eks/soak/spark-soak.yaml | kubectl apply -f - >/dev/null 2>&1
render eks/soak/flink-soak.yaml | kubectl apply -f - >/dev/null 2>&1

echo "==> [3/5] wait both RUNNING"
for i in $(seq 1 60); do
  sst=$(kubectl get sparkapplication -n spark -o jsonpath='{.items[0].status.applicationState.state}' 2>/dev/null||true)
  fst=$(kubectl get flinkdeployment -n flink -o jsonpath='{.items[0].status.jobStatus.state}' 2>/dev/null||true)
  echo "  spark=${sst:-pending} flink=${fst:-pending}"
  [ "$sst" = RUNNING ] && [ "$fst" = RUNNING ] && break
  sleep 10
done
sleep 30

echo "==> [4/5] launch load (producer + 2 soak consumers)"
render eks/soak/load.yaml | kubectl apply -f - >/dev/null 2>&1

echo "==> [5/5] sample resources every 30 min for 2h (5 snapshots incl t=0)"
: > "$RES/resources.tsv"   # ts<TAB>ns<TAB>pod<TAB>cpu_m<TAB>mem_mi
for snap in $(seq 0 4); do
  ts=$(date +%s)
  for ns in spark flink; do
    kubectl top pods -n "$ns" --no-headers 2>/dev/null \
      | grep -E "rtm-bench|flink-bench" | awk -v t="$ts" -v n="$ns" '{print t"\t"n"\t"$1"\t"$2"\t"$3}' >> "$RES/resources.tsv"
  done
  echo "  [$(date +%H:%M)] resource snapshot $snap written"
  # also pull the latest soak snapshots from the consumer pods (so we have partial data live)
  for eng in spark flink; do
    pod=$(kubectl get pods -n kafka -l job-name=soak-consumer-$eng -o jsonpath='{.items[0].metadata.name}' 2>/dev/null||true)
    [ -n "$pod" ] && kubectl exec -n kafka "$pod" -- cat /tmp/${eng}_soak.jsonl 2>/dev/null > "$RES/${eng}_soak.jsonl" || true
  done
  [ "$snap" -lt 4 ] && sleep 1800   # 30 min
done

echo "==> soak done $(date). Pulling final snapshots."
for eng in spark flink; do
  pod=$(kubectl get pods -n kafka -l job-name=soak-consumer-$eng -o jsonpath='{.items[0].metadata.name}' 2>/dev/null||true)
  [ -n "$pod" ] && kubectl exec -n kafka "$pod" -- cat /tmp/${eng}_soak.jsonl 2>/dev/null > "$RES/${eng}_soak.jsonl" || true
  echo "  $eng: $(wc -l < "$RES/${eng}_soak.jsonl" 2>/dev/null || echo 0) windows"
done
echo "==> artifacts in $RES/"
