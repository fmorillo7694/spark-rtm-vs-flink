#!/usr/bin/env bash
# Fault-tolerance test on EKS: deploy an engine WITH checkpointing + operator restart,
# start steady load, kill the executor/taskmanager pod mid-stream, and measure:
#   - recovery downtime (largest output stall)
#   - duplicate rate (at-least-once replay signature)
#   - whether the operator brought the job back to RUNNING
#
# Usage: eks/05_chaos.sh <spark-rtm-java|pyspark-rtm|flink|pyflink>
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE/.."
export AWS_PROFILE="${AWS_PROFILE:-default}" AWS_REGION=us-east-1
export KUBECONFIG="$HERE/kubeconfig"
source eks/.registry.env

ENGINE="$1"
LABEL="${ENGINE}-chaos"
PRODPODS="${PRODPODS:-4}"     # ~50k/s is enough to see recovery clearly without saturating
RATE="${RATE:-12500}"
mkdir -p results/eks

ACCOUNT="${ACCOUNT:-$(aws sts get-caller-identity --query Account --output text 2>/dev/null)}"
render() { sed -e "s#__REGISTRY__#$REGISTRY#g" -e "s#__TAG__#$TAG#g" \
               -e "s#__ENGINE__#$LABEL#g" -e "s#__PRODPODS__#$PRODPODS#g" \
               -e "s#__RATE__#$RATE#g" -e "s#__PAYLOAD__#0#g" \
               -e "s#__ACCOUNT__#$ACCOUNT#g" "$1"; }

echo "==> [1/6] Deploy $ENGINE (checkpointing + operator restart enabled)"
case "$ENGINE" in
  spark-rtm-java) render eks/jobs/spark-rtm.yaml | sed "s#__MAINCLASS__#bench.RtmPipelineJava#;s#__JAR__#local:///opt/spark/jars/java-rtm.jar#;s#__TYPE__#Java#;s#__CKPTNAME__#chaos-java#" | kubectl apply -f - ;;
  pyspark-rtm)    render eks/jobs/pyspark-rtm.yaml | kubectl apply -f - ;;
  flink)          render eks/jobs/flink-datastream.yaml | kubectl apply -f - ;;
  pyflink)        render eks/jobs/pyflink.yaml | kubectl apply -f - ;;
  *) echo "unknown $ENGINE"; exit 1 ;;
esac

echo "==> [2/6] Wait RUNNING"
case "$ENGINE" in
  spark-*|pyspark-*) NS=spark; KIND=sparkapplication; JP='{.items[0].status.applicationState.state}';;
  flink|pyflink)     NS=flink; KIND=flinkdeployment;  JP='{.items[0].status.jobStatus.state}';;
esac
for i in $(seq 1 50); do st=$(kubectl get $KIND -n $NS -o jsonpath="$JP" 2>/dev/null||true); echo "  $st"; [ "$st" = RUNNING ] && break; sleep 8; done
sleep 25

echo "==> [3/6] Start load + consumer"
kubectl -n kafka delete job latency-consumer producer --ignore-not-found >/dev/null 2>&1
render eks/jobs/load.yaml | sed 's/--warmup", "20"/--warmup", "0"/;s/--seconds", "150"/--seconds", "150"/' | kubectl apply -f -
sleep 40

echo "==> [4/6] CHAOS: kill an engine worker pod at $(date +%H:%M:%S)"
case "$ENGINE" in
  spark-*|pyspark-*) VICTIM=$(kubectl get pods -n spark --no-headers 2>/dev/null | grep -E 'exec-1' | awk '{print $1}' | head -1)
                     kubectl delete pod -n spark "$VICTIM" --grace-period=0 --force 2>/dev/null && echo "   killed executor $VICTIM" ;;
  flink|pyflink)     VICTIM=$(kubectl get pods -n flink --no-headers 2>/dev/null | grep -E 'taskmanager' | awk '{print $1}' | head -1)
                     kubectl delete pod -n flink "$VICTIM" --grace-period=0 --force 2>/dev/null && echo "   killed taskmanager $VICTIM" ;;
esac

echo "==> [5/6] Wait for consumer to finish, then analyze"
kubectl -n kafka wait --for=condition=complete job/latency-consumer --timeout=240s || \
  kubectl -n kafka wait --for=condition=failed job/latency-consumer --timeout=10s || true
kubectl -n kafka logs job/latency-consumer > results/eks/${LABEL}_consumer.log 2>&1 || true
# Pull the per-record CSV out of the consumer pod for duplicate/gap analysis
CPOD=$(kubectl get pods -n kafka -l app=latency-consumer -o name 2>/dev/null | head -1)
kubectl cp -n kafka "${CPOD#pod/}:/tmp/${LABEL}_latency.csv" "results/eks/${LABEL}_latency.csv" 2>/dev/null || true
.venv/bin/python bench/analyze_chaos.py "eks/${LABEL}" 2>/dev/null || \
  ( cp "results/eks/${LABEL}_latency.csv" "results/${LABEL}_latency.csv" 2>/dev/null && .venv/bin/python bench/analyze_chaos.py "${LABEL}" )

echo "==> [6/6] Did the operator recover the job?"
kubectl get $KIND -n $NS -o jsonpath="$JP" 2>/dev/null; echo
kubectl -n kafka delete job latency-consumer producer --ignore-not-found >/dev/null 2>&1
kubectl -n $NS delete $KIND --all --ignore-not-found >/dev/null 2>&1
echo "==> $LABEL done"
