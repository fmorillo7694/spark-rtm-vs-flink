#!/usr/bin/env bash
# Java-vs-Java at 6 partitions / parallelism 6, identical resources (7 cores / 14 GB each).
# Uses the p6/ manifests + 6-partition topics. 60s checkpoints, 5-min window.
# Usage: eks/04b_run_p6.sh <spark-rtm-java|flink>
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE/.."
export AWS_PROFILE="${AWS_PROFILE:-default}" AWS_REGION=us-east-1
export KUBECONFIG="$HERE/kubeconfig"
source eks/.registry.env

ENGINE="$1"; LABEL="${ENGINE}-p12eq"
PRODPODS="${PRODPODS:-8}"; RATE="${RATE:-12500}"
RUN_SECONDS="${RUN_SECONDS:-300}"; WARMUP="${WARMUP:-60}"
CONSUMER_SECONDS=$((WARMUP+RUN_SECONDS)); PRODUCER_SECONDS=$((CONSUMER_SECONDS+20))
ACCOUNT="${ACCOUNT:-$(aws sts get-caller-identity --query Account --output text)}"
RUNID="${RUNID:-$(date +%s)}"; export CKPT_SUFFIX="-p12eq-${RUNID}"
mkdir -p results/eks
echo "==> $ENGINE p6: parallelism 12, 13 cores/14GB each, 60s ckpt, win=${RUN_SECONDS}s"

render() { sed -e "s#__REGISTRY__#$REGISTRY#g" -e "s#__TAG__#$TAG#g" \
  -e "s#__ENGINE__#$LABEL#g" -e "s#__PRODPODS__#$PRODPODS#g" -e "s#__RATE__#$RATE#g" \
  -e "s#__PAYLOAD__#0#g" -e "s#__ACCOUNT__#$ACCOUNT#g" \
  -e "s#__WARMUP__#$WARMUP#g" -e "s#__CONSUMER_SECONDS__#$CONSUMER_SECONDS#g" \
  -e "s#__PRODUCER_SECONDS__#$PRODUCER_SECONDS#g" \
  -e "s#/flink-java\"#/flink-java${CKPT_SUFFIX}\"#g" \
  -e "s#/flink-sql\"#/flink-sql${CKPT_SUFFIX}\"#g" \
  -e "s#/pyflink\"#/pyflink${CKPT_SUFFIX}\"#g" \
  -e "s#/pyspark-rtm\"#/pyspark-rtm${CKPT_SUFFIX}\"#g" "$1"; }

echo "==> [1/6] reset topics to 12 partitions"
kubectl delete kafkatopic input-events output-events -n kafka --ignore-not-found >/dev/null 2>&1; sleep 6
kubectl apply -f eks/02_kafka_strimzi.yaml >/dev/null 2>&1; sleep 12

echo "==> [2/6] deploy $ENGINE (p12eq manifest, equal resources)"
case "$ENGINE" in
  spark-rtm-java)  render eks/jobs/p12eq/spark-rtm.yaml | sed "s#__MAINCLASS__#bench.RtmPipelineJava#;s#__JAR__#local:///opt/spark/jars/java-rtm.jar#;s#__TYPE__#Java#;s#__CKPTNAME__#spark-rtm-java${CKPT_SUFFIX}#" | kubectl apply -f - ;;
  spark-rtm-scala) render eks/jobs/p12eq/spark-rtm.yaml | sed "s#__MAINCLASS__#RtmPipeline#;s#__JAR__#local:///opt/spark/jars/scala-rtm.jar#;s#__TYPE__#Scala#;s#__CKPTNAME__#spark-rtm-scala${CKPT_SUFFIX}#" | kubectl apply -f - ;;
  pyspark-rtm)     render eks/jobs/p12eq/pyspark-rtm.yaml | kubectl apply -f - ;;
  flink)           render eks/jobs/p12eq/flink-datastream.yaml | kubectl apply -f - ;;
  flink-sql)       render eks/jobs/p12eq/flink-sql.yaml | kubectl apply -f - ;;
  pyflink)         render eks/jobs/p12eq/pyflink.yaml | kubectl apply -f - ;;
  *) echo "use spark-rtm-java|spark-rtm-scala|pyspark-rtm|flink|flink-sql|pyflink"; exit 1 ;;
esac

echo "==> [3/6] wait RUNNING"
case "$ENGINE" in
  spark-rtm-*|pyspark-rtm) for i in $(seq 1 45); do st=$(kubectl get sparkapplication -n spark -o jsonpath='{.items[0].status.applicationState.state}' 2>/dev/null||true); echo "  spark:${st:-pending}"; [ "$st" = RUNNING ]&&break; sleep 8; done ;;
  flink|flink-sql|pyflink) for i in $(seq 1 50); do st=$(kubectl get flinkdeployment -n flink -o jsonpath='{.items[0].status.jobStatus.state}' 2>/dev/null||true); echo "  flink:${st:-pending}"; [ "$st" = RUNNING ]&&break; sleep 8; done ;;
esac
sleep 30

echo "==> [4/6] load + samplers"
kubectl -n kafka delete job latency-consumer producer --ignore-not-found --wait=true >/dev/null 2>&1; sleep 3
render eks/jobs/load.yaml | kubectl create -f -
( for i in $(seq 1 140); do ts=$(date +%s); kubectl top pods -n spark --no-headers 2>/dev/null|sed "s/^/$ts spark /"; kubectl top pods -n flink --no-headers 2>/dev/null|sed "s/^/$ts flink /"; sleep 3; done ) > results/eks/${LABEL}_top.txt 2>/dev/null &
TOP=$!

echo "==> [5/6] stream logs + wait"
( kubectl -n kafka logs -f job/latency-consumer > results/eks/${LABEL}_consumer.log 2>/dev/null ) &
LOGF=$!
kubectl -n kafka wait --for=condition=complete job/latency-consumer --timeout=$((CONSUMER_SECONDS+150))s || kubectl -n kafka wait --for=condition=failed job/latency-consumer --timeout=10s || true
sleep 3; kill $TOP $LOGF 2>/dev/null || true
# Robust fallback: if the streamed log is empty (the -f attached before the pod was ready,
# a known race), re-pull logs directly from the (still-present) completed pod, with retries.
if [ ! -s results/eks/${LABEL}_consumer.log ] || ! grep -q pipeline_ms results/eks/${LABEL}_consumer.log; then
  echo "  (streamed log empty — re-pulling from consumer pod)"
  for try in 1 2 3 4 5; do
    POD=$(kubectl -n kafka get pods -l job-name=latency-consumer -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    [ -n "$POD" ] && kubectl -n kafka logs "$POD" > results/eks/${LABEL}_consumer.log 2>/dev/null
    grep -q pipeline_ms results/eks/${LABEL}_consumer.log 2>/dev/null && break
    sleep 5
  done
fi
.venv/bin/python - "$LABEL" <<'PY'
import json,re,sys,os
l=sys.argv[1]; f=f"results/eks/{l}_consumer.log"
m=re.search(r'\{.*\}',open(f).read(),re.S) if os.path.exists(f) else None
if m:
    s=json.loads(m.group(0)); json.dump(s,open(f"results/eks/{l}_summary.json","w"),indent=2)
    p,e=s["pipeline_ms"],s["e2e_ms"]
    print(f"  SAVED {l}: pipe p50={p['p50']} p99={p['p99']} | e2e p99={e['p99']} | recs={s['kept_records']} eps={s['throughput_eps']:.0f}")
else: print(f"  !! {l}: NO SUMMARY")
PY

echo "==> [6/6] cleanup engine"
kubectl -n kafka delete job latency-consumer producer --ignore-not-found >/dev/null 2>&1
kubectl -n spark delete sparkapplication --all --ignore-not-found >/dev/null 2>&1
kubectl -n flink delete flinkdeployment --all --ignore-not-found >/dev/null 2>&1
echo "==> $LABEL done"
