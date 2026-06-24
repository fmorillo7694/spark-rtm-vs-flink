#!/usr/bin/env bash
# Run one engine's EKS benchmark: deploy the engine, fan out the producer, measure
# latency (consumer Job) + Kafka consumer-group lag + pod CPU/mem, then clean up.
#
# Usage: eks/04_run_benchmark.sh <engine> [prodpods] [rate] [payload_bytes]
#   engine: spark-rtm-scala | spark-rtm-java | pyspark-rtm | flink | pyflink
# Env knobs (override defaults): PRODPODS, RATE, PAYLOAD_BYTES, RUN_SECONDS
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE/.."
export AWS_PROFILE="${AWS_PROFILE:-default}" AWS_REGION=us-east-1
export KUBECONFIG="$HERE/kubeconfig"
source eks/.registry.env   # REGISTRY, TAG

ENGINE="$1"
PRODPODS="${2:-${PRODPODS:-8}}"
RATE="${3:-${RATE:-12500}}"
PAYLOAD_BYTES="${4:-${PAYLOAD_BYTES:-0}}"
RUN_SECONDS="${RUN_SECONDS:-150}"
TAG="${TAG:-v2}"
LABEL="${LABEL:-$ENGINE}"
mkdir -p results/eks
echo "==> ENGINE=$ENGINE pods=$PRODPODS rate=$RATE/pod payload=${PAYLOAD_BYTES}B label=$LABEL"

# ACCOUNT defaults to the caller's account id (for the S3 checkpoint bucket name).
ACCOUNT="${ACCOUNT:-$(aws sts get-caller-identity --query Account --output text 2>/dev/null)}"
render() { sed -e "s#__REGISTRY__#$REGISTRY#g" -e "s#__TAG__#$TAG#g" \
               -e "s#__ENGINE__#$LABEL#g" -e "s#__PRODPODS__#$PRODPODS#g" \
               -e "s#__RATE__#$RATE#g" -e "s#__PAYLOAD__#$PAYLOAD_BYTES#g" \
               -e "s#__ACCOUNT__#$ACCOUNT#g" \
               -e "s#/flink-java\"#/flink-java${CKPT_SUFFIX}\"#g" \
               -e "s#/pyflink\"#/pyflink${CKPT_SUFFIX}\"#g" \
               -e "s#/pyspark-rtm\"#/pyspark-rtm${CKPT_SUFFIX}\"#g" "$1"; }

# ---- 0. unique checkpoint suffix per run, so a steady-state run always starts from a
#         clean checkpoint path (resuming a stale checkpoint fails with OffsetOutOfRange
#         once Kafka retention expires those offsets). Pass via RUNID; chaos uses its own.
RUNID="${RUNID:-$(date +%s)}"
export CKPT_SUFFIX="-${RUNID}"

# ---- 1. deploy engine ----
echo "==> [1/6] Deploy engine: $ENGINE"
case "$ENGINE" in
  spark-rtm-scala) render eks/jobs/spark-rtm.yaml | sed "s#__MAINCLASS__#RtmPipeline#;s#__JAR__#local:///opt/spark/jars/scala-rtm.jar#;s#__TYPE__#Scala#;s#__CKPTNAME__#spark-rtm-scala${CKPT_SUFFIX}#" | kubectl apply -f - ;;
  spark-rtm-java)  render eks/jobs/spark-rtm.yaml | sed "s#__MAINCLASS__#bench.RtmPipelineJava#;s#__JAR__#local:///opt/spark/jars/java-rtm.jar#;s#__TYPE__#Java#;s#__CKPTNAME__#spark-rtm-java${CKPT_SUFFIX}#" | kubectl apply -f - ;;
  pyspark-rtm)     render eks/jobs/pyspark-rtm.yaml | kubectl apply -f - ;;
  flink)           render eks/jobs/flink-datastream.yaml | kubectl apply -f - ;;
  pyflink)         render eks/jobs/pyflink.yaml | kubectl apply -f - ;;
  *) echo "unknown engine $ENGINE"; exit 1 ;;
esac

# ---- 2. wait until running ----
echo "==> [2/6] Wait for engine RUNNING"
case "$ENGINE" in
  spark-rtm-*|pyspark-rtm)
    for i in $(seq 1 40); do st=$(kubectl get sparkapplication -n spark -o jsonpath='{.items[0].status.applicationState.state}' 2>/dev/null||true); echo "  spark: ${st:-pending}"; [ "$st" = RUNNING ] && break; sleep 8; done ;;
  flink|pyflink)
    for i in $(seq 1 50); do st=$(kubectl get flinkdeployment -n flink -o jsonpath='{.items[0].status.jobStatus.state}' 2>/dev/null||true); echo "  flink: ${st:-pending}"; [ "$st" = RUNNING ] && break; sleep 8; done ;;
esac
echo "    warmup 30s"; sleep 30

# ---- 3. launch load ----
echo "==> [3/6] Launch consumer + $PRODPODS producers x $RATE/s (payload ${PAYLOAD_BYTES}B)"
kubectl -n kafka delete job latency-consumer producer --ignore-not-found --wait=true >/dev/null 2>&1
sleep 3
render eks/jobs/load.yaml | kubectl create -f -

# ---- 4. sample pod CPU/mem + Kafka lag during the run ----
echo "==> [4/6] Sample CPU/mem + Kafka consumer-group lag"
( for i in $(seq 1 70); do
    ts=$(date +%s)
    kubectl top pods -n spark --no-headers 2>/dev/null | sed "s/^/$ts spark /"
    kubectl top pods -n flink --no-headers 2>/dev/null | sed "s/^/$ts flink /"
    kubectl top pods -n kafka --no-headers 2>/dev/null | sed "s/^/$ts kafka /"
    sleep 3
  done ) > results/eks/${LABEL}_top.txt 2>/dev/null &
TOP=$!
# Backlog signal (source-agnostic: RTM/Flink Kafka sources don't commit consumer groups).
# Sample input & output topic total end-offsets over time. If the engine keeps up, output
# grows at input_rate * filter_pass(~0.33); a growing (input_delta - output_delta/0.33)
# gap = the engine falling behind. Columns: ts in_end out_end
( for i in $(seq 1 45); do
    ts=$(date +%s)
    ine=$(kubectl exec -n kafka bench-brokers-0 -- bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic input-events 2>/dev/null | awk -F: '{s+=$3} END{print s}')
    oute=$(kubectl exec -n kafka bench-brokers-0 -- bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic output-events 2>/dev/null | awk -F: '{s+=$3} END{print s}')
    echo "$ts ${ine:-0} ${oute:-0}"
    sleep 4
  done ) > results/eks/${LABEL}_lag.txt 2>/dev/null &
LAG=$!

# ---- 5. wait for consumer to finish ----
# Stream consumer logs to the host file FOR THE WHOLE RUN, so the LATENCY SUMMARY is
# captured as it's printed — robust to the pod being GC'd after completion.
echo "==> [5/6] Stream consumer logs + wait (~${RUN_SECONDS}s)"
( kubectl -n kafka logs -f job/latency-consumer > results/eks/${LABEL}_consumer.log 2>/dev/null ) &
LOGF=$!
kubectl -n kafka wait --for=condition=complete job/latency-consumer --timeout=$((RUN_SECONDS+120))s || \
  kubectl -n kafka wait --for=condition=failed job/latency-consumer --timeout=10s || true
sleep 3; kill $TOP $LAG $LOGF 2>/dev/null || true
# Fallback: only if the streamed log didn't capture the summary, try once more by pod.
if ! grep -q 'LATENCY SUMMARY' results/eks/${LABEL}_consumer.log 2>/dev/null; then
  CPOD=$(kubectl get pods -n kafka -l app=latency-consumer -o name 2>/dev/null | head -1)
  kubectl -n kafka logs "$CPOD" > results/eks/${LABEL}_consumer.log 2>&1 || true
fi
# Extract the LATENCY SUMMARY JSON into a per-engine, never-overwritten summary file.
.venv/bin/python - "$LABEL" <<'PYEOF'
import json, re, sys, os
label=sys.argv[1]; f=f"results/eks/{label}_consumer.log"
m=re.search(r'\{.*\}', open(f).read(), re.S) if os.path.exists(f) else None
if m:
    s=json.loads(m.group(0)); json.dump(s, open(f"results/eks/{label}_summary.json","w"), indent=2)
    p,e=s["pipeline_ms"],s["e2e_ms"]
    print(f"  SAVED {label}: pipe p50={p['p50']} p99={p['p99']} | e2e p99={e['p99']} | recs={s['kept_records']} eps={s['throughput_eps']:.0f}")
else:
    print(f"  !! {label}: NO SUMMARY JSON in consumer log (capture failed) — investigate before trusting")
PYEOF

# ---- 6. clean up engine + load ----
echo "==> [6/6] Tear down engine + load"
kubectl -n kafka delete job latency-consumer producer --ignore-not-found >/dev/null 2>&1
kubectl -n spark delete sparkapplication --all --ignore-not-found >/dev/null 2>&1
kubectl -n flink delete flinkdeployment --all --ignore-not-found >/dev/null 2>&1
echo "==> $ENGINE done. Artifacts: results/eks/${LABEL}_{consumer.log,top.txt,lag.txt}"
