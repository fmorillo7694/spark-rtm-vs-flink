#!/usr/bin/env bash
# One measured EKS run at 6 partitions / parallelism 6, identical resources per engine
# (1 coordinator 1c/2GB + 2 workers 3c/6144m pod-total). Fresh topics + fresh checkpoint
# path every run, durable S3 checkpoints, 60 s cadence by default, 5-min window.
#
# Usage: eks/04b_run_p6.sh <engine>
#   engines: spark-rtm-java spark-rtm-scala spark-rtm-syncckpt pyspark-rtm spark-microbatch
#            flink flink-sql pyflink
# Env: RUN_ID (label suffix -r<N>), TRIGGER_S (checkpoint cadence s, default 60),
#      RESULTS_DIR (default results/eks-4.3), RUN_SECONDS (300), WARMUP (60)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE/.."
export AWS_REGION=us-east-1 PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"
export KUBECONFIG="$HERE/kubeconfig"
source eks/.registry.env

ENGINE="$1"
TRIGGER_S="${TRIGGER_S:-60}"
LABEL="${ENGINE}-p6"; [ "$TRIGGER_S" != 60 ] && LABEL="${LABEL}-ckpt${TRIGGER_S}s"
LABEL="${LABEL}${RUN_ID:+-r$RUN_ID}"
OUT="${RESULTS_DIR:-results/eks-4.3}"
PRODPODS="${PRODPODS:-8}"; RATE="${RATE:-12500}"
RUN_SECONDS="${RUN_SECONDS:-300}"; WARMUP="${WARMUP:-60}"
CONSUMER_SECONDS=$((WARMUP+RUN_SECONDS)); PRODUCER_SECONDS=$((CONSUMER_SECONDS+20))
ACCOUNT="${ACCOUNT:-$(aws sts get-caller-identity --query Account --output text)}"
BUCKET="rtm-flink-bench-ckpt-${ACCOUNT}"
RUNSTAMP="$(date +%s)"; CKPT_SUFFIX="-p6-43-${RUNSTAMP}"
mkdir -p "$OUT"
echo "==> $LABEL: parallelism 6, 7 cores/14GB, ${TRIGGER_S}s ckpt, win=${RUN_SECONDS}s"

render() { sed -e "s#__REGISTRY__#$REGISTRY#g" -e "s#__TAG__#$TAG#g" \
  -e "s#__ENGINE__#$LABEL#g" -e "s#__PRODPODS__#$PRODPODS#g" -e "s#__RATE__#$RATE#g" \
  -e "s#__PAYLOAD__#0#g" -e "s#__ACCOUNT__#$ACCOUNT#g" \
  -e "s#__WARMUP__#$WARMUP#g" -e "s#__CONSUMER_SECONDS__#$CONSUMER_SECONDS#g" \
  -e "s#__PRODUCER_SECONDS__#$PRODUCER_SECONDS#g" \
  -e "s#\"60 seconds\"#\"${TRIGGER_S} seconds\"#g" \
  -e "s#checkpointing.interval: \"60 s\"#checkpointing.interval: \"${TRIGGER_S} s\"#g" \
  -e "s#/flink-java\"#/flink-java${CKPT_SUFFIX}\"#g" \
  -e "s#/flink-sql\"#/flink-sql${CKPT_SUFFIX}\"#g" \
  -e "s#/pyflink\"#/pyflink${CKPT_SUFFIX}\"#g" \
  -e "s#/pyspark-rtm\"#/pyspark-rtm${CKPT_SUFFIX}\"#g" "$1"; }
spark_app() {  # main-class jar type ckpt-name mode async
  render eks/jobs/p6/spark-rtm.yaml | sed -e "s#__MAINCLASS__#$1#" -e "s#__JAR__#$2#" -e "s#__TYPE__#$3#" \
    -e "s#__CKPTNAME__#$4${CKPT_SUFFIX}#" -e "s#__MODE__#$5#" -e "s#__ASYNC__#$6#"; }
JAVA="bench.RtmPipelineJava local:///opt/spark/jars/java-rtm.jar Java"
SCALA="RtmPipeline local:///opt/spark/jars/scala-rtm.jar Scala"

echo "==> [1/7] fresh 6-partition topics"
kubectl -n kafka delete job latency-consumer producer --ignore-not-found --wait=true >/dev/null 2>&1 || true
kubectl delete kafkatopic input-events output-events -n kafka --ignore-not-found >/dev/null 2>&1
for i in $(seq 1 30); do
  kubectl -n kafka exec bench-brokers-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null \
    | grep -qE '^(input|output)-events$' || break; sleep 3; done
kubectl apply -f eks/p6_kafka_strimzi.yaml >/dev/null
kubectl -n kafka wait kafkatopic/input-events kafkatopic/output-events --for=condition=Ready --timeout=120s >/dev/null

echo "==> [2/7] deploy $ENGINE"
case "$ENGINE" in
  spark-rtm-java)     spark_app $JAVA  spark-rtm-java     rtm        auto  | kubectl apply -f - ;;
  spark-rtm-scala)    spark_app $SCALA spark-rtm-scala    rtm        auto  | kubectl apply -f - ;;
  spark-rtm-syncckpt) spark_app $JAVA  spark-rtm-syncckpt rtm        false | kubectl apply -f - ;;
  spark-microbatch)   spark_app $JAVA  spark-microbatch   microbatch auto  | kubectl apply -f - ;;
  pyspark-rtm)        render eks/jobs/p6/pyspark-rtm.yaml | kubectl apply -f - ;;
  flink)              render eks/jobs/p6/flink-datastream.yaml | kubectl apply -f - ;;
  flink-sql)          render eks/jobs/p6/flink-sql.yaml | kubectl apply -f - ;;
  pyflink)            render eks/jobs/p6/pyflink.yaml | kubectl apply -f - ;;
  *) echo "unknown engine $ENGINE"; exit 1 ;;
esac
case "$ENGINE" in *spark*) KIND=spark ;; *) KIND=flink ;; esac

echo "==> [3/7] wait RUNNING"
st=""
for i in $(seq 1 60); do
  if [ $KIND = spark ]; then st=$(kubectl -n spark get sparkapplication rtm-bench -o jsonpath='{.status.applicationState.state}' 2>/dev/null || true)
  else st=$(kubectl -n flink get flinkdeployment flink-bench -o jsonpath='{.status.jobStatus.state}' 2>/dev/null || true); fi
  [ "$st" = RUNNING ] && break; sleep 8
done
echo "    state=$st"
[ "$st" = RUNNING ] || { echo "!! not RUNNING"; kubectl -n $KIND get pods; exit 1; }
sleep 30

echo "==> [4/7] load + cpu/mem sampler"
render eks/jobs/load.yaml | kubectl create -f - >/dev/null
( while true; do ts=$(date +%s); kubectl top pods -n $KIND --no-headers 2>/dev/null | sed "s/^/$ts $KIND /"; sleep 3; done ) > "$OUT/${LABEL}_top.txt" &
TOP=$!

echo "==> [5/7] wait for consumer (${CONSUMER_SECONDS}s)"
kubectl -n kafka wait --for=condition=complete job/latency-consumer --timeout=$((CONSUMER_SECONDS+180))s >/dev/null \
  || kubectl -n kafka wait --for=condition=failed job/latency-consumer --timeout=10s >/dev/null || true

echo "==> [6/7] evidence (engine still running)"
EV="$OUT/${LABEL}_evidence.txt"
set +e   # evidence is best effort; never abort the run here
{
  echo "# $LABEL evidence at $(date -u +%FT%TZ)  ckpt_suffix=$CKPT_SUFFIX trigger=${TRIGGER_S}s"
  if [ $KIND = spark ]; then
    echo "--- driver log lines"
    kubectl -n spark logs rtm-bench-driver 2>/dev/null | grep -E 'RtmPipeline|pyspark-native|asyncProgress|STARTED' | head -5
    DUMP=$(kubectl -n spark exec rtm-bench-driver -- bash -c 'P=$(jps -l | awk "/SparkSubmit/{print \$1;exit}"); jstack $P' 2>&1 || true)
    echo "async-log-write threads: $(grep -c '"async-log-write' <<<"$DUMP")"
    echo "AsyncProgressTrackingMicroBatchExecution frames: $(grep -c 'AsyncProgressTrackingMicroBatchExecution' <<<"$DUMP")"
    CK=$(aws s3 ls "s3://$BUCKET/" | awk '{print $2}' | grep -- "${CKPT_SUFFIX}/" | head -1)
    echo "--- checkpoint s3://$BUCKET/$CK (LastModified UTC, key)"
    for d in offsets commits; do
      echo "[$d]"
      aws s3api list-objects-v2 --bucket "$BUCKET" --prefix "${CK}$d/" \
        --query 'Contents[?!contains(Key, `.crc`) && !contains(Key, `/.`)].[LastModified,Key]' --output text
    done
    LAST=$(aws s3api list-objects-v2 --bucket "$BUCKET" --prefix "${CK}commits/" --query 'Contents[].Key' --output text | tr '\t' '\n' | grep -E '/[0-9]+$' | sort -t/ -k3 -n | tail -1)
    echo "--- latest commit log entry ($LAST)"
    [ -n "$LAST" ] && aws s3 cp "s3://$BUCKET/$LAST" - 2>/dev/null; echo
  else
    JID=$(kubectl -n flink get flinkdeployment flink-bench -o jsonpath='{.status.jobStatus.jobId}')
    JM=$(kubectl -n flink get pods -l component=jobmanager -o jsonpath='{.items[0].metadata.name}')
    kubectl -n flink exec "$JM" -- curl -s "localhost:8081/jobs/$JID/checkpoints" > "$OUT/${LABEL}_checkpoints.json" || true
    echo "jobId=$JID checkpoints: $(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["counts"])' "$OUT/${LABEL}_checkpoints.json" 2>&1)"
    echo "taskmanager pods: $(kubectl -n flink get pods -l component=taskmanager --no-headers | wc -l | tr -d ' ')"
  fi
} > "$EV" 2>&1
# never keep the account id in committed artifacts
sed -i '' "s/$ACCOUNT/__ACCOUNT__/g" "$EV" "$OUT/${LABEL}_checkpoints.json" 2>/dev/null
head -8 "$EV" | sed 's/^/    /'
kill $TOP 2>/dev/null
set -e

echo "==> [7/7] collect consumer output + cleanup"
LOG="$OUT/${LABEL}_consumer.log"
for try in 1 2 3 4 5; do
  POD=$(kubectl -n kafka get pods -l job-name=latency-consumer -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  [ -n "$POD" ] && kubectl -n kafka logs "$POD" > "$LOG.tmp" 2>/dev/null || true
  grep -q 'LATENCY SUMMARY' "$LOG.tmp" 2>/dev/null && break; sleep 5
done
grep '^HISTJSON ' "$LOG.tmp" | sed 's/^HISTJSON //' > "$OUT/${LABEL}_hist.json" || true
grep -v '^HISTJSON ' "$LOG.tmp" > "$LOG"; rm -f "$LOG.tmp"
.venv/bin/python - "$LOG" "$OUT/${LABEL}_summary.json" <<'PY'
import json,sys
txt=open(sys.argv[1]).read()
i=txt.find("LATENCY SUMMARY")
if i<0: print("  !! NO SUMMARY"); sys.exit(0)
j=txt.find("{",i); k=txt.find("\n}",j)
s=json.loads(txt[j:k+2]); json.dump(s,open(sys.argv[2],"w"),indent=2)
p,e=s["pipeline_ms"],s["e2e_ms"]
print(f"  SAVED: pipe p50={p['p50']} p99={p['p99']} max={p['max']} | e2e p50={e['p50']} p99={e['p99']} | recs={s['kept_records']} eps={s['throughput_eps']:.0f}")
PY
kubectl -n kafka delete job latency-consumer producer --ignore-not-found >/dev/null 2>&1 || true
kubectl -n spark delete sparkapplication --all --ignore-not-found --wait=true >/dev/null 2>&1 || true
kubectl -n flink delete flinkdeployment --all --ignore-not-found --wait=true >/dev/null 2>&1 || true
for i in $(seq 1 30); do [ -z "$(kubectl get pods -n $KIND -l 'spark-role in (driver,executor)' --no-headers 2>/dev/null; kubectl get pods -n flink -l app=flink-bench --no-headers 2>/dev/null)" ] && break; sleep 4; done
echo "==> $LABEL done"
