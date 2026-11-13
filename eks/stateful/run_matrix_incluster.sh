#!/usr/bin/env bash
# In-cluster benchmark matrix runner. Runs INSIDE the EKS cluster as a Kubernetes Job
# (eks/stateful/runner-job.yaml), so a laptop losing network cannot kill the matrix.
#
#   - strictly one engine at a time: each run deletes every benchmark SparkApplication /
#     FlinkDeployment and load Job before it deploys its own;
#   - fresh topics + a fresh checkpoint path per run;
#   - a run that fails (engine never RUNNING, consumer failed, no/short summary) is retried up
#     to MAX_ATTEMPTS times; MAX_CONSEC_FAILS failed runs in a row stop the matrix;
#   - hard wall-clock budget: no run starts unless BUDGET_S minus elapsed leaves room for one
#     run (RUN_EST_S); the Job's activeDeadlineSeconds is the backstop kill;
#   - per-run artifacts and matrix_status.jsonl go to s3://$BUCKET/$S3_PREFIX/ after every run;
#     a run already in S3 is skipped, so re-creating the Job resumes the matrix.
#
# Plan items: "engine:workload:run[:knobs]". knobs = comma list of k=v, each changing one
# setting for that run and adding a label suffix (in this order):
#   rate=<per-pod evt/s>  -at<aggregate>k    ckpt=<s>   -ckpt<s>s     keys=<n>  -keys<n/1e6>m
#   hot=<fraction>        -hot<pct>          kill=<s>   -kill  (delete one worker pod <s> after
#                                                              the load starts; recovery test)
#   secs=<s>              -<s>s  measured load seconds after WARMUP (default RUN_SECONDS)
# A bare number as 4th field is a rate (session A labels). Stateless items use engine
# "sl-<engine>" and workload "p6" with the unchanged eks/jobs/p6 manifests (label
# <engine>-p6-r<run>, the eks/04b_run_p6.sh naming).
#
# DRY_RUN=1 renders every manifest into $WORK/rendered, validates it as YAML, prints the kubectl
# calls instead of running them, and prints the plan with an estimated duration per run and
# the runs a budget stop would cut (no cluster, no AWS). Used by eks/stateful/check.sh.
#
# Env (defaults in brackets):
#   REGISTRY TAG ACCOUNT (required unless DRY_RUN)   BUCKET [rtm-flink-bench-ckpt-$ACCOUNT]
#   STATELESS_TAG [TAG]  engine + load image tag for the stateless p6 arms (the published v43 images)
#   S3_PREFIX [stateful/<utc stamp>]  MANIFESTS [/etc/stateful]  WORK [/work]
#   PLAN [items; overrides PLAN_SET]  PLAN_SET [B]  A = session A matrix, B = session B,
#     C = session C joins (s4 stream-static, s5 stream-stream; results/eks-stateful/SESSION_C_PLAN.md)
#   PRODPODS [8] RATE [6250 per pod = 50k/s] KEYS [1000000] HOT [0] DUP [0.1] SEED [42]
#   RUN_SECONDS [300] WARMUP [60] CKPT_S [60] WATERMARK_S [5] WINDOW_S [10] GAP_MS [5000]
#   SL_RATE [12500 per pod = 100k/s, stateless]  SL_MIN_KEPT [9900000]  TRUTH_SAMPLE [100]
#   REF_ROWS [1000000, s4]  S5_RATE [3400 orders/s per pod, s5]
#   BUDGET_S [45000 = 12.5 h, session B as approved] RUN_EST_S [RUN_SECONDS+WARMUP+420] MAX_ATTEMPTS [2] MAX_CONSEC_FAILS [3]
#   MIN_KEPT [1000]
#   SELF_TEARDOWN [0; runner-job.yaml sets 1]  after MATRIX DONE/STOPPED or the Job deadline:
#     upload this pod's log to s3://.../logs/runner.log, then scale NODEGROUPS of CLUSTER to 0
#     (IRSA policy rtm-bench-runner-scale-down). CLUSTER [rtm-flink-bench4] NODEGROUPS [engine-ng bench-ng]
#   PLAN_SET D = session D scaling (SCALING_PLAN.md): knobs steps=R:S+R:S+... (per-pod rate
#     steps from producer start, producer --rate-steps; label -steps) and truth=<sample>; arms
#     flink-auto[-ro] flink-reactive flink-peak spark-rtm-peak spark-rtm-manual spark-mb-da run
#     on 24-partition topics, wait for <= START_NODES [2] engine nodes (Cluster Autoscaler) and
#     sample scaling.txt. D_STEPS D_SECS = the arms' profile. CA_DEPLOY [cluster-autoscaler]
#     (kube-system) is scaled to 0 by the self-teardown before the nodegroups.
#   ENGINE_NODES [dedicated for PLAN_SET B, shared for A]  dedicated = engine pods get a
#     nodeSelector + toleration for the tainted bench/role=engine nodes (eks/cluster.yaml
#     engine-ng) and one worker per node; every run records placement.txt
set -uo pipefail

DRY_RUN="${DRY_RUN:-0}"
MANIFESTS="${MANIFESTS:-/etc/stateful}"
WORK="${WORK:-/work}"
ACCOUNT="${ACCOUNT:-000000000000}"
REGISTRY="${REGISTRY:-registry.invalid}"; TAG="${TAG:-dev}"; STATELESS_TAG="${STATELESS_TAG:-$TAG}"
BUCKET="${BUCKET:-rtm-flink-bench-ckpt-${ACCOUNT}}"
S3_PREFIX="${S3_PREFIX:-stateful/$(date -u +%Y%m%dT%H%M%SZ)}"
RUNS="${RUNS:-2}"
# RATE = per-pod producer rate: 6250 x 8 pods = 50k evt/s. 100k evt/s (the stateless rate)
# saturates the stateful jobs on 7 cores (session A: Spark RTM S1 15 s p50 backlog at
# 6.0 cores, Flink p99 2.2 s at ~5 cores), so stateful arms run it only as probe cells.
PRODPODS="${PRODPODS:-8}"; RATE="${RATE:-6250}"; KEYS="${KEYS:-1000000}"
HOT="${HOT:-0}"; DUP="${DUP:-0.1}"; SEED="${SEED:-42}"
RUN_SECONDS="${RUN_SECONDS:-300}"; WARMUP="${WARMUP:-60}"; CKPT_S="${CKPT_S:-60}"
WATERMARK_S="${WATERMARK_S:-5}"; WINDOW_S="${WINDOW_S:-10}"; GAP_MS="${GAP_MS:-5000}"
SL_RATE="${SL_RATE:-12500}"; SL_MIN_KEPT="${SL_MIN_KEPT:-9900000}"; TRUTH_SAMPLE="${TRUTH_SAMPLE:-100}"
# Session C joins: S4 reference rows; S5 orders per pod (3400 x 8 = 27.2k orders/s + ~23k
# payments/s = ~50k input records/s across both topics, the S1-S3 base rate)
REF_ROWS="${REF_ROWS:-1000000}"; S5_RATE="${S5_RATE:-3400}"
BUDGET_S="${BUDGET_S:-45000}"; RUN_EST_S="${RUN_EST_S:-$((RUN_SECONDS + WARMUP + 420))}"   # final run A: 1260 (a 45-min Spark S3 run fits)
MAX_ATTEMPTS="${MAX_ATTEMPTS:-2}"; MAX_CONSEC_FAILS="${MAX_CONSEC_FAILS:-3}"; MIN_KEPT="${MIN_KEPT:-1000}"
# Session B pins engine pods to the tainted engine nodes; session A ran with shared nodes.
if [ "${PLAN_SET:-B}" = A ]; then ENGINE_NODES="${ENGINE_NODES:-shared}"; else ENGINE_NODES="${ENGINE_NODES:-dedicated}"; fi
# Stateless p6 arms (sl-*): the published runs had no dedicated nodes, so by default they get the
# toleration only (one shared 6-node pool, engine + load pods), never the engine-node pinning.
if [ "$ENGINE_NODES" = shared ]; then SL_NODES="${SL_NODES:-shared}"; else SL_NODES="${SL_NODES:-pool}"; fi
# Final run (PLAN_SET FA/FB): stateless engines pinned to the engine nodes too (load pods stay on
# bench-ng), so cAdvisor per-node numbers and engine CPU attribution match the stateful arms;
# and the measurement sampler (eks/stateful/sampler.py) on by default.
case "${PLAN_SET:-B}" in
  F*) SL_NODES="${SL_NODES_FINAL:-dedicated}"; MEASURE="${MEASURE:-1}" ;;
  *)  MEASURE="${MEASURE:-0}" ;;
esac
CONSUMER_SECONDS=$((WARMUP + RUN_SECONDS)); PRODUCER_SECONDS=$((CONSUMER_SECONDS + 20))
STARTED="${START_EPOCH:-$(date +%s)}"   # set by deploy_runner.sh so a pod restart keeps the budget clock
PY="${PY:-python3}"
CLUSTER="${CLUSTER:-rtm-flink-bench4}"; NODEGROUPS="${NODEGROUPS:-engine-ng bench-ng}"
CA_DEPLOY="${CA_DEPLOY:-cluster-autoscaler}"; START_NODES="${START_NODES:-2}"
# Session D load profile, per producer pod from producer start (x8 pods): 50k for 9 min (1 min
# warmup + 8 measured), 100k for 17 min, 50k for the last 20 measured min = 45 min measured.
# Shorter than SCALING_PLAN.md's 10 + 20 + 25 min so 10 arms + 3 smokes fit the 13 cluster-h cap.
D_STEPS="${D_STEPS:-6250:540+12500:1020+6250:1220}"; D_SECS="${D_SECS:-2700}"
mkdir -p "$WORK/runs" "$WORK/rendered"
STATUS="$WORK/matrix_status.jsonl"

# kubectl only falls back to in-cluster config when no global override flag is given; our
# --request-timeout counts as one and sends it to localhost:8080. So write an explicit
# kubeconfig that reads the (kubelet-rotated) service-account token file on every call.
SA=/var/run/secrets/kubernetes.io/serviceaccount
if [ "$DRY_RUN" != 1 ] && [ -z "${KUBECONFIG:-}" ] && [ -n "${KUBERNETES_SERVICE_HOST:-}" ] && [ -f "$SA/token" ]; then
  export KUBECONFIG="$WORK/kubeconfig"
  cat >"$KUBECONFIG" <<EOF
apiVersion: v1
kind: Config
clusters:
- name: in-cluster
  cluster:
    server: https://${KUBERNETES_SERVICE_HOST}:${KUBERNETES_SERVICE_PORT:-443}
    certificate-authority: $SA/ca.crt
users:
- name: runner
  user:
    tokenFile: $SA/token
contexts:
- name: in-cluster
  context: {cluster: in-cluster, user: runner, namespace: kafka}
current-context: in-cluster
EOF
fi

# ------------------------------------------------------------------------------- plans
# Both plans are priority-ordered so a budget stop only cuts the least important runs; within a
# round, Spark and Flink arms alternate so a partial stop still leaves comparable pairs.
plan_a() {  # session A (2026-11-08): flush 2x2 headline x3, other arms x2, 100k probe x1
  local hl="spark-rtm:s1 flink-ds:s1-update spark-rtm-sw1:s1 flink-ds-bt100:s1-update flink-sql:s1 flink-sql-bt100:s1
spark-rtm:s2 flink-ds:s2 spark-rtm-sw1:s2 flink-ds-bt100:s2 flink-sql:s2 flink-sql-bt100:s2
spark-rtm:s3 flink-ds:s3 spark-rtm-sw1:s3 flink-ds-bt100:s3" a i
  for i in 1 2 3; do for a in $hl; do echo "$a:$i"; done; done
  for i in $(seq 1 "$RUNS"); do for a in spark-mb:s1 flink-ds:s1-window flink-ds-eo:s2 flink-sql-eo:s2; do echo "$a:$i"; done; done
  for a in $hl; do echo "$a:1:12500"; done
}
plan_b() {  # session B: see results/eks-stateful/SESSION_B_PLAN.md
  local a i r k
  # P1: S2 with the fixed Flink dedup, default + tuned flush, DS + SQL, x3;
  #     Spark default S2 control x1 (run 4); fixed Flink S2 at 100k x1.
  # SQL uses s2-ttl60, not s2-rt: the rowtime keep-first operator holds each first row until the
  # watermark passes it (local smoke p50 5.2 s vs 6 ms), so s2-rt is a semantics arm, not a
  # latency arm. Both leak 0 duplicates in the stall test.
  local p1="flink-ds:s2-et flink-ds-bt100:s2-et flink-sql:s2-ttl60 flink-sql-bt100:s2-ttl60"
  for i in 1 2 3; do for a in $p1; do echo "$a:$i"; done; done
  echo "spark-rtm:s2:4"
  for a in $p1; do echo "$a:1:rate=12500"; done
  #     Vendor-tuned Spark (10 ms flush, no RocksDB row tracking) on S1/S2/S3 x3. S3 is the
  #     light-timer s3-lt, so it pairs with the P2 Flink s3-lt arms and with P2's default Spark.
  for i in 1 2 3; do for a in spark-rtm-vt:s1 spark-rtm-vt:s2 spark-rtm-vt:s3-lt; do echo "$a:$i"; done; done
  # P2: S3 light timer, Spark default + Flink tuned + Flink default, x3.
  for i in 1 2 3; do for a in spark-rtm:s3-lt flink-ds:s3-lt flink-ds-bt100:s3-lt; do echo "$a:$i"; done; done
  # P3: stateless leftovers (100k evt/s, published p6 setup), x3, + third Spark RTM Scala run.
  for i in 1 2 3; do for a in sl-flink sl-pyspark-rtm sl-flink-sql sl-spark-rtm-syncckpt sl-pyflink; do echo "$a:p6:$i"; done; done
  echo "sl-spark-rtm-scala:p6:3"
  # P4: gap tests, Spark default vs Flink tuned (approved order): recovery kill, hot keys,
  #     checkpoint 5 s / 30 s, 5-minute cadence. The 10M/50M key-cardinality runs were dropped
  #     (state is time-bounded, so key count does not grow it); the keys= knob stays available.
  for r in 1 2 3; do for a in spark-rtm:s1 flink-ds:s1-update; do echo "$a:$r:kill=180"; done; done
  for r in 1 2; do for a in spark-rtm:s1 flink-ds:s1-update; do echo "$a:$r:hot=0.1"; done; done
  for r in 1 2; do for k in 5 30; do for a in spark-rtm:s1 flink-ds:s1-update spark-rtm:s2 flink-ds:s2-et; do echo "$a:$r:ckpt=$k"; done; done; done
  #     5-minute cadence (Spark RTM trigger = Flink checkpoint interval = 300 s), 720 s measured
  #     so every run spans at least 2 batch/checkpoint boundaries.
  for r in 1 2; do for a in spark-rtm:s1 flink-ds:s1-update; do echo "$a:$r:ckpt=300,secs=720"; done; done
  # P5: third runs of session A's 2-run arms.
  for a in spark-mb:s1 flink-ds:s1-window flink-ds-eo:s2 flink-sql-eo:s2; do echo "$a:3"; done
}
plan_c() {  # session C, joins: see results/eks-stateful/SESSION_C_PLAN.md
  local a i
  # P1: S4 stream-static enrichment join, RTM vs Flink: Spark default, Flink default (100 ms),
  #     Flink tuned (0 ms), x3. No vendor-tuned Spark arm: S4 has no shuffle and no state store,
  #     so the 10 ms flush and trackTotalNumberOfRows=false change nothing.
  for i in 1 2 3; do for a in spark-rtm:s4 flink-ds-bt100:s4 flink-ds:s4; do echo "$a:$i"; done; done
  # P2: S5 stream-stream interval join: Flink default + tuned vs Spark MICRO-BATCH (RTM 4.3 has
  #     no stream-stream join), default trigger + 1 s trigger, x3. "What leaving RTM costs".
  for i in 1 2 3; do for a in flink-ds-bt100:s5 spark-mb:s5 flink-ds:s5 spark-mb1s:s5; do echo "$a:$i"; done; done
  # P3: capacity probes x1: S4 at 100k evt/s, S5 at 2x (54.4k orders/s, ~100k input records/s).
  for a in spark-rtm:s4 flink-ds-bt100:s4 flink-ds:s4; do echo "$a:1:rate=12500"; done
  for a in flink-ds-bt100:s5 spark-mb:s5 flink-ds:s5 spark-mb1s:s5; do echo "$a:1:rate=6800"; done
  # P4: recovery-truth check on EKS (session B lost the producers' TRUTHJSON): one worker kill per
  #     engine on S1, so lost / overcounted windows are measured before the publication run.
  for a in spark-rtm:s1 flink-ds:s1-update; do echo "$a:1:kill=180"; done
}
plan_d() {  # session D, scaling on S1 50k -> 100k -> 50k: see SCALING_PLAN.md (base design, no scale-in)
  local a k="steps=$D_STEPS,secs=$D_SECS,truth=$TRUTH_SAMPLE"
  # smokes first: (b) S4 Spark exact counts with the start gating (1f06ad2); (c) 30 min of the
  # Flink autoscaler in report-only mode (50k 5 min -> 100k 15 min -> 50k), recommendations only.
  # Smoke (a), the self-teardown, is a separate short runner Job before this one.
  echo "spark-rtm:s4:1"
  echo "flink-auto-ro:s1-update:1:steps=6250:300+12500:900+6250:620,secs=1740,truth=$TRUTH_SAMPLE"
  # round 1 = every arm once, Flink and Spark alternating; round 2 = the 2-run arms again
  for a in flink-auto:s1-update spark-rtm-manual:s1 flink-reactive:s1-update spark-mb-da:s1 flink-peak:s1-update spark-rtm-peak:s1; do
    echo "$a:1:$k"
  done
  for a in flink-auto:s1-update spark-rtm-manual:s1 flink-reactive:s1-update spark-mb-da:s1; do echo "$a:2:$k"; done
}
plan_d2() {  # session D, second runner Job: the two flink-reactive runs (r1/r2 failed in the first Job:
  # standalone JobManager could not load the job jar, fixed by #JMUSRLIB#), then the approved
  # scale-in profile 100k -> 25k (17 min incl. warmup, then 28 min) for the Flink autoscaler and
  # Reactive arms, one run each.
  local k="steps=$D_STEPS,secs=$D_SECS,truth=$TRUTH_SAMPLE" si="steps=12500:1020+3125:1760,secs=$D_SECS,truth=$TRUTH_SAMPLE"
  echo "flink-reactive:s1-update:1:$k"; echo "flink-reactive:s1-update:2:$k"
  echo "flink-auto:s1-update:1:$si"; echo "flink-reactive:s1-update:1:$si"
}
# Final lean run (FINAL_RUN_PLAN.md): the cells behind the post's headline tables and charts,
# 3 runs each at the 5-minute measured window of sessions A-C, on two clusters at once. Rounds:
# every cell once, then again (r2, r3), so a budget stop leaves complete rounds.
plan_fa() {  # cluster A: stateless p6 (6 cells), S2 2x2 + vendor + the two dedup variants (7), S4 (3); O7 last
  local a i
  for i in 1 2 3; do
    for a in sl-spark-rtm-java:p6 sl-flink:p6 sl-spark-rtm-scala:p6 sl-pyflink:p6 sl-pyspark-rtm:p6 sl-spark-rtm-syncckpt:p6 \
             spark-rtm:s2 flink-ds-bt100:s2-et spark-rtm-sw1:s2 flink-ds:s2-et spark-rtm-vt:s2 flink-sql:s2-ttl60 flink-ds:s2 \
             spark-rtm:s4 flink-ds-bt100:s4 flink-ds:s4; do echo "$a:$i"; done
  done
  # O7 (approved): S5 at 2000/pod, 1 run per arm, where each engine bends
  for a in spark-mb:s5 flink-ds-bt100:s5 spark-mb1s:s5 flink-ds:s5; do echo "$a:1:rate=2000"; done
}
plan_fb() {  # cluster B: S1 and S3 2x2 + vendor (10), S5 at 1000/pod (4), S1 recovery kill (2)
  local a i
  for i in 1 2 3; do
    for a in spark-rtm:s1 flink-ds-bt100:s1-update spark-rtm-sw1:s1 flink-ds:s1-update spark-rtm-vt:s1 \
             spark-rtm:s3 flink-ds-bt100:s3 spark-rtm-sw1:s3 flink-ds:s3 spark-rtm-vt:s3; do echo "$a:$i"; done
    for a in spark-mb:s5 flink-ds-bt100:s5 spark-mb1s:s5 flink-ds:s5; do echo "$a:$i:rate=1000"; done
    for a in spark-rtm:s1 flink-ds:s1-update; do echo "$a:$i:kill=180"; done
  done
}
plan_f30() {  # final run option A (FINAL30_PLAN.md): one workload's 2x2 + 3 stateless runs per cluster
  # $1 stateful workload (Spark name) $2 Flink workload name $3 run index of the stateless runs
  # $4 $5 $6 the three stateless engines. Stateful at the base 50k (rate=6250 per pod), stateless at
  # SL_RATE 100k, all 30 min measured. Round 1 then round 2, Spark and Flink alternating, one
  # stateless run after every two stateful runs.
  local s=$1 f=$2 r=$3 k="secs=1800"
  echo "spark-rtm:$s:1:$k"; echo "flink-ds-bt100:$f:1:$k"; echo "sl-$4:p6:$r:$k"
  echo "spark-rtm-vt:$s:1:$k"; echo "flink-ds:$f:1:$k"; echo "sl-$5:p6:$r:$k"
  echo "spark-rtm:$s:2:$k"; echo "flink-ds-bt100:$f:2:$k"; echo "sl-$6:p6:$r:$k"
  echo "spark-rtm-vt:$s:2:$k"; echo "flink-ds:$f:2:$k"
}
plan_f30a() { plan_f30 s1 s1-update 1 spark-rtm-java flink pyspark-rtm; }
plan_f30b() { plan_f30 s2 s2-et 1 spark-rtm-scala flink-sql pyflink; }
plan_f30c() { plan_f30 s3 s3 2 spark-rtm-java flink pyspark-rtm; }
plan_f30d() { plan_f30 s4 s4 2 spark-rtm-scala flink-sql pyflink; }
if [ -n "${PLAN:-}" ]; then read -r -a ITEMS <<<"$PLAN"
else
  ITEMS=()
  while IFS= read -r it; do ITEMS+=("$it"); done < <(case "${PLAN_SET:-B}" in (A) plan_a ;; (C) plan_c ;; (D) plan_d ;; (D2) plan_d2 ;; (FA) plan_fa ;; (FB) plan_fb ;; (F30A) plan_f30a ;; (F30B) plan_f30b ;; (F30C) plan_f30c ;; (F30D) plan_f30d ;; (*) plan_b ;; esac)
fi

parse_item() {  # item -> sets E WL RUN_I RUN_RATE RUN_CKPT_S RUN_KEYS RUN_HOT RUN_KILL RUN_TRUTH LABEL
  local knobs kv sfx=""
  IFS=: read -r E WL RUN_I knobs <<<"$1"
  RUN_RATE="$RATE"; RUN_CKPT_S="$CKPT_S"; RUN_KEYS="$KEYS"; RUN_HOT="$HOT"; RUN_KILL=0; RUN_TRUTH=0
  RUN_SECS="$RUN_SECONDS"; RUN_STEPS=""
  [ "${E#sl-}" != "$E" ] && RUN_RATE="$SL_RATE"
  [ "$WL" = s5 ] && RUN_RATE="$S5_RATE"   # orders/s per pod; payments add ~0.85x on top
  local base_rate=$RUN_RATE
  if [ -n "$knobs" ] && [ -z "${knobs//[0-9]/}" ]; then knobs="rate=$knobs"; fi
  local -a parts=()
  IFS=, read -r -a parts <<<"$knobs"
  for kv in ${parts[@]+"${parts[@]}"}; do
    case "${kv%%=*}" in
      rate) RUN_RATE=${kv#*=} ;; ckpt) RUN_CKPT_S=${kv#*=} ;; keys) RUN_KEYS=${kv#*=} ;;
      hot) RUN_HOT=${kv#*=} ;; kill) RUN_KILL=${kv#*=}; RUN_TRUTH="$TRUTH_SAMPLE" ;;
      secs) RUN_SECS=${kv#*=} ;;
      steps) RUN_STEPS=${kv#*=} ;;   # RATE:SECONDS+RATE:SECONDS+... per pod, from producer start
      truth) RUN_TRUTH=${kv#*=} ;;
      *) echo "!! unknown knob '$kv' in $1" >&2; return 1 ;;
    esac
  done
  # per-run load length (the consumer measures RUN_SECS after WARMUP; producers run 20 s longer)
  CONSUMER_SECONDS=$((WARMUP + RUN_SECS)); PRODUCER_SECONDS=$((CONSUMER_SECONDS + 20))
  # final run option A labels every stateful cell with its rate, base rate included (-at50k-1800s)
  { [ "$RUN_RATE" != "$base_rate" ] || [ "${PLAN_SET#F30}" != "${PLAN_SET:-}" ]; } && [ "${E#sl-}" = "$E" ] \
    && sfx="$sfx-at$((RUN_RATE * PRODPODS / 1000))k"
  [ "$RUN_CKPT_S" != "$CKPT_S" ] && sfx="$sfx-ckpt${RUN_CKPT_S}s"
  [ "$RUN_KEYS" != "$KEYS" ] && sfx="$sfx-keys$((RUN_KEYS / 1000000))m"
  [ "$RUN_HOT" != "$HOT" ] && sfx="$sfx-hot$(awk "BEGIN{printf \"%d\", $RUN_HOT * 100}")"
  [ "$RUN_KILL" != 0 ] && sfx="$sfx-kill"
  [ "$RUN_SECS" != "$RUN_SECONDS" ] && [ -z "$RUN_STEPS" ] && sfx="$sfx-${RUN_SECS}s"
  # -scalein: a profile that ends below the base rate (100k -> 25k), else -steps (50k -> 100k -> 50k)
  if [ -n "$RUN_STEPS" ]; then
    local last=${RUN_STEPS##*+}; last=${last%%:*}
    if [ "$last" -lt "$RATE" ]; then sfx="$sfx-scalein"; else sfx="$sfx-steps"; fi
  fi
  if [ "${E#sl-}" != "$E" ]; then LABEL="${E#sl-}-$WL-r$RUN_I"; else LABEL="$E-$WL$sfx-r$RUN_I"; fi
}

est_min() {  # rough duration per run (min), from session A (stateful) and the 4.3 stateless round,
  # plus the extra load time of a longer run (secs knob)
  local base
  case "$E:$WL" in
    sl-pyflink:*) base=10 ;; sl-*) base=9 ;; spark-*:s3*) base=12.2 ;; flink-*:s3*) base=8.1 ;;
    *:s4) base=9.4 ;;   # + ~1 min reference-table load per run
    *) base=8.4 ;;
  esac
  is_scaling "$E" && base=$(awk -v b="$base" 'BEGIN{print b + 3}')   # + waiting for CA scale-down
  awk -v b="$base" -v x="$((RUN_SECS - RUN_SECONDS))" 'BEGIN{print b + x / 60}'
}
run_need_s() {  # budget a run needs before it may start: RUN_EST_S plus any extra load time
  echo $((RUN_EST_S + RUN_SECS - RUN_SECONDS))
}

log() { echo "[$(date -u +%T)] $*"; }
elapsed() { echo $(( $(date +%s) - STARTED )); }
kc() {  # kubectl, or print it in dry-run
  if [ "$DRY_RUN" = 1 ]; then echo "    DRY kubectl $*" >&2; return 0; fi
  kubectl --request-timeout=60s "$@"
}
s3up() {  # local_dir s3_subprefix
  if [ "$DRY_RUN" = 1 ]; then echo "    DRY s3 sync $1 -> s3://$BUCKET/$S3_PREFIX/$2" >&2; return 0; fi
  for t in 1 2 3; do $PY /app/s3_sync.py "$1" "$BUCKET" "$S3_PREFIX/$2" && return 0; sleep $((t * 10)); done
  log "!! s3 upload failed for $1 (kept in $WORK)"; return 1
}
s3_has() {  # label -> 0 if its summary is already in S3
  [ "$DRY_RUN" = 1 ] && return 1
  $PY - "$BUCKET" "$S3_PREFIX/runs/$1/summary.json" <<'PY' 2>/dev/null
import sys, boto3
try:
    boto3.client("s3").head_object(Bucket=sys.argv[1], Key=sys.argv[2]); sys.exit(0)
except Exception:
    sys.exit(1)
PY
}

render() {  # file label workload ckptname extra-sed...   (stateful templates)
  local f=$1 label=$2 wl=$3 ck=$4 dup=0; shift 4
  # Duplicates only for S2 (dedup). In S1/S3 a late duplicate would be measured against the
  # window's/session's newer max ts and inflate the latency anchor (see STATEFUL_CONFIG.md).
  case "$wl" in s2*) dup="$DUP" ;; esac
  sed -e "s#__REGISTRY__#$REGISTRY#g" -e "s#__TAG__#$TAG#g" -e "s#__ACCOUNT__#$ACCOUNT#g" \
      -e "s#__WORKLOAD__#$wl#g" -e "s#__CKPTNAME__#$ck#g" -e "s#__CKPT_MS__#$((RUN_CKPT_S * 1000))#g" \
      -e "s#__TRIGGER__#$RUN_CKPT_S seconds#g" -e "s#__WATERMARK_S__#$WATERMARK_S#g" \
      -e "s#__WINDOW_S__#$WINDOW_S#g" -e "s#__GAP_MS__#$GAP_MS#g" \
      -e "s#__RUN__#$label#g" -e "s#__PRODPODS__#$PRODPODS#g" -e "s#__RATE__#$RUN_RATE#g" \
      -e "s#__PRODUCER_SECONDS__#$PRODUCER_SECONDS#g" -e "s#__CONSUMER_SECONDS__#$CONSUMER_SECONDS#g" \
      -e "s#__WARMUP__#$WARMUP#g" -e "s#__KEYS__#$RUN_KEYS#g" -e "s#__HOT__#$RUN_HOT#g" -e "s#__DUP__#$dup#g" \
      -e "s#__SEED__#$SEED#g" -e "s#__TRUTH__#$RUN_TRUTH#g" \
      -e "s#__SECOND_TOPIC__#$(second_topic "$wl")#g" -e "s#__REF_ROWS__#$REF_ROWS#g" \
      -e "s#__PROD_EXTRA__#$([ "$wl" = s5 ] && echo "--payments-topic st-payments")$([ -n "$RUN_STEPS" ] && echo " --rate-steps ${RUN_STEPS//+/,}")#g" \
      "$@" -e "s#__MB_TRIGGER__##g" "$MANIFESTS/$f"   # engine-specific seds ("$@") win
}
second_topic() {  # workload -> the join workloads' second topic
  case "$1" in s4) echo st-ref ;; s5) echo st-payments ;; esac
}
render_sl() {  # file label tag ckptsuffix extra-sed...  (stateless p6 manifests, eks/04b_run_p6.sh rules)
  local f=$1 label=$2 tag=$3 sfx=$4; shift 4
  sed -e "s#__REGISTRY__#$REGISTRY#g" -e "s#__TAG__#$tag#g" -e "s#__ACCOUNT__#$ACCOUNT#g" \
      -e "s#__ENGINE__#$label#g" -e "s#__PRODPODS__#$PRODPODS#g" -e "s#__RATE__#$RUN_RATE#g" \
      -e "s#__PAYLOAD__#0#g" -e "s#__WARMUP__#$WARMUP#g" -e "s#__CONSUMER_SECONDS__#$CONSUMER_SECONDS#g" \
      -e "s#__PRODUCER_SECONDS__#$PRODUCER_SECONDS#g" \
      -e "s#/flink-java\"#/flink-java${sfx}\"#g" -e "s#/flink-sql\"#/flink-sql${sfx}\"#g" \
      -e "s#/pyflink\"#/pyflink${sfx}\"#g" -e "s#/pyspark-rtm\"#/pyspark-rtm${sfx}\"#g" "$@" "$MANIFESTS/$f"
}
validate_yaml() {  # file: every document parses and no __PLACEHOLDER__ is left
  $PY - "$1" <<'PY'
import re, sys, yaml
text = open(sys.argv[1]).read()
left = sorted(set(re.findall(r"__[A-Z_]+__", text)))
if left:
    sys.exit(f"unrendered placeholders in {sys.argv[1]}: {left}")
docs = [d for d in yaml.safe_load_all(text) if d]
for d in docs:
    assert d.get("apiVersion") and d.get("kind") and d["metadata"].get("name"), sys.argv[1]
print(f"    ok {sys.argv[1].rsplit('/', 1)[-1]}: " + ", ".join(f"{d['kind']}/{d['metadata']['name']}" for d in docs))
PY
}

cleanup() {
  kc -n kafka delete job st-consumer st-producer st-ref-load latency-consumer producer --ignore-not-found --wait=true >/dev/null 2>&1
  kc -n spark delete sparkapplication -l bench=stateful --ignore-not-found --wait=true >/dev/null 2>&1
  kc -n spark delete sparkapplication rtm-bench --ignore-not-found --wait=true >/dev/null 2>&1
  kc -n flink delete flinkdeployment -l bench=stateful --ignore-not-found --wait=true >/dev/null 2>&1
  kc -n flink delete flinkdeployment flink-bench --ignore-not-found --wait=true >/dev/null 2>&1
  kc -n flink delete hpa flink-stateful-tm --ignore-not-found >/dev/null 2>&1
  [ "$DRY_RUN" = 1 ] && return 0
  for _ in $(seq 1 45); do
    [ -z "$(kubectl -n spark get pods -l spark-role --no-headers 2>/dev/null; kubectl -n flink get pods -l 'app in (flink-stateful,flink-bench)' --no-headers 2>/dev/null)" ] && return 0
    sleep 4
  done
  log "!! engine pods still terminating after 180 s"
}
self_teardown() {  # after the last run: keep the runner log in S3, then scale every nodegroup to 0
  # SELF_TEARDOWN=1 (runner-job.yaml) makes the matrix end independent of any agent: the nodes
  # stop billing as soon as the runner finishes. The cluster itself is deleted by the
  # EventBridge Scheduler deadline (eks/stateful/arm_deadline.sh) or by the operator.
  [ "${SELF_TEARDOWN:-0}" = 1 ] || return 0
  log "self-teardown: uploading runner log, scaling nodegroups ${NODEGROUPS} of ${CLUSTER} to 0"
  if [ "$DRY_RUN" = 1 ]; then
    echo "    DRY upload runner log; DRY eks update-nodegroup-config $CLUSTER {$NODEGROUPS} min=0 desired=0" >&2
    return 0
  fi
  mkdir -p "$WORK/logs"
  kubectl -n kafka logs "${HOSTNAME:-}" >"$WORK/logs/runner.log" 2>/dev/null && s3up "$WORK/logs" "logs" >/dev/null
  # Kafka broker volumes: `eksctl delete cluster` never deletes PVCs, so sessions A and B left
  # 6 x 100 GB EBS volumes behind. Delete the Kafka cluster and its PVCs while the nodes (and so
  # the EBS CSI controller) still run; the gp3 class reclaims with Delete, so the PVs and their
  # volumes go before the nodes are scaled away.
  log "self-teardown: deleting Kafka cluster bench and its PVCs"
  kubectl -n kafka delete kafka bench --ignore-not-found --wait=true --timeout=300s >/dev/null 2>&1
  kubectl -n kafka delete kafkanodepool brokers --ignore-not-found --wait=true --timeout=300s >/dev/null 2>&1
  kubectl -n kafka delete pvc -l strimzi.io/cluster=bench --ignore-not-found --wait=true --timeout=300s >/dev/null 2>&1
  for _ in $(seq 1 60); do   # PV (and EBS volume) deletion by the CSI controller, up to 5 min
    [ -z "$(kubectl get pv -o jsonpath='{range .items[?(@.spec.claimRef.namespace=="kafka")]}{.metadata.name}{"\n"}{end}' 2>/dev/null)" ] && break
    sleep 5
  done
  log "self-teardown: kafka PVs left: [$(kubectl get pv -o jsonpath='{range .items[?(@.spec.claimRef.namespace=="kafka")]}{.metadata.name} {end}' 2>/dev/null)]"
  # Session D: Cluster Autoscaler would scale engine-ng straight back up for any pending pod, and
  # an EKS nodegroup cannot have maxSize 0, so stop the autoscaler before the nodegroups go to 0.
  if kubectl -n kube-system get deployment "$CA_DEPLOY" >/dev/null 2>&1; then
    kubectl -n kube-system scale deployment "$CA_DEPLOY" --replicas=0 >/dev/null 2>&1 \
      && log "self-teardown: cluster autoscaler $CA_DEPLOY scaled to 0" || log "!! self-teardown: could not stop $CA_DEPLOY"
  fi
  # shellcheck disable=SC2086
  $PY - "$CLUSTER" $NODEGROUPS <<'PY'
import os, sys, boto3
eks, cluster = boto3.client("eks", region_name=os.environ.get("AWS_REGION", "us-east-1")), sys.argv[1]
for ng in sys.argv[2:]:
    sc = eks.describe_nodegroup(clusterName=cluster, nodegroupName=ng)["nodegroup"]["scalingConfig"]
    eks.update_nodegroup_config(clusterName=cluster, nodegroupName=ng,
                                scalingConfig={"minSize": 0, "desiredSize": 0, "maxSize": max(1, sc["maxSize"])})
    print(f"    scaled {cluster}/{ng} from desired {sc['desiredSize']} to 0", flush=True)
PY
}
FINAL=0   # set at MATRIX DONE / STOPPED, and by on_term at the budget end or on a stop marker
STOP_MARKER="${STOP_MARKER:-stateful-runner-stop}"
stop_requested() {  # explicit stop: the operator creates ConfigMap kafka/$STOP_MARKER, then deletes the Job
  [ "$DRY_RUN" = 1 ] && return 1
  kubectl --request-timeout=20s -n kafka get configmap "$STOP_MARKER" >/dev/null 2>&1
}
on_term() {  # SIGTERM/INT: tear down ONLY at the budget end (activeDeadlineSeconds) or on an
  # explicit stop. Any other TERM (Job deleted to change the budget, node drain, eviction) exits
  # without teardown: the Job restarts the pod, and runs already in S3 are skipped.
  if [ "$(elapsed)" -ge "$BUDGET_S" ] || stop_requested; then
    FINAL=1; log "TERM at the budget end or with the stop marker: self-teardown follows"
  else
    log "TERM before the budget end and no stop marker: exiting WITHOUT teardown"
  fi
  exit 143
}
trap 'log "exit: cleaning up"; cleanup; [ "$FINAL" = 1 ] && self_teardown' EXIT
trap on_term TERM INT

fresh_topics() {  # topics-file topic... : delete, wait until gone from the broker, re-apply
  local f=$1; shift
  local re t
  re="($(IFS='|'; echo "$*"))"
  kc -n kafka delete kafkatopic "$@" --ignore-not-found --wait=true >/dev/null 2>&1
  if [ "$DRY_RUN" != 1 ]; then
    for _ in $(seq 1 40); do
      kubectl -n kafka exec bench-brokers-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null \
        | grep -qxE "$re" || break
      sleep 3
    done
  fi
  kc apply -f "$MANIFESTS/$f" >/dev/null || return 1
  for t in "$@"; do kc -n kafka wait "kafkatopic/$t" --for=condition=Ready --timeout=180s >/dev/null || return 1; done
}

load_ref() {  # dir: write the S4 reference table to st-ref (ref-load.yaml) before the engine starts
  render ref-load.yaml "$LABEL" s4 none >"$WORK/rendered/$LABEL-ref.yaml"
  [ "$DRY_RUN" = 1 ] && validate_yaml "$WORK/rendered/$LABEL-ref.yaml"
  kc -n kafka delete job st-ref-load --ignore-not-found --wait=true >/dev/null 2>&1
  kc create -f "$WORK/rendered/$LABEL-ref.yaml" >/dev/null || return 1
  kc -n kafka wait --for=condition=complete job/st-ref-load --timeout=300s >/dev/null || return 1
  [ "$DRY_RUN" = 1 ] || kubectl -n kafka logs job/st-ref-load --tail=1 >"$1/ref_load.txt" 2>&1
  return 0
}

engine_manifest() {  # engine workload label ckptname -> path of rendered manifest
  local e=$1 wl=$2 label=$3 ck=$4 out="$WORK/rendered/$3.yaml"
  case "$e" in
    spark-rtm)    render spark-stateful.yaml "$label" "$wl" "$ck" -e "s#__MODE__#rtm#g" -e "s#__SHUFFLE_WAIT_MS__#50#g" -e "s#__TRACK_ROWS__#true#g" ;;
    spark-rtm-sw1) render spark-stateful.yaml "$label" "$wl" "$ck" -e "s#__MODE__#rtm#g" -e "s#__SHUFFLE_WAIT_MS__#1#g" -e "s#__TRACK_ROWS__#true#g" ;;
    # vendor-tuned (the Databricks latency-benchmarks settings that exist in OSS 4.3.0):
    # 10 ms shuffle flush + no RocksDB row tracking; everything else as spark-rtm
    spark-rtm-vt) render spark-stateful.yaml "$label" "$wl" "$ck" -e "s#__MODE__#rtm#g" -e "s#__SHUFFLE_WAIT_MS__#10#g" -e "s#__TRACK_ROWS__#false#g" ;;
    spark-mb)     render spark-stateful.yaml "$label" "$wl" "$ck" -e "s#__MODE__#microbatch#g" -e "s#__SHUFFLE_WAIT_MS__#50#g" -e "s#__TRACK_ROWS__#true#g" ;;
    # micro-batch with a 1 s processing-time trigger (session C s5)
    spark-mb1s)   render spark-stateful.yaml "$label" "$wl" "$ck" -e "s#__MODE__#microbatch#g" -e "s#__SHUFFLE_WAIT_MS__#50#g" -e "s#__TRACK_ROWS__#true#g" -e "s#__MB_TRIGGER__#1 second#g" ;;
    flink-ds)     render flink-stateful-datastream.yaml "$label" "$wl" "$ck" -e "s#__DELIVERY__#AT_LEAST_ONCE#g" -e "s#__BUFFER_TIMEOUT_MS__#0#g" ;;
    flink-ds-bt100) render flink-stateful-datastream.yaml "$label" "$wl" "$ck" -e "s#__DELIVERY__#AT_LEAST_ONCE#g" -e "s#__BUFFER_TIMEOUT_MS__#100#g" ;;
    flink-ds-eo)  render flink-stateful-datastream.yaml "$label" "$wl" "$ck" -e "s#__DELIVERY__#EXACTLY_ONCE#g" -e "s#__BUFFER_TIMEOUT_MS__#0#g" ;;
    flink-sql)    render flink-stateful-sql.yaml "$label" "$wl" "$ck" -e "s#__DELIVERY__#at-least-once#g" -e "s#__BUFFER_TIMEOUT_MS__#0#g" ;;
    flink-sql-bt100) render flink-stateful-sql.yaml "$label" "$wl" "$ck" -e "s#__DELIVERY__#at-least-once#g" -e "s#__BUFFER_TIMEOUT_MS__#100#g" ;;
    flink-sql-eo) render flink-stateful-sql.yaml "$label" "$wl" "$ck" -e "s#__DELIVERY__#exactly-once#g" -e "s#__BUFFER_TIMEOUT_MS__#0#g" ;;
    # Session D scaling arms (flink-scaling.yaml / spark-scaling.yaml, SCALING_PLAN.md): S1 on
    # 24-partition topics, latency-tuned flush (Flink 0 ms, Spark 50 ms = the main-matrix defaults)
    flink-auto|flink-auto-ro|flink-reactive|flink-peak)
      local fmode=native sk=jobmanager.scheduler sv=adaptive auto=true act=true par=6 tms=2
      case "$e" in
        flink-auto-ro) act=false ;;
        flink-reactive) fmode=standalone; sk=scheduler-mode; sv=reactive; auto=false ;;
        flink-peak) sv=default; auto=false; par=12; tms=4 ;;
      esac
      # native: the JobManager sizes the TaskManagers from the parallelism, no replicas line
      local -a norep=(-e "s/^#JMUSRLIB#//")   # standalone: jar copy into the JM's user-lib-dir
      [ "$fmode" = native ] && norep=(-e "/replicas: __TM_REPLICAS__/d")
      render flink-scaling.yaml "$label" "$wl" "$ck" -e "s#__DELIVERY__#AT_LEAST_ONCE#g" -e "s#__BUFFER_TIMEOUT_MS__#0#g" \
        -e "s#__FLINK_MODE__#$fmode#g" -e "s#__SCHED_KEY__#$sk#g" -e "s#__SCHED_VAL__#$sv#g" \
        -e "s#__AUTOSCALER__#$auto#g" -e "s#__AUTOSCALER_ACT__#$act#g" -e "s#__PARALLELISM__#$par#g" \
        ${norep[@]+"${norep[@]}"} -e "s#__TM_REPLICAS__#$tms#g" ;;
    spark-rtm-peak|spark-rtm-manual|spark-mb-da)
      # SPARK_SHAPE (spark-rtm-manual): low = 2 x 18 slots, peak = 4 x 9; 36 slots either way
      local smode=rtm inst=4 slots=9 da=false
      [ "$e" = spark-rtm-manual ] && [ "${SPARK_SHAPE:-low}" = low ] && { inst=2; slots=18; }
      [ "$e" = spark-mb-da ] && { smode=microbatch; inst=2; slots=3; da=true; }
      render spark-scaling.yaml "$label" "$wl" "$ck" -e "s#__MODE__#$smode#g" \
        -e "s#__EXEC_INSTANCES__#$inst#g" -e "s#__EXEC_SLOTS__#$slots#g" -e "s#__DA__#$da#g" \
        -e "s#__DA_MIN__#1#g" -e "s#__DA_MAX__#4#g" ;;
    *) return 1 ;;
  esac | place "$ENGINE_NODES" >"$out" || return 1
  echo "$out"
}

place() {  # mode; stdin manifest -> stdout. Injected at render time so the p6/stateful manifest
  # files stay unchanged.
  #   dedicated  engine pods: nodeSelector + toleration for the tainted bench/role=engine nodes
  #              (eks/cluster.yaml engine-ng), the two workers one per node (stateful arms)
  #   pool       toleration only, on engine AND load pods: all 6 nodes are one shared pool for
  #              the scheduler, as on the untainted 6-node cluster of the published stateless
  #              runs (stateless p6 arms)
  #   shared     unchanged manifest (session A reproduction)
  case "$1" in dedicated|pool) ;; *) cat; return ;; esac
  awk -v mode="$1" '
    function pin(ind) {
      if (mode == "dedicated") print ind "nodeSelector: { bench/role: engine }"
      print ind "tolerations: [{ key: bench/role, operator: Equal, value: engine, effect: NoSchedule }]"
    }
    function spread(ind, lk, lv) {
      print ind "affinity:"
      print ind "  podAntiAffinity:"
      print ind "    requiredDuringSchedulingIgnoredDuringExecution:"
      print ind "      - labelSelector: { matchLabels: { " lk ": " lv " } }"
      print ind "        topologyKey: kubernetes.io/hostname"
    }
    { print }
    /^  (podTemplate|template):$/ { pt = 1; next }
    pt && /^    spec:$/ { pin("      "); pt = 0; next }
    /^  driver:$/ { pin("    ") }
    /^  executor:$/ { pin("    "); if (mode == "dedicated") spread("    ", "spark-role", "executor") }
    mode == "dedicated" && /^  taskManager:$/ { print "    podTemplate:"; print "      spec:"; spread("        ", "component", "taskmanager") }
  '
}

placement() {  # kind dir: which node each engine / kafka-namespace pod is on; flags sharing
  [ "$DRY_RUN" = 1 ] && return 0
  local q='{range .items[*]}{.spec.nodeName}{" "}{.metadata.name}{"\n"}{end}' shared sel=component
  [ "$1" = spark ] && sel=spark-role   # engine pods only, not the operator pods in the same namespace
  kubectl -n "$1" get pods -l "$sel" -o jsonpath="$q" 2>/dev/null | sed "s/^/$1 /" >"$2/placement.txt"
  kubectl -n kafka get pods -o jsonpath="$q" 2>/dev/null | sed 's/^/kafka /' >>"$2/placement.txt"
  shared=$(awk '$1 != "kafka" {e[$2] = 1} $1 == "kafka" && ($2 in e) {print $2}' "$2/placement.txt" | sort -u | tr '\n' ' ')
  echo "engine_nodes=$3 shared_with_kafka_ns=[${shared}]" >>"$2/placement.txt"
  [ -n "$shared" ] && [ "$3" = dedicated ] && log "    !! engine pods share nodes with kafka-namespace pods: $shared"
  return 0
}

sl_manifest() {  # stateless engine label ckptsuffix -> rendered manifest (eks/04b_run_p6.sh mapping)
  local e=${1#sl-} label=$2 sfx=$3 out="$WORK/rendered/$2.yaml"
  local java="bench.RtmPipelineJava local:///opt/spark/jars/java-rtm.jar Java"
  local scala="RtmPipeline local:///opt/spark/jars/scala-rtm.jar Scala"
  spark_app() {  # main jar type ckptname mode async
    render_sl p6-spark-rtm.yaml "$label" "$STATELESS_TAG" "$sfx" -e "s#__MAINCLASS__#$1#" -e "s#__JAR__#$2#" \
      -e "s#__TYPE__#$3#" -e "s#__CKPTNAME__#$4${sfx}#" -e "s#__MODE__#$5#" -e "s#__ASYNC__#$6#"; }
  case "$e" in
    spark-rtm-java)     spark_app $java  spark-rtm-java     rtm auto ;;
    spark-rtm-scala)    spark_app $scala spark-rtm-scala    rtm auto ;;
    spark-rtm-syncckpt) spark_app $java  spark-rtm-syncckpt rtm false ;;
    pyspark-rtm)        render_sl p6-pyspark-rtm.yaml "$label" "$STATELESS_TAG" "$sfx" ;;
    flink)              render_sl p6-flink-datastream.yaml "$label" "$STATELESS_TAG" "$sfx" ;;
    flink-sql)          render_sl p6-flink-sql.yaml "$label" "$STATELESS_TAG" "$sfx" ;;
    pyflink)            render_sl p6-pyflink.yaml "$label" "$STATELESS_TAG" "$sfx" ;;
    *) return 1 ;;
  esac | place "$SL_NODES" >"$out" || return 1
  echo "$out"
}

wait_running() {  # kind name
  [ "$DRY_RUN" = 1 ] && return 0
  local st=""
  for _ in $(seq 1 75); do   # up to 10 min (image pulls)
    if [ "$1" = spark ]; then st=$(kubectl -n spark get sparkapplication "$2" -o jsonpath='{.status.applicationState.state}' 2>/dev/null)
    else st=$(kubectl -n flink get flinkdeployment "$2" -o jsonpath='{.status.jobStatus.state}' 2>/dev/null); fi
    [ "$st" = RUNNING ] && return 0
    case "$st" in FAILED|SUBMISSION_FAILED) break ;; esac
    sleep 8
  done
  log "    engine state=$st"; return 1
}

http_json() {  # url -> stdout (python urllib, no curl in the image)
  $PY -c 'import sys,urllib.request;print(urllib.request.urlopen(sys.argv[1],timeout=15).read().decode())' "$1"
}

jstack_counts() {  # driver pod -> async-log-write / AsyncProgressTracking counts
  DUMP=$(kubectl -n spark exec "$1" -- bash -c 'P=$(jps -l | awk "/SparkSubmit/{print \$1;exit}"); jstack $P' 2>&1)
  echo "async-log-write threads: $(grep -c '"async-log-write' <<<"$DUMP")"
  echo "AsyncProgressTrackingMicroBatchExecution frames: $(grep -c 'AsyncProgressTrackingMicroBatchExecution' <<<"$DUMP")"
}

evidence() {  # kind dir
  local kind=$1 d=$2
  [ "$DRY_RUN" = 1 ] && { echo "dry-run" >"$d/evidence.txt"; return 0; }
  {
    echo "# evidence at $(date -u +%FT%TZ)"
    if [ "$kind" = spark ]; then
      jstack_counts stateful-bench-driver
      echo "--- active stages (pipelined: source + stateful stage RUNNING together)"
      APP=$(http_json "http://stateful-bench-ui-svc.spark.svc:4040/api/v1/applications" | $PY -c 'import sys,json;print(json.load(sys.stdin)[0]["id"])')
      http_json "http://stateful-bench-ui-svc.spark.svc:4040/api/v1/applications/$APP/stages?status=active" \
        | $PY -c 'import sys,json
for s in json.load(sys.stdin): print(s["stageId"], s["status"], s["numTasks"], s["numActiveTasks"], s["name"][:90])'
      echo "--- driver log: pipelined / commits / batch starts (boundary split input)"
      kubectl -n spark logs stateful-bench-driver >"$d/driver.raw" 2>/dev/null
      grep -E 'StatefulPipelineJava\]|concurrently with its running pipelined|Committed offsets for batch|SparkContext: Starting job:' "$d/driver.raw" | cut -c1-240
      # per-batch commit metrics (the Spark side of checkpoint duration/size), one JSON line per batch
      $PY "$MANIFESTS/spark_progress.py" <"$d/driver.raw" >"$d/progress.jsonl"
      echo "--- progress blocks: $(wc -l <"$d/progress.jsonl" | tr -d ' ')"
      rm -f "$d/driver.raw"
    else
      J=$(http_json "http://flink-stateful-rest.flink.svc:8081/jobs/overview" | $PY -c 'import sys,json;print([j["jid"] for j in json.load(sys.stdin)["jobs"] if j["state"]=="RUNNING"][0])')
      http_json "http://flink-stateful-rest.flink.svc:8081/jobs/$J/checkpoints" >"$d/checkpoints.json"
      echo "--- checkpoint config"; http_json "http://flink-stateful-rest.flink.svc:8081/jobs/$J/checkpoints/config"
      echo "--- counts"; $PY -c 'import sys,json;print(json.load(open(sys.argv[1]))["counts"])' "$d/checkpoints.json"
    fi
  } >"$d/evidence.txt" 2>&1
  sed -i "s/$ACCOUNT/__ACCOUNT__/g" "$d/evidence.txt" "$d/checkpoints.json" 2>/dev/null
}

s3_listing() {  # checkpoint prefix -> [offsets]/[commits] blocks (eks/04b_run_p6.sh format)
  $PY - "$BUCKET" "$1" <<'PY'
import sys, boto3
s3, b, pre = boto3.client("s3"), sys.argv[1], sys.argv[2].strip("/")
for d in ("offsets", "commits"):
    print(f"[{d}]")
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=f"{pre}/{d}/"):
        for o in page.get("Contents", []):
            if not o["Key"].endswith(".crc") and "/." not in o["Key"]:
                print(f"{o['LastModified'].isoformat()}\t{o['Key']}")
print("--- latest commit log entry")
PY
}

evidence_sl() {  # kind dir ckptprefix
  local kind=$1 d=$2 ck=$3
  [ "$DRY_RUN" = 1 ] && { echo "dry-run" >"$d/evidence.txt"; return 0; }
  {
    echo "# $LABEL evidence at $(date -u +%FT%TZ) ckpt=$ck"
    if [ "$kind" = spark ]; then
      echo "--- driver log lines"
      kubectl -n spark logs rtm-bench-driver 2>/dev/null | grep -E 'RtmPipeline|pyspark-native|asyncProgress|STARTED' | head -5
      jstack_counts rtm-bench-driver
      echo "--- checkpoint s3://$BUCKET/$ck (LastModified UTC, key)"
      s3_listing "$ck"
    else
      J=$(http_json "http://flink-bench-rest.flink.svc:8081/jobs/overview" | $PY -c 'import sys,json;print([j["jid"] for j in json.load(sys.stdin)["jobs"] if j["state"]=="RUNNING"][0])')
      http_json "http://flink-bench-rest.flink.svc:8081/jobs/$J/checkpoints" >"$d/checkpoints.json"
      echo "jobId=$J checkpoints: $($PY -c 'import sys,json;print(json.load(open(sys.argv[1]))["counts"])' "$d/checkpoints.json")"
      echo "taskmanager pods: $(kubectl -n flink get pods -l component=taskmanager --no-headers | wc -l | tr -d ' ')"
    fi
  } >"$d/evidence.txt" 2>&1
  sed -i "s/$ACCOUNT/__ACCOUNT__/g" "$d/evidence.txt" "$d/checkpoints.json" 2>/dev/null
}

collect() {  # consumer-job dir min-kept -> 0 if summary has >= min-kept records
  local job=$1 d=$2 min=$3
  [ "$DRY_RUN" = 1 ] && { echo '{"kept_records": 0, "dry_run": true}' >"$d/summary.json"; return 0; }
  POD=$(kubectl -n kafka get pods -l "job-name=$job" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  [ -n "$POD" ] && kubectl -n kafka logs "$POD" >"$d/consumer.raw" 2>/dev/null
  grep '^HISTJSON ' "$d/consumer.raw" 2>/dev/null | sed 's/^HISTJSON //' >"$d/hist.json"
  grep '^RATEJSON ' "$d/consumer.raw" 2>/dev/null | sed 's/^RATEJSON //' >"$d/rate.json"
  grep '^MAXJSON ' "$d/consumer.raw" 2>/dev/null | sed 's/^MAXJSON //' >"$d/maxcnt.json"
  grep -v -E '^(HISTJSON|RATEJSON|MAXJSON) ' "$d/consumer.raw" >"$d/consumer.log" 2>/dev/null; rm -f "$d/consumer.raw"
  find "$d" -maxdepth 1 -empty -type f -delete
  $PY - "$d/consumer.log" "$d/summary.json" "$min" <<'PY'
import json, sys
txt = open(sys.argv[1]).read()
i = txt.find("LATENCY SUMMARY")
if i < 0:
    print("    no summary"); sys.exit(1)
j = txt.find("{", i); k = txt.find("\n}", j)
s = json.loads(txt[j:k + 2]); json.dump(s, open(sys.argv[2], "w"), indent=2)
p = s["pipeline_ms"]
print(f"    kept={s['kept_records']} p50={p['p50']} p99={p['p99']} max={p['max']}")
sys.exit(0 if s["kept_records"] >= int(sys.argv[3]) else 1)
PY
}

topic_total() {  # topic -> sum of end offsets over partitions (record count; EO adds markers)
  kubectl -n kafka exec bench-brokers-0 -- /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic "$1" --time -1 2>/dev/null | awk -F: '{s+=$3} END {print s+0}'
}

verify() {  # workload dir: whole-run counts -> verify.json (+ truth.jsonl on recovery runs)
  local wl=$1 d=$2 out prev=-1 originals sent
  [ "$DRY_RUN" = 1 ] && { echo '{"dry_run": true}' >"$d/verify.json"; return 0; }
  kc -n kafka wait --for=condition=complete job/st-producer --timeout=180s >/dev/null || log "    producers not complete"
  # Whole producer logs, not --tail: the kubelet stores a line over 16 KiB as several partial
  # entries and --tail counts entries, so --tail=3 kept only fragments of TRUTHJSON (session B:
  # no truth.jsonl). A whole-log read returns the line rejoined (B's 3.4 MB MAXJSON line arrived intact).
  kubectl -n kafka logs -l job-name=st-producer --tail=-1 --max-log-requests=16 2>/dev/null >"$d/producers.raw"
  grep 'DONE sent=' "$d/producers.raw" >"$d/producers_done.txt"
  grep '^STEP ' "$d/producers.raw" | sort -k2,2n >"$d/steps.txt"   # session D: per-pod rate step times
  [ -s "$d/steps.txt" ] || rm -f "$d/steps.txt"
  grep '^TRUTHJSON ' "$d/producers.raw" | sed 's/^TRUTHJSON //' >"$d/truth.jsonl"; rm -f "$d/producers.raw"
  [ -s "$d/truth.jsonl" ] || rm -f "$d/truth.jsonl"
  originals=$(grep -oE 'originals=[0-9]+' "$d/producers_done.txt" | awk -F= '{s+=$2} END {print s+0}')
  sent=$(grep -oE 'DONE sent=[0-9]+' "$d/producers_done.txt" | awk -F= '{s+=$2} END {print s+0}')
  # Wait until the engine has drained: 3 equal output-offset samples in a row (20 s unchanged).
  # One 10 s sample is not enough: Spark RTM pauses ~7-10 s at every batch boundary, so a
  # single unchanged sample can land inside that pause and undercount (session B probe).
  local same=0
  for _ in $(seq 1 36); do
    out=$(topic_total st-output)
    if [ "$out" = "$prev" ]; then same=$((same + 1)); [ "$same" -ge 2 ] && break; else same=0; fi
    prev=$out; sleep 10
  done
  local payments=0; [ "$wl" = s5 ] && payments=$(topic_total st-payments)
  printf '{"workload":"%s","producer_pods_done":%s,"originals":%s,"sent":%s,"input_records":%s,"payments_records":%s,"output_records":%s}\n' \
    "$wl" "$(wc -l <"$d/producers_done.txt" | tr -d ' ')" "$originals" "$sent" "$(topic_total st-input)" "$payments" "$out" >"$d/verify.json"
  sed 's/^/    /' "$d/verify.json"
}

measure_start() {  # kind dir flink-rest-url -> MEAS_PID (empty when off): eks/stateful/sampler.py
  # (2 s cAdvisor CPU/memory/throttling per container + node root, 10 s JVM heap, Flink RocksDB)
  MEAS_PID=""
  [ "$MEASURE" = 1 ] || return 0
  if [ "$DRY_RUN" = 1 ]; then echo "    DRY sampler.py sample --kind $1 --flink-rest $3" >&2; return 0; fi
  $PY "$MANIFESTS/sampler.py" sample --out "$2" --kind "$1" --flink-rest "$3" --spark-pods spark-role \
    2>"$2/sampler.err" & MEAS_PID=$!
}
measure_stop() {  # pid dir ckpt-prefix: stop the sampler, then size the run's checkpoint prefix in S3
  [ "$MEASURE" = 1 ] || return 0
  if [ "$DRY_RUN" = 1 ]; then echo "    DRY sampler.py s3sum s3://$BUCKET/$3" >&2; return 0; fi
  [ -n "$1" ] && { kill -TERM "$1" 2>/dev/null; wait "$1" 2>/dev/null; }
  $PY "$MANIFESTS/sampler.py" s3sum --bucket "$BUCKET" --prefix "$3" --out "$2/ckpt_s3.json" 2>>"$2/sampler.err" \
    || log "    !! checkpoint S3 summary failed"
  log "    sampler: cadvisor $(wc -l <"$2/cadvisor.jsonl" 2>/dev/null | tr -d ' ') jvm $(wc -l <"$2/jvm.jsonl" 2>/dev/null | tr -d ' ') rocksdb $(wc -l <"$2/rocksdb.jsonl" 2>/dev/null | tr -d ' ') lines, errors $(grep -c '"err"' "$2/cadvisor.jsonl" "$2/jvm.jsonl" 2>/dev/null | awk -F: '{s+=$2} END {print s+0}')"
  find "$2" -maxdepth 1 -empty -type f -delete
}
sample_top() {  # kind dir: kubectl top every 5 s until killed
  while true; do
    ts=$(date +%s)
    kubectl top pods -n "$1" --no-headers 2>/dev/null | sed "s/^/$ts $1 /"   # bench/analyze_eks43.py format
    sleep 5
  done >"$2/top.txt"
}

kill_worker() {  # kind dir delay: delete one engine worker pod <delay> s after the load starts
  local kind=$1 d=$2 delay=$3 pod
  sleep "$delay"
  if [ "$kind" = spark ]; then
    pod=$(kubectl -n spark get pods -l spark-role=executor -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  else
    pod=$(kubectl -n flink get pods -l app=flink-stateful,component=taskmanager -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  fi
  [ -n "$pod" ] || { echo "0 no-worker-pod-found" >"$d/kill.txt"; return 1; }
  echo "$(date +%s) $pod" >"$d/kill.txt"
  kubectl -n "$kind" delete pod "$pod" --wait=false >/dev/null 2>&1
  log "    killed $kind worker $pod"
}

# ------------------------------------------------------------------ session D (scaling) helpers
is_scaling() {  # engine -> 0 for the session D arms (24-partition topics, Cluster Autoscaler nodes)
  case "$1" in flink-auto|flink-auto-ro|flink-reactive|flink-peak|spark-rtm-peak|spark-rtm-manual|spark-mb-da) return 0 ;; esac
  return 1
}
engine_nodes() {  # -> "<listed> <ready>" engine nodes (listed = registered, Ready or not)
  kubectl get nodes -l bench/role=engine --no-headers 2>/dev/null | awk '{n++; if ($2 ~ /^Ready/) r++} END {print n+0, r+0}'
}
wait_engine_nodes() {  # max timeout_s dir: let Cluster Autoscaler remove the previous arm's extra
  # nodes first, so every scaling arm starts from the same node count (recorded either way)
  local max=$1 t0 n
  [ "$DRY_RUN" = 1 ] && { echo "    DRY wait until <= $max engine nodes" >&2; return 0; }
  t0=$(date +%s)
  while :; do
    n=$(engine_nodes | awk '{print $1}')
    [ "$n" -le "$max" ] && break
    [ $(( $(date +%s) - t0 )) -ge "$2" ] && { log "    !! still $n engine nodes after $2 s, starting anyway"; break; }
    sleep 15
  done
  echo "start_engine_nodes=$n waited_s=$(( $(date +%s) - t0 ))" >"$3/nodes_start.txt"
}
sample_scaling() {  # kind dir: every 10 s engine nodes, worker pods by phase, Flink vertex
  # parallelism + job state, HPA replicas / CPU -> scaling.txt (bench/analyze_scaling_d.py)
  local kind=$1 sel ph fl hpa
  sel=spark-role=executor; [ "$kind" = flink ] && sel=component=taskmanager
  while true; do
    ph=$(kubectl -n "$kind" get pods -l "$sel" -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' 2>/dev/null \
      | awk '{c[$1]++} END {printf "running=%d pending=%d", c["Running"], c["Pending"]}')
    fl=""; hpa=""
    if [ "$kind" = flink ]; then
      fl=$($PY - 2>/dev/null <<'PY'
import json, urllib.request as u
b = "http://flink-stateful-rest.flink.svc:8081"
jobs = json.load(u.urlopen(b + "/jobs/overview", timeout=5))["jobs"]
j = [x for x in jobs if x["state"] not in ("FINISHED", "CANCELED", "FAILED")]
if j:
    d = json.load(u.urlopen(b + "/jobs/" + j[0]["jid"], timeout=5))
    print("job=" + d["state"] + " par=" + ",".join(str(v["parallelism"]) for v in d["vertices"]))
else:
    print("job=none")
PY
)
      hpa=$(kubectl -n flink get hpa flink-stateful-tm -o jsonpath='hpa={.status.currentReplicas}/{.status.desiredReplicas} cpu={.status.currentMetrics[0].resource.current.averageUtilization}' 2>/dev/null)
    fi
    echo "$(date +%s) nodes=$(engine_nodes | tr ' ' /) $ph ${fl:-job=?} $hpa"
    sleep 10
  done >"$2/scaling.txt"
}
spark_manual() {  # dir ck t_load: spark-rtm-manual restarts at every load step from the same
  # checkpoint, low shape for base rate, peak shape above it (the operator knows the schedule:
  # a best case for manual scaling). Times go to manual.txt; each driver's progress is kept.
  local d=$1 ck=$2 t_load=$3 off=0 k=0 step rate secs shape m
  local -a steps=()
  IFS=+ read -r -a steps <<<"$RUN_STEPS"
  for step in "${steps[@]}"; do
    rate=${step%%:*}; secs=${step#*:}; k=$((k + 1))
    if [ "$k" -gt 1 ]; then
      shape=low; [ "$rate" -gt "$RATE" ] && shape=peak
      sleep $(( t_load + off - $(date +%s) > 0 ? t_load + off - $(date +%s) : 0 ))
      echo "$(date +%s) stop step=$k rate=$rate shape=$shape" >>"$d/manual.txt"
      log "    manual restart $k: -> $shape (rate $rate)"
      kubectl -n spark logs stateful-bench-driver >"$d/driver-$((k - 1)).raw" 2>/dev/null
      $PY "$MANIFESTS/spark_progress.py" <"$d/driver-$((k - 1)).raw" >"$d/progress-$((k - 1)).jsonl" 2>/dev/null
      rm -f "$d/driver-$((k - 1)).raw"
      kubectl -n spark delete sparkapplication stateful-bench --wait=true --timeout=180s >/dev/null 2>&1
      for _ in $(seq 1 45); do
        [ -z "$(kubectl -n spark get pods -l spark-role --no-headers 2>/dev/null)" ] && break; sleep 2
      done
      echo "$(date +%s) gone step=$k" >>"$d/manual.txt"
      m=$(SPARK_SHAPE=$shape engine_manifest spark-rtm-manual "$WL" "$LABEL-step$k" "$ck") || return 1
      cp "$m" "$d/engine-step$k.yaml"
      kubectl apply -f "$m" >/dev/null || return 1
      wait_running spark stateful-bench && echo "$(date +%s) running step=$k" >>"$d/manual.txt"
      for _ in $(seq 1 60); do
        kubectl -n spark logs stateful-bench-driver 2>/dev/null | grep -q '\[StatefulPipelineJava\] workload=' && break
        sleep 5
      done
      echo "$(date +%s) query-started step=$k" >>"$d/manual.txt"
    fi
    off=$((off + secs))
  done
}
scaling_evidence() {  # kind dir: what the scaler decided (Flink events + autoscaler state, HPA)
  [ "$DRY_RUN" = 1 ] && return 0
  local d=$2
  if [ "$1" = flink ]; then
    kubectl -n flink get events --field-selector involvedObject.name=flink-stateful \
      -o jsonpath='{range .items[*]}{.lastTimestamp}{" "}{.reason}{" "}{.message}{"\n"}{end}' >"$d/flink_events.txt" 2>/dev/null
    kubectl -n flink get configmap autoscaler-flink-stateful -o yaml >"$d/autoscaler_cm.yaml" 2>/dev/null
    kubectl -n flink get flinkdeployment flink-stateful -o jsonpath='{.status}' >"$d/flinkdeployment_status.json" 2>/dev/null
    kubectl -n flink get hpa flink-stateful-tm -o yaml >"$d/hpa.yaml" 2>/dev/null
    sed -i "s/$ACCOUNT/__ACCOUNT__/g" "$d/autoscaler_cm.yaml" "$d/flinkdeployment_status.json" 2>/dev/null
  fi
  find "$d" -maxdepth 1 -empty -type f -delete
  return 0
}

one_run() {  # stateful run (E WL LABEL + RUN_* set by parse_item) -> 0 ok
  local e=$E wl=$WL label=$LABEL kind=flink iso=read_uncommitted d="$WORK/runs/$LABEL" ck m name=flink-stateful
  case "$e" in spark-*) kind=spark; name=stateful-bench ;; esac
  case "$e" in *-eo) iso=read_committed ;; esac
  rm -rf "$d"; mkdir -p "$d"
  ck="$label-$(date +%s)"
  cleanup
  local scaling=0; is_scaling "$e" && scaling=1
  case "$wl" in
    s4|s5) fresh_topics join-topics.yaml st-input st-output st-payments st-ref || { log "    topics not ready"; return 1; } ;;
    *) if [ "$scaling" = 1 ]; then fresh_topics scale-topics.yaml st-input st-output || { log "    topics not ready"; return 1; }
       else fresh_topics topics.yaml st-input st-output || { log "    topics not ready"; return 1; }; fi ;;
  esac
  [ "$scaling" = 1 ] && wait_engine_nodes "$START_NODES" 480 "$d"
  if [ "$wl" = s4 ]; then load_ref "$d" || { log "    reference table load failed"; return 1; }; fi
  m=$(engine_manifest "$e" "$wl" "$label" "$ck") || { log "    unknown engine $e"; return 1; }
  [ "$DRY_RUN" = 1 ] && validate_yaml "$m"
  cp "$m" "$d/engine.yaml"
  kc apply -f "$m" >/dev/null || return 1
  wait_running "$kind" "$name" || return 1
  [ "$DRY_RUN" = 1 ] || sleep 30
  # Spark resolves startingOffsets=latest only when query.start() runs, which for s4 comes after
  # the driver has read and counted the reference table. Start the load only once the driver has
  # logged the started query, or the first seconds of input are never read (session C, s4 r1:
  # 302,229 records short; r2/r3 exact).
  if [ "$kind" = spark ] && [ "$DRY_RUN" != 1 ]; then
    for _ in $(seq 1 60); do
      kubectl -n spark logs stateful-bench-driver 2>/dev/null | grep -q '\[StatefulPipelineJava\] workload=' && break
      sleep 5
    done
  fi
  if [ "$e" = flink-reactive ]; then
    kc apply -f "$MANIFESTS/hpa-reactive.yaml" >/dev/null || { log "    HPA not created"; return 1; }
    cp "$MANIFESTS/hpa-reactive.yaml" "$d/hpa-reactive.yaml"
  fi
  # A stepped run keeps ~190M latencies (~4.5 GB peak with the float64 percentile copies), over
  # the 4Gi consumer limit of the 5-min runs; a 30-min run at 50k keeps ~90M (~2.6 GB peak), so
  # long windows (>= 1200 s) get the same headroom.
  local -a mem=()
  { [ -n "$RUN_STEPS" ] || [ "$RUN_SECS" -ge 1200 ]; } \
    && mem=(-e "s#requests: { cpu: \"2\", memory: 2Gi }#requests: { cpu: \"2\", memory: 6Gi }#" \
            -e "s#limits:   { cpu: \"2\", memory: 4Gi }#limits:   { cpu: \"2\", memory: 10Gi }#")
  render load.yaml "$label" "$wl" "$ck" -e "s#__ISOLATION__#$iso#g" ${mem[@]+"${mem[@]}"} >"$WORK/rendered/$label-load.yaml"
  [ "$DRY_RUN" = 1 ] && validate_yaml "$WORK/rendered/$label-load.yaml"
  kc create -f "$WORK/rendered/$label-load.yaml" >/dev/null || return 1
  local top="" killer="" scal="" manual="" t_load meas=""
  t_load=$(date +%s); echo "$t_load" >"$d/load_start.txt"
  measure_start "$kind" "$d" http://flink-stateful-rest.flink.svc:8081; meas=$MEAS_PID
  if [ "$DRY_RUN" != 1 ]; then
    sample_top "$kind" "$d" & top=$!
    [ "$scaling" = 1 ] && { sample_scaling "$kind" "$d" & scal=$!; }
    [ "$e" = spark-rtm-manual ] && [ -n "$RUN_STEPS" ] && { spark_manual "$d" "$ck" "$t_load" & manual=$!; }
    if [ "$RUN_KILL" != 0 ]; then kill_worker "$kind" "$d" "$RUN_KILL" & killer=$!; fi
  else
    [ "$RUN_KILL" != 0 ] && echo "    DRY kill one $kind worker pod at load+${RUN_KILL}s" >&2
    [ "$e" = spark-rtm-manual ] && echo "    DRY manual restarts at steps $RUN_STEPS, e.g. peak shape:" >&2 \
      && { SPARK_SHAPE=peak engine_manifest spark-rtm-manual "$wl" "$label-step2" "$ck" >/dev/null && validate_yaml "$WORK/rendered/$label-step2.yaml"; }
  fi
  kc -n kafka wait --for=condition=complete job/st-consumer --timeout=$((CONSUMER_SECONDS + 240))s >/dev/null \
    || log "    consumer did not complete"
  [ -n "$top" ] && kill "$top" 2>/dev/null
  measure_stop "$meas" "$d" "stateful/$ck"
  [ -n "$killer" ] && wait "$killer"
  [ -n "$manual" ] && { wait "$manual" || log "    !! manual restart loop failed"; }
  placement "$kind" "$d" "$ENGINE_NODES"
  evidence "$kind" "$d"
  [ "$scaling" = 1 ] && scaling_evidence "$kind" "$d"
  verify "$wl" "$d"
  collect st-consumer "$d" "$MIN_KEPT"; local rc=$?
  [ -n "$scal" ] && kill "$scal" 2>/dev/null
  return $rc
}

one_run_sl() {  # stateless p6 run, eks/04b_run_p6.sh steps, in-cluster -> 0 ok
  local e=$E label=$LABEL kind=flink name=flink-bench d="$WORK/runs/$LABEL" m sfx ck
  case "${e#sl-}" in *spark*) kind=spark; name=rtm-bench ;; esac
  rm -rf "$d"; mkdir -p "$d"
  sfx="-p6-43b-$(date +%s)"
  case "${e#sl-}" in
    spark-rtm-java|spark-rtm-scala|spark-rtm-syncckpt) ck="${e#sl-}${sfx}" ;;
    pyspark-rtm) ck="pyspark-rtm${sfx}" ;; flink) ck="flink-java${sfx}" ;; *) ck="${e#sl-}${sfx}" ;;
  esac
  cleanup
  fresh_topics p6-topics.yaml input-events output-events || { log "    topics not ready"; return 1; }
  m=$(sl_manifest "$e" "$label" "$sfx") || { log "    unknown stateless engine $e"; return 1; }
  [ "$DRY_RUN" = 1 ] && validate_yaml "$m"
  cp "$m" "$d/engine.yaml"
  kc apply -f "$m" >/dev/null || return 1
  wait_running "$kind" "$name" || return 1
  [ "$DRY_RUN" = 1 ] || sleep 30
  # load image too at STATELESS_TAG: the published stateless consumer, not the stateful one.
  # dedicated: the engine is pinned, the load pods stay unpinned on bench-ng (place() would pin
  # the Job's pod template to the engine nodes)
  local lmode=$SL_NODES; [ "$lmode" = dedicated ] && lmode=shared
  # Long windows (>= 1200 s, final run option A): the published v43 consumer keeps every latency
  # in Python lists and writes a raw CSV of every record, which a 30-min run at ~33k out/s turns
  # into ~60M records (OOM at 2Gi, ~5 GB of CSV). Only the CONSUMER switches to this run's load
  # image (float32 arrays, --no-raw) with 6/10Gi; producers and engines stay on STATELESS_TAG.
  local -a lcons=()
  [ "$RUN_SECS" -ge 1200 ] && lcons=(-e "/name: latency-consumer/,/^---/ s#rtm-bench/load:$STATELESS_TAG\"#rtm-bench/load:$TAG\"#" \
    -e 's#"--outdir", "/tmp", "--hist"\]#"--outdir", "/tmp", "--hist", "--no-raw"]#' \
    -e 's#requests: { cpu: "1", memory: 1Gi }#requests: { cpu: "1", memory: 6Gi }#' \
    -e 's#limits:   { cpu: "2", memory: 2Gi }#limits:   { cpu: "2", memory: 10Gi }#')
  render_sl p6-load.yaml "$label" "$STATELESS_TAG" "$sfx" ${lcons[@]+"${lcons[@]}"} | place "$lmode" >"$WORK/rendered/$label-load.yaml"
  [ "$DRY_RUN" = 1 ] && validate_yaml "$WORK/rendered/$label-load.yaml"
  kc create -f "$WORK/rendered/$label-load.yaml" >/dev/null || return 1
  local top="" meas=""
  measure_start "$kind" "$d" http://flink-bench-rest.flink.svc:8081; meas=$MEAS_PID
  if [ "$DRY_RUN" != 1 ]; then sample_top "$kind" "$d" & top=$!; fi
  kc -n kafka wait --for=condition=complete job/latency-consumer --timeout=$((CONSUMER_SECONDS + 180))s >/dev/null \
    || log "    consumer did not complete"
  [ -n "$top" ] && kill "$top" 2>/dev/null
  measure_stop "$meas" "$d" "$ck"
  placement "$kind" "$d" "$SL_NODES"
  evidence_sl "$kind" "$d" "$ck"
  collect latency-consumer "$d" "$SL_MIN_KEPT"
}

# ------------------------------------------------------------------------------- main
log "matrix: ${#ITEMS[@]} runs (plan ${PLAN:+custom}${PLAN:-${PLAN_SET:-B}}), budget ${BUDGET_S}s, s3://$BUCKET/$S3_PREFIX dry_run=$DRY_RUN"
if [ "$DRY_RUN" = 1 ]; then  # the plan with estimates, and what a budget stop would cut
  cum=0; n=0
  for item in "${ITEMS[@]}"; do
    parse_item "$item" || exit 1
    n=$((n + 1)); est=$(est_min)
    cut=$(awk -v c="$cum" -v b="$BUDGET_S" -v need="$(run_need_s)" 'BEGIN{print (c > (b - need) / 60) ? "  CUT-BY-BUDGET" : ""}')
    # the runner stops at the first run that does not fit, so everything after it is cut too
    [ -n "${stopped:-}" ] && cut="  CUT-BY-BUDGET"; [ -n "$cut" ] && stopped=1
    printf 'PLAN %3d %-44s rate=%-5s ckpt=%-3s keys=%-9s hot=%-4s kill=%-3s secs=%-4s est=%4.1fmin start=%5.1fh%s\n' \
      "$n" "$LABEL" "$RUN_RATE" "$RUN_CKPT_S" "$RUN_KEYS" "$RUN_HOT" "$RUN_KILL" "$RUN_SECS" "$est" "$(awk -v c="$cum" 'BEGIN{print c/60}')" "$cut"
    cum=$(awk "BEGIN{print $cum + $est}")
  done
  tot_h=$(awk -v c="$cum" 'BEGIN{printf "%.1f", c/60}'); bud_h=$(awk -v b="$BUDGET_S" 'BEGIN{printf "%.1f", b/3600}')
  echo "PLAN total ${n} runs, est ${tot_h} h of runner time (budget ${bud_h} h)"
fi
FAILS=0
for item in "${ITEMS[@]}"; do
  parse_item "$item" || exit 1
  label=$LABEL
  if s3_has "$label"; then log "-- $label already in S3, skipping"; continue; fi
  ok=0
  for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
    left=$((BUDGET_S - $(elapsed)))
    if [ "$left" -lt "$(run_need_s)" ]; then
      log "!! BUDGET: ${left}s left < $(run_need_s)s for this run, stopping before $label"
      echo "{\"label\":\"$label\",\"status\":\"skipped-budget\"}" >>"$STATUS"; s3up "$STATUS" "matrix_status.jsonl" >/dev/null
      # exit 0: a planned stop must not trigger the Job's restart (that would reset the budget clock)
      echo "MATRIX STOPPED (budget)"; FINAL=1; exit 0
    fi
    log "######## $label attempt=$attempt elapsed=$(elapsed)s"
    t0=$(date +%s)
    if [ "${E#sl-}" != "$E" ]; then one_run_sl && ok=1; else one_run && ok=1; fi
    echo "{\"label\":\"$label\",\"attempt\":$attempt,\"ok\":$ok,\"seconds\":$(( $(date +%s) - t0 ))}" >>"$STATUS"
    s3up "$WORK/runs/$label" "runs/$label"
    s3up "$STATUS" "matrix_status.jsonl" >/dev/null
    [ $ok = 1 ] && break
    log "    attempt $attempt failed"
  done
  if [ $ok = 1 ]; then FAILS=0; else
    FAILS=$((FAILS + 1))
    [ "$FAILS" -ge "$MAX_CONSEC_FAILS" ] && { log "!! $FAILS failed runs in a row, stopping"; echo "MATRIX STOPPED (failures)"; FINAL=1; exit 0; }
  fi
done
log "MATRIX DONE elapsed=$(elapsed)s"
FINAL=1
