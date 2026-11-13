#!/usr/bin/env bash
# Offline checks for the in-cluster stateful runner (no cluster, no AWS):
#   1. bash -n + shellcheck (if installed) on the runner scripts;
#   2. runner-job.yaml rendered with dummy values parses as YAML, no placeholder left;
#   3. a DRY_RUN of the whole default matrix: renders and validates every engine + load manifest
#      and prints the kubectl calls it would make.
# Needs .venv with pyyaml (pip install pyyaml).
set -euo pipefail
cd "$(dirname "$0")/../.."
export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"
PY="$PWD/.venv/bin/python"
S=eks/stateful
SCRIPTS="$S/run_matrix_incluster.sh $S/deploy_runner.sh $S/check.sh $S/assemble_manifests.sh $S/arm_deadline.sh $S/install_cluster_autoscaler.sh $S/preflight_runner.sh $S/verify_s3_results.sh $S/wrapup_cluster.sh"
for f in $SCRIPTS; do bash -n "$f"; done
echo "bash -n ok"
if command -v shellcheck >/dev/null; then
  # shellcheck disable=SC2086
  shellcheck -S warning $SCRIPTS && echo "shellcheck ok"
else
  echo "shellcheck not installed: skipped"
fi

T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
sed -e "s#__REGISTRY__#123.dkr.ecr.us-east-1.amazonaws.com#g" -e "s#__TAG__#v1#g" -e "s#__ACCOUNT__#123#g" \
    -e "s#__RUNNER_ROLE_ARN__#arn:aws:iam::123:role/x#g" -e "s#__BUDGET_S__#21600#g" \
    -e "s#__DEADLINE_S__#23400#g" -e "s#__PLAN__##g" -e "s#__RUNS__#2#g" -e "s#__START_EPOCH__#1#g" -e "s#__S3_PREFIX__#stateful/test#g" -e "s#__SELF_TEARDOWN__#1#g" -e "s#__RUN_EST_S__#780#g" \
    -e "s#__PLAN_SET__#B#g" -e "s#__STATELESS_TAG__#v43#g" -e "s#__CLUSTER__#rtm-flink-bench4#g" \
    $S/runner-job.yaml >"$T/runner-job.yaml"
"$PY" - "$T/runner-job.yaml" <<'PY'
import re, sys, yaml
t = open(sys.argv[1]).read()
assert not re.findall(r"__[A-Z_]+__", t), "placeholder left"
docs = [d for d in yaml.safe_load_all(t) if d]
job = [d for d in docs if d["kind"] == "Job"][0]
assert isinstance(job["spec"]["activeDeadlineSeconds"], int)
print("runner-job.yaml ok:", ", ".join(f"{d['kind']}/{d['metadata']['name']}@{d['metadata'].get('namespace')}" for d in docs))
PY

bash $S/assemble_manifests.sh "$T/m" | sed 's/^/assembled: /'
# The ConfigMap keys are the file names: they must be flat and fit the 1 MiB ConfigMap limit.
echo "configmap payload: $(cat "$T"/m/* | wc -c | tr -d ' ') bytes"

# DRY runs of every plan set (B = session B default, A = session A reproduction, C = session C
# joins, D/D2 = scaling, FA/FB = the final lean run's two clusters). Each renders and validates every manifest; the PLAN lines show per-run estimates and
# what BUDGET_S would cut.
for ps in B A C D D2 FA FB F30A F30B F30C F30D; do
  echo "===== DRY_RUN PLAN_SET=$ps"
  DRY_RUN=1 PY="$PY" MANIFESTS="$T/m" WORK="$T/work-$ps" PLAN_SET=$ps RUNS=2 STATELESS_TAG=v43 SELF_TEARDOWN=1 \
    bash $S/run_matrix_incluster.sh 2>"$T/dry-$ps.err" >"$T/dry-$ps.out" || true
  grep -E '^PLAN ' "$T/dry-$ps.out" >"$T/plan-$ps.txt" || true
  [ "${VERBOSE:-0}" = 1 ] && cat "$T/plan-$ps.txt"
  grep -E 'CUT-BY-BUDGET' "$T/plan-$ps.txt" | sed 's/^/  /' || true
  grep -E '^PLAN total' "$T/dry-$ps.out"
  grep -c 'DRY kubectl apply' "$T/dry-$ps.err" | sed 's/^/kubectl apply calls: /'
  grep -q 'DRY eks update-nodegroup-config' "$T/dry-$ps.err" && echo "self-teardown step reached" \
    || { echo "self-teardown step missing"; exit 1; }
  if grep -qE 'unrendered|Traceback|unknown|!!' "$T/dry-$ps.out" "$T/dry-$ps.err"; then
    grep -E 'unrendered|Traceback|unknown|!!' "$T/dry-$ps.out" "$T/dry-$ps.err" | head; echo "dry-run $ps FAILED"; exit 1
  fi
  grep -q 'MATRIX DONE' "$T/dry-$ps.out" && echo "dry-run $ps ok" || { tail -5 "$T/dry-$ps.out"; exit 1; }
  # Placement. B and C: stateful engine pods pinned to the tainted engine nodes, one worker per
  # node; stateless p6 arms (engine + load pods) tolerate the taint only (one shared pool, no
  # selector/affinity). A: nothing added anywhere.
  "$PY" - "$T/work-$ps/rendered" "$ps" <<'PY'
import collections, glob, sys, yaml
a = sys.argv[2] == "A"
sel, tol = {"bench/role": "engine"}, {"key": "bench/role", "operator": "Equal", "value": "engine", "effect": "NoSchedule"}
n = collections.Counter()
for f in sorted(glob.glob(sys.argv[1] + "/*.yaml")):
    stateless = "-p6-r" in f
    final = sys.argv[2].startswith("F")   # final run: stateless engines pinned, their load unpinned
    want = "shared" if a else ("pool" if stateless and not final else "dedicated")
    for d in yaml.safe_load_all(open(f)):
        if not d:
            continue
        s = d["spec"]
        if d["kind"] == "Job":
            if not stateless:
                continue
            if final:   # load pods: no engine-node selector or toleration
                p = s["template"]["spec"]
                assert not p.get("nodeSelector") and not p.get("tolerations"), f"final-run load pinned in {f}"
                n[("load", "shared")] += 1
                continue
            pods, spread = [s["template"]["spec"]], None
        elif d["kind"] == "SparkApplication":
            pods = [s["driver"], s["executor"]]
            spread = s["executor"].get("affinity")
        elif d["kind"] == "FlinkDeployment":
            pods = [s["podTemplate"]["spec"]]
            spread = s["taskManager"].get("podTemplate", {}).get("spec", {}).get("affinity")
        else:
            continue
        n[(d["kind"] == "Job" and "load" or "engine", want)] += 1
        tols = [p.get("tolerations") for p in pods]
        sels = [p.get("nodeSelector") for p in pods]
        if want == "dedicated":
            ok = all(t == [tol] for t in tols) and all(x == sel for x in sels) and spread
            ok = ok and spread["podAntiAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"][0]["topologyKey"] == "kubernetes.io/hostname"
        elif want == "pool":
            ok = all(t == [tol] for t in tols) and not any(sels) and not spread
        else:
            ok = not any(tols) and not any(sels) and not spread
        assert ok, f"placement wrong ({want}) in {f}: {d['kind']}"
print("placement ok: " + ", ".join(f"{k[0]} {k[1]}={v}" for k, v in sorted(n.items())))
PY
done
[ -n "${PLAN_OUT:-}" ] && cp "$T/plan-B.txt" "$PLAN_OUT"
# The SIGTERM rule (on_term): teardown only at the budget end or with the stop marker, never on a
# plain TERM before the budget (Job deleted to relaunch, node drain, eviction). Runs the runner's
# own on_term with stubbed clock and marker in a subshell per case.
fn=$(sed -n '/^on_term() {/,/^}/p' $S/run_matrix_incluster.sh)
[ -n "$fn" ] || { echo "on_term not found"; exit 1; }
term_case() {  # elapsed budget marker(0/1) -> prints FINAL after on_term
  # shellcheck disable=SC2034  # BUDGET_S and FINAL are read by the eval'd on_term
  ( T_ELAPSED=$1; BUDGET_S=$2; log() { :; }; elapsed() { echo "$T_ELAPSED"; }
    if [ "$3" = 1 ]; then stop_requested() { return 0; }; else stop_requested() { return 1; }; fi
    FINAL=0; trap 'echo "$FINAL"' EXIT; eval "$fn"; on_term ) 2>/dev/null || true   # on_term exits 143
}
r1=$(term_case 100 1000 0); r2=$(term_case 1000 1000 0); r3=$(term_case 100 1000 1)
[ "$r1$r2$r3" = 011 ] && echo "on_term ok: before budget -> no teardown, budget end -> teardown, stop marker -> teardown" \
  || { echo "on_term FAILED: got $r1$r2$r3, want 011"; exit 1; }
[ -n "${PLAN_OUT_C:-}" ] && cp "$T/plan-C.txt" "$PLAN_OUT_C"
# Deadline schedules (EventBridge Scheduler): render the arm calls without credentials.
DRY_RUN=1 DEADLINE_IN_S=21600 bash $S/arm_deadline.sh arm 2>/dev/null | sed 's/^/deadline: /'
true
