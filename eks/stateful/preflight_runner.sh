#!/usr/bin/env bash
# Self-teardown preflight BEFORE the runner Job exists (final run option A, clusters B-D; A reused
# the IAM role and RBAC its smoke Job had created).
#
#   1. the runner's IAM role (IRSA) and its per-cluster scale-down policy: the same calls as
#      deploy_runner.sh, which repeats them idempotently at launch;
#   2. the runner's RBAC: every document of runner-job.yaml before the Job, nothing else;
#   3. checks: IAM simulation of eks:UpdateNodegroupConfig / DescribeNodegroup on both nodegroups,
#      RBAC can-i for what self_teardown() and the stop marker need, and no stop marker present.
#
# Pass = 4 x allowed, 6 x yes, no marker (exit 0); anything else exits 1. Launch the Job only after
# a pass, with SELF_TEARDOWN=1.
#
# Usage: CLUSTER=<name> KUBECONFIG_FILE=<kubeconfig> [OUT=<file>] bash eks/stateful/preflight_runner.sh
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE/../.."
export AWS_REGION="${AWS_REGION:-us-east-1}" PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"
export KUBECONFIG="${KUBECONFIG_FILE:-$PWD/eks/kubeconfig}"
C="${CLUSTER:?set CLUSTER}"
OUT="${OUT:-/dev/null}"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"

POLICY_ARN=$(aws iam list-policies --scope Local --query "Policies[?PolicyName=='rtm-flink-ckpt-s3'].Arn" --output text)
SCALE_POLICY="${C}-runner-scale-down"
SCALE_ARN=$(aws iam list-policies --scope Local --query "Policies[?PolicyName=='$SCALE_POLICY'].Arn" --output text)
if [ -z "$SCALE_ARN" ] || [ "$SCALE_ARN" = None ]; then
  SCALE_ARN=$(aws iam create-policy --policy-name "$SCALE_POLICY" --query Policy.Arn --output text --policy-document "{
    \"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",
    \"Action\":[\"eks:DescribeNodegroup\",\"eks:UpdateNodegroupConfig\"],
    \"Resource\":\"arn:aws:eks:${AWS_REGION}:${ACCOUNT}:nodegroup/${C}/*/*\"}]}")
fi
eksctl create iamserviceaccount --cluster "$C" --region "$AWS_REGION" --namespace kafka --name stateful-runner \
  --attach-policy-arn "$POLICY_ARN,$SCALE_ARN" --role-name "${C}-stateful-runner" --role-only --approve >/dev/null 2>&1
R="arn:aws:iam::${ACCOUNT}:role/${C}-stateful-runner"

# RBAC only: the documents before `kind: Job` (drop that Job's own apiVersion line)
awk '/^kind: Job/{exit} {print}' eks/stateful/runner-job.yaml | sed '$d' \
  | sed "s#__RUNNER_ROLE_ARN__#$R#g" | kubectl apply -f - >/dev/null

S=system:serviceaccount:kafka:stateful-runner
P="$(mktemp)"; trap 'rm -f "$P"' EXIT
{
  echo "--- self-teardown preflight $C $(date -u +%FT%TZ) (before matrix Job; SELF_TEARDOWN=1 in launch cmd)"
  echo "IAM: $(aws iam list-attached-role-policies --role-name "$C-stateful-runner" --query 'AttachedPolicies[].PolicyName' --output text)"
  for ng in engine-ng bench-ng; do
    A=$(aws eks describe-nodegroup --cluster-name "$C" --nodegroup-name "$ng" --query nodegroup.nodegroupArn --output text)
    echo "simulate $ng: $(aws iam simulate-principal-policy --policy-source-arn "$R" \
      --action-names eks:UpdateNodegroupConfig eks:DescribeNodegroup --resource-arns "$A" \
      --query 'EvaluationResults[].EvalDecision' --output text)"
  done
  for r in "delete kafkas.kafka.strimzi.io -n kafka" "delete kafkanodepools.kafka.strimzi.io -n kafka" \
           "delete persistentvolumeclaims -n kafka" "list persistentvolumes" "get nodes/proxy" \
           "get configmap/stateful-runner-stop -n kafka"; do
    # shellcheck disable=SC2086  # $r is a verb + resource + flags, split on purpose
    echo "can-i $r: $(kubectl auth can-i $r --as="$S" 2>/dev/null)"
  done
  echo "stop marker present: $(kubectl -n kafka get configmap stateful-runner-stop -o name 2>/dev/null || echo no)"
} > "$P"
tee "$OUT" < "$P"
if [ "$(grep -o allowed "$P" | wc -l | tr -d ' ')" = 4 ] && [ "$(grep -c ': yes$' "$P")" = 6 ] \
   && grep -q 'marker present: no' "$P"; then
  echo "PREFLIGHT PASS $C"
else
  echo "PREFLIGHT FAIL $C"; exit 1
fi
