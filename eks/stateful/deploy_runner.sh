#!/usr/bin/env bash
# Launch the in-cluster stateful matrix (NOT run as part of the local work; needs the EKS
# cluster from eks/01..03 and the images from eks/03_build_push_images.sh).
#
#   1. IRSA for the runner SA, scoped to the checkpoint bucket policy from eks/02b_irsa_s3.sh;
#   2. ConfigMap `stateful-runner` = eks/stateful/assemble_manifests.sh output (runner script,
#      eks/jobs/stateful/*.yaml, unchanged eks/jobs/p6 manifests as p6-*.yaml, p6 topics);
#   3. the RBAC + Job from runner-job.yaml.
# Teardown does not depend on this laptop: the runner scales both nodegroups to 0 when it ends
# (IRSA policy rtm-bench-runner-scale-down), and an EventBridge Scheduler deadline deletes the
# nodegroups and the cluster at launch + BUDGET_S + 1 h (arm_deadline.sh; NO_DEADLINE=1 skips it).
# After this the laptop can disconnect: follow with
#   kubectl -n kafka logs -f job/stateful-runner      (optional)
#   aws s3 cp --recursive s3://rtm-flink-bench-ckpt-<acct>/<S3_PREFIX>/ results/eks-stateful/
#
# Env: SELF_TEARDOWN (1; 0 only for a smoke Job that must leave the cluster up), BUDGET_S (45000 = 12.5 h), PLAN_SET (B; A = session A, C = joins, D = scaling), PLAN ("" = PLAN_SET),
#      RUNS (2, PLAN_SET A only), STATELESS_TAG (v43), S3_PREFIX (stateful/<stamp>), CLUSTER
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE/../.."
export AWS_REGION=us-east-1 PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"
export KUBECONFIG="${KUBECONFIG_FILE:-$PWD/eks/kubeconfig}"
source eks/.registry.env
CLUSTER="${CLUSTER:-rtm-flink-bench4}"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
BUDGET_S="${BUDGET_S:-45000}"
RUNS="${RUNS:-2}"
PLAN_SET="${PLAN_SET:-B}"
STATELESS_TAG="${STATELESS_TAG:-v43}"
S3_PREFIX="${S3_PREFIX:-stateful/$(date -u +%Y%m%dT%H%M%SZ)}"
POLICY_ARN=$(aws iam list-policies --scope Local --query "Policies[?PolicyName=='rtm-flink-ckpt-s3'].Arn" --output text)
# Self scale-down (runner SELF_TEARDOWN=1): only these two EKS calls, only on this cluster's
# nodegroups. No EC2/VPC permission at all, so the NAT gateway, EIP and endpoint are out of reach.
SCALE_POLICY="${CLUSTER}-runner-scale-down"   # per cluster: scoped to this cluster's nodegroup ARNs
SCALE_ARN=$(aws iam list-policies --scope Local --query "Policies[?PolicyName=='$SCALE_POLICY'].Arn" --output text)
if [ -z "$SCALE_ARN" ] || [ "$SCALE_ARN" = None ]; then
  SCALE_ARN=$(aws iam create-policy --policy-name "$SCALE_POLICY" --query Policy.Arn --output text --policy-document "{
    \"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",
    \"Action\":[\"eks:DescribeNodegroup\",\"eks:UpdateNodegroupConfig\"],
    \"Resource\":\"arn:aws:eks:${AWS_REGION}:${ACCOUNT}:nodegroup/${CLUSTER}/*/*\"}]}")
fi

eksctl create iamserviceaccount --cluster "$CLUSTER" --region "$AWS_REGION" \
  --namespace kafka --name stateful-runner --attach-policy-arn "$POLICY_ARN,$SCALE_ARN" \
  --role-name "${CLUSTER}-stateful-runner" --role-only --approve
ROLE_ARN="arn:aws:iam::${ACCOUNT}:role/${CLUSTER}-stateful-runner"

# Hard deadline, independent of this laptop and of the runner: EventBridge Scheduler deletes the
# nodegroups at launch + BUDGET_S + 1 h and the cluster 45 min later (eks/stateful/arm_deadline.sh).
[ "${NO_DEADLINE:-0}" = 1 ] || CLUSTER="$CLUSTER" DEADLINE_EPOCH=$(( $(date +%s) + BUDGET_S + 3600 )) \
  bash eks/stateful/arm_deadline.sh arm

M="$(mktemp -d)"; trap 'rm -rf "$M"' EXIT
bash eks/stateful/assemble_manifests.sh "$M" >/dev/null
kubectl -n kafka create configmap stateful-runner --from-file="$M/" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n kafka delete job stateful-runner --ignore-not-found --wait=true
sed -e "s#__REGISTRY__#$REGISTRY#g" -e "s#__TAG__#$TAG#g" -e "s#__ACCOUNT__#$ACCOUNT#g" \
    -e "s#__RUNNER_ROLE_ARN__#$ROLE_ARN#g" -e "s#__BUDGET_S__#$BUDGET_S#g" \
    -e "s#__DEADLINE_S__#$((BUDGET_S + 1800))#g" -e "s#__PLAN__#${PLAN:-}#g" -e "s#__RUNS__#$RUNS#g" -e "s#__START_EPOCH__#$(date +%s)#g" \
    -e "s#__PLAN_SET__#$PLAN_SET#g" -e "s#__STATELESS_TAG__#$STATELESS_TAG#g" -e "s#__CLUSTER__#$CLUSTER#g" \
    -e "s#__S3_PREFIX__#$S3_PREFIX#g" -e "s#__SELF_TEARDOWN__#${SELF_TEARDOWN:-1}#g" eks/stateful/runner-job.yaml | kubectl apply -f -
echo "runner started; artifacts -> s3://rtm-flink-bench-ckpt-${ACCOUNT}/${S3_PREFIX}/"
