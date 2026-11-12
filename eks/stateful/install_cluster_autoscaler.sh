#!/usr/bin/env bash
# Session D (scaling): Cluster Autoscaler on engine-ng ONLY, with a hard node cap.
#
#   1. engine-ng scaling config -> min 1 / desired 2 / max ENGINE_MAX (5): the ASG max is the
#      hard cap a runaway engine autoscaler cannot pass (5 x m5.2xlarge ~ $1.92/h);
#   2. IAM policy <cluster>-cluster-autoscaler: read-only Describe calls, and the two write
#      calls (SetDesiredCapacity, TerminateInstanceInAutoScalingGroup) only on ASGs tagged
#      eks:cluster-name=$CLUSTER + eks:nodegroup-name=engine-ng. bench-ng (Kafka) is out of
#      reach; no EC2/VPC write permission at all (NAT, EIP, endpoint untouched);
#   3. IRSA role (eksctl --role-only) + helm chart 9.60.0, image v1.36.1 (= the cluster's 1.36),
#      with the engine ASG named explicitly (no tag auto-discovery: EKS tags every managed
#      nodegroup's ASG, so auto-discovery would also pick up bench-ng).
# Scale-down: 3 min unneeded / 3 min after an add, so a scaled-out arm's extra nodes go before
# the next arm (the runner waits for <= START_NODES engine nodes). Engine pods are
# safe-to-evict=false, so only empty engine nodes are removed.
# The runner's self-teardown scales the deployment `cluster-autoscaler` to 0 before nodes go to 0.
#
# Env: CLUSTER [rtm-flink-bench4] ENGINE_MAX [5] ENGINE_MIN [1] ENGINE_DESIRED [2]
set -euo pipefail
cd "$(dirname "$0")/../.."
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1 PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"
export KUBECONFIG="${KUBECONFIG_FILE:-$PWD/eks/kubeconfig}"
CLUSTER="${CLUSTER:-rtm-flink-bench4}"
ENGINE_MAX="${ENGINE_MAX:-5}"; ENGINE_MIN="${ENGINE_MIN:-1}"; ENGINE_DESIRED="${ENGINE_DESIRED:-2}"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"

echo "==> engine-ng scaling min=$ENGINE_MIN desired=$ENGINE_DESIRED max=$ENGINE_MAX"
aws eks update-nodegroup-config --cluster-name "$CLUSTER" --nodegroup-name engine-ng \
  --scaling-config "minSize=$ENGINE_MIN,maxSize=$ENGINE_MAX,desiredSize=$ENGINE_DESIRED" >/dev/null
aws eks wait nodegroup-active --cluster-name "$CLUSTER" --nodegroup-name engine-ng
ASG=$(aws eks describe-nodegroup --cluster-name "$CLUSTER" --nodegroup-name engine-ng \
  --query 'nodegroup.resources.autoScalingGroups[0].name' --output text)
echo "    engine ASG: $ASG"

POLICY="${CLUSTER}-cluster-autoscaler"   # per cluster: write calls conditioned on this cluster's tag
POLICY_ARN=$(aws iam list-policies --scope Local --query "Policies[?PolicyName=='$POLICY'].Arn" --output text)
if [ -z "$POLICY_ARN" ] || [ "$POLICY_ARN" = None ]; then
  DOC=$(mktemp); trap 'rm -f "$DOC"' EXIT
  cat >"$DOC" <<EOF
{"Version": "2012-10-17", "Statement": [
  {"Effect": "Allow", "Resource": "*", "Action": [
    "autoscaling:DescribeAutoScalingGroups", "autoscaling:DescribeAutoScalingInstances",
    "autoscaling:DescribeLaunchConfigurations", "autoscaling:DescribeScalingActivities",
    "autoscaling:DescribeTags", "ec2:DescribeImages", "ec2:DescribeInstanceTypes",
    "ec2:DescribeLaunchTemplateVersions", "ec2:GetInstanceTypesFromInstanceRequirements",
    "eks:DescribeNodegroup"]},
  {"Effect": "Allow", "Resource": "*",
   "Action": ["autoscaling:SetDesiredCapacity", "autoscaling:TerminateInstanceInAutoScalingGroup"],
   "Condition": {"StringEquals": {"aws:ResourceTag/eks:cluster-name": "$CLUSTER",
                                  "aws:ResourceTag/eks:nodegroup-name": "engine-ng"}}}]}
EOF
  POLICY_ARN=$(aws iam create-policy --policy-name "$POLICY" --policy-document "file://$DOC" --query Policy.Arn --output text)
fi
eksctl create iamserviceaccount --cluster "$CLUSTER" --region "$AWS_REGION" \
  --namespace kube-system --name cluster-autoscaler --attach-policy-arn "$POLICY_ARN" \
  --role-name "${CLUSTER}-cluster-autoscaler" --role-only --approve
ROLE_ARN="arn:aws:iam::${ACCOUNT}:role/${CLUSTER}-cluster-autoscaler"

echo "==> helm cluster-autoscaler 9.60.0 (image v1.36.1)"
helm repo add --force-update autoscaler https://kubernetes.github.io/autoscaler >/dev/null
helm upgrade --install cluster-autoscaler autoscaler/cluster-autoscaler --version 9.60.0 -n kube-system \
  --set fullnameOverride=cluster-autoscaler --set cloudProvider=aws --set awsRegion="$AWS_REGION" \
  --set image.tag=v1.36.1 \
  --set "autoscalingGroups[0].name=$ASG" --set "autoscalingGroups[0].minSize=$ENGINE_MIN" \
  --set "autoscalingGroups[0].maxSize=$ENGINE_MAX" \
  --set rbac.serviceAccount.name=cluster-autoscaler \
  --set "rbac.serviceAccount.annotations.eks\.amazonaws\.com/role-arn=$ROLE_ARN" \
  --set extraArgs.scale-down-unneeded-time=3m --set extraArgs.scale-down-delay-after-add=3m \
  --set extraArgs.max-nodes-total=$((ENGINE_MAX + 6))
kubectl -n kube-system rollout status deploy/cluster-autoscaler --timeout=180s
kubectl -n kube-system logs deploy/cluster-autoscaler --tail=20 | grep -iE 'error|registered|node group' | head -10 || true
