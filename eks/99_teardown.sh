#!/usr/bin/env bash
# Tear down the EKS benchmark CLUSTER only (node group, control plane, eksctl-made IRSA
# roles/OIDC provider, addons). Uses the dedicated eks/kubeconfig.
#
# Deliberately LEFT IN PLACE (shared / reused across benchmark rounds):
#   - the S3 checkpoint bucket, the ECR repos rtm-bench/*, the KMS secrets CMK
#   - the shared VPC/subnets (they predate this benchmark)
#   - the rtm-flink-ckpt-s3 IAM policy
# Never touches any other cluster in the account.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"
export AWS_REGION=us-east-1 PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"
export KUBECONFIG="$HERE/kubeconfig"
CLUSTER="${CLUSTER:-rtm-flink-bench4}"

kubectl config current-context | grep -q "$CLUSTER" || { echo "!! kubeconfig is not $CLUSTER"; exit 1; }

echo "==> [1/3] Delete in-cluster workloads (best effort; releases EBS PVCs)"
kubectl -n spark delete sparkapplication --all --ignore-not-found 2>/dev/null || true
kubectl -n flink delete flinkdeployment --all --ignore-not-found 2>/dev/null || true
kubectl -n kafka delete job --all --ignore-not-found 2>/dev/null || true
kubectl -n kafka delete kafka --all --ignore-not-found --wait=true --timeout=300s 2>/dev/null || true
kubectl -n kafka delete pvc --all --ignore-not-found --wait=true --timeout=300s 2>/dev/null || true

echo "==> [2/3] Delete EKS cluster $CLUSTER (node group + control plane, ~10-15 min)"
eksctl delete cluster --region us-east-1 --name "$CLUSTER" --disable-nodegroup-eviction --wait

echo "==> [3/3] Remove the NAT gateway, its EIP and the S3 gateway endpoint (eks/00_egress.sh up)"
bash "$HERE/00_egress.sh" down

echo "==> Teardown complete. Remaining eksctl stacks for $CLUSTER:"
aws cloudformation list-stacks --query "StackSummaries[?starts_with(StackName,'eksctl-$CLUSTER') && StackStatus!='DELETE_COMPLETE'].[StackName,StackStatus]" --output text
