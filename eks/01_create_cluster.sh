#!/usr/bin/env bash
# Create the EKS cluster (security baseline in cluster.yaml).
#
# PREREQS: cluster.yaml carries placeholders. Put real values in the gitignored copy
# eks/cluster.local.yaml (used automatically when present):
#   <YOUR_ACCOUNT_ID>, <YOUR_KMS_KEY_ID>  -> a KMS CMK for secrets envelope encryption
#   <YOUR_VPC_ID> + the 4 subnet IDs      -> an existing VPC's subnets (or delete the whole
#                                            `vpc:` block to let eksctl create a fresh VPC)
#
# To create a dedicated KMS key first (prints the key id to paste into cluster.local.yaml):
#   aws kms create-key --region us-east-1 \
#     --description "EKS secrets enc" --query 'KeyMetadata.KeyId' --output text
#
# The kubeconfig is written ONLY to eks/kubeconfig (never the shared ~/.kube/config).
# ~15-25 min for cluster + node group.
set -euo pipefail
cd "$(dirname "$0")"

export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION=us-east-1
CLUSTER="${CLUSTER:-rtm-flink-bench4}"
CONFIG="${CLUSTER_CONFIG:-cluster.yaml}"
KC="${KUBECONFIG_FILE:-$PWD/kubeconfig}"   # one file per cluster when two run at once
[ -f cluster.local.yaml ] && [ -z "${CLUSTER_CONFIG:-}" ] && CONFIG=cluster.local.yaml
grep -q "name: $CLUSTER" "$CONFIG" || { echo "!! $CONFIG does not name cluster $CLUSTER"; exit 1; }

echo "==> eksctl create cluster ($CONFIG)"
eksctl create cluster -f "$CONFIG" --kubeconfig "$KC"

echo "==> Refresh the DEDICATED kubeconfig (never touch shared ~/.kube/config)"
aws eks update-kubeconfig --name "$CLUSTER" --region us-east-1 --kubeconfig "$KC"

echo "==> Cluster up. Nodes:"
KUBECONFIG="$KC" kubectl get nodes -o wide
