#!/usr/bin/env bash
# Create the EKS cluster (security baseline in cluster.yaml).
#
# PREREQS — before running, edit cluster.yaml and replace the placeholders:
#   <YOUR_ACCOUNT_ID>, <YOUR_KMS_KEY_ID>  -> a KMS CMK for secrets envelope encryption
#   <YOUR_VPC_ID> + the 4 subnet IDs      -> an existing VPC's subnets (or delete the whole
#                                            `vpc:` block to let eksctl create a fresh VPC)
#
# To create a dedicated KMS key first (prints the key id to paste into cluster.yaml):
#   aws kms create-key --region us-east-1 \
#     --description "EKS secrets enc" --query 'KeyMetadata.KeyId' --output text
#
# ~15-20 min for cluster + node group.
set -euo pipefail
cd "$(dirname "$0")"

export AWS_PROFILE="${AWS_PROFILE:-default}"
export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION=us-east-1

echo "==> eksctl create cluster"
eksctl create cluster -f cluster.yaml

echo "==> Point a DEDICATED kubeconfig at the cluster (never touch shared ~/.kube/config)"
aws eks update-kubeconfig --name rtm-flink-bench3 --region us-east-1 --kubeconfig "$PWD/kubeconfig"

echo "==> Cluster up. Nodes:"
KUBECONFIG="$PWD/kubeconfig" kubectl get nodes -o wide
