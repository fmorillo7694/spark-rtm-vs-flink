#!/usr/bin/env bash
# Tear down ALL AWS resources created for the EKS benchmark. Run when finished.
# Deletes: engine deployments, Kafka, operators, the EKS cluster (+ node group), the
# dedicated KMS key alias (schedules key deletion), and ECR repos.
# Leaves the shared VPC/subnets untouched (they predate this benchmark).
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"
export AWS_PROFILE="${AWS_PROFILE:-default}" AWS_REGION=us-east-1
export KUBECONFIG="$HERE/kubeconfig"
CLUSTER=rtm-flink-bench3

echo "==> [1/5] Delete in-cluster workloads (best effort)"
kubectl -n spark delete sparkapplication --all --ignore-not-found 2>/dev/null || true
kubectl -n flink delete flinkdeployment --all --ignore-not-found 2>/dev/null || true
kubectl -n kafka delete kafka --all --ignore-not-found 2>/dev/null || true
kubectl -n kafka delete job --all --ignore-not-found 2>/dev/null || true

echo "==> [2/5] Delete EKS cluster (node group + control plane, ~10-15 min)"
eksctl delete cluster --region us-east-1 --name "$CLUSTER" --disable-nodegroup-eviction --wait || \
  echo "   (eksctl delete reported issues; check CloudFormation console)"

echo "==> [3/5] Schedule KMS key deletion (7-day window)"
KEY_ARN="arn:aws:kms:us-east-1:<YOUR_ACCOUNT_ID>:key/<YOUR_KMS_KEY_ID>"
aws kms schedule-key-deletion --key-id "$KEY_ARN" --pending-window-in-days 7 2>/dev/null || \
  echo "   (key already scheduled or gone)"

echo "==> [4/6] Delete ECR repos"
for r in rtm-bench/spark rtm-bench/flink rtm-bench/pyflink rtm-bench/load; do
  aws ecr delete-repository --repository-name "$r" --force 2>/dev/null || true
done

echo "==> [5/6] Delete the S3 checkpoint bucket"
aws s3 rb "s3://rtm-flink-bench-ckpt-<YOUR_ACCOUNT_ID>" --force 2>/dev/null || \
  echo "   (bucket already gone, or run manually: aws s3 rb s3://<bucket> --force)"

echo "==> [6/6] Orphaned ROLLBACK_COMPLETE stacks from failed attempts (manual note)"
echo "   eksctl-rtm-flink-bench-cluster and eksctl-rtm-flink-bench2-cluster are empty"
echo "   husks with termination protection; delete from the CFN console if desired."

echo "==> Teardown complete (verify: eksctl get cluster --region us-east-1)"
