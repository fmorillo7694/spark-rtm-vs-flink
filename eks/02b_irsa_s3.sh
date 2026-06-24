#!/usr/bin/env bash
# Grant the engine service accounts S3 access (IRSA) so Spark/Flink can read+write
# checkpoints to the S3 bucket. Run after 02_install.sh (operators + namespaces exist).
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"
export AWS_PROFILE="${AWS_PROFILE:-default}" AWS_REGION=us-east-1
export KUBECONFIG="$HERE/kubeconfig"
CLUSTER=rtm-flink-bench3
BUCKET=rtm-flink-bench-ckpt-<YOUR_ACCOUNT_ID>

# A least-privilege policy scoped to the checkpoint bucket only.
POLICY_ARN=$(aws iam list-policies --scope Local --query "Policies[?PolicyName=='rtm-flink-ckpt-s3'].Arn" --output text 2>/dev/null)
if [ -z "$POLICY_ARN" ] || [ "$POLICY_ARN" = "None" ]; then
  POLICY_ARN=$(aws iam create-policy --policy-name rtm-flink-ckpt-s3 --policy-document "{
    \"Version\":\"2012-10-17\",
    \"Statement\":[{\"Effect\":\"Allow\",
      \"Action\":[\"s3:GetObject\",\"s3:PutObject\",\"s3:DeleteObject\",\"s3:ListBucket\"],
      \"Resource\":[\"arn:aws:s3:::$BUCKET\",\"arn:aws:s3:::$BUCKET/*\"]}]}" \
    --query 'Policy.Arn' --output text)
fi
echo "policy: $POLICY_ARN"

# Bind the policy to each engine service account via IRSA (eksctl creates the role + annotates the SA).
for ns_sa in "spark:spark-operator-spark" "flink:flink"; do
  ns="${ns_sa%%:*}"; sa="${ns_sa##*:}"
  eksctl create iamserviceaccount --cluster "$CLUSTER" --region us-east-1 \
    --namespace "$ns" --name "$sa" --attach-policy-arn "$POLICY_ARN" \
    --approve --override-existing-serviceaccounts
done
echo "IRSA configured for spark + flink service accounts"
