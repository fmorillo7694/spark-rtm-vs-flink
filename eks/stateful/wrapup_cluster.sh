#!/usr/bin/env bash
# Wrap-up of one benchmark cluster after its runner self-tore down (final run option A):
#
#   1. gate: both nodegroups at desired 0 (self-teardown finished), else stop;
#   2. gate: verify_s3_results.sh on the cluster's S3 prefix (S3 is the only copy), else stop;
#   3. eksctl delete cluster --wait;
#   4. arm_deadline.sh disarm (a later cluster with the same name must not inherit the deadline);
#   5. verify: no cluster, no undeleted eksctl stacks, no instances, no tagged volumes, no
#      schedules; NAT gateway(s) of the VPC still available (00_egress.sh / 99_teardown.sh untouched).
#
# Nothing is deleted unless 1 and 2 pass. Logs go to OUT_DIR.
# Usage: CLUSTER=rtm-flink-bench7a S3_PREFIX=final30/a OUT_DIR=results/final30/a/logs \
#        [EXPECT=11] [NODEGROUPS="engine-ng bench-ng"] bash eks/stateful/wrapup_cluster.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE/../.." || exit 1
export AWS_REGION="${AWS_REGION:-us-east-1}" PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"
C="${CLUSTER:?set CLUSTER}"
PFX="${S3_PREFIX:?set S3_PREFIX}"
L="${OUT_DIR:?set OUT_DIR}"
NODEGROUPS="${NODEGROUPS:-engine-ng bench-ng}"
mkdir -p "$L"

for ng in $NODEGROUPS; do
  d=$(aws eks describe-nodegroup --cluster-name "$C" --nodegroup-name "$ng" \
        --query nodegroup.scalingConfig.desiredSize --output text)
  [ "$d" = 0 ] || { echo "STOP: $C/$ng desired=$d, self-teardown not done; nothing deleted"; exit 1; }
done
echo "self-teardown done: nodegroups $NODEGROUPS at desired 0"

S3_PREFIX="$PFX" EXPECT="${EXPECT:-11}" bash eks/stateful/verify_s3_results.sh > "$L/s3_verify.txt" 2>&1
rc=$?
tail -n 3 "$L/s3_verify.txt"
[ "$rc" = 0 ] || { echo "STOP: S3 gaps for $C; nothing deleted"; exit 1; }

eksctl delete cluster --name "$C" --region "$AWS_REGION" --wait > "$L/eksctl_delete.log" 2>&1
echo $? > "$L/eksctl_delete.rc"
echo "eksctl delete rc=$(cat "$L/eksctl_delete.rc")"
CLUSTER="$C" bash eks/stateful/arm_deadline.sh disarm | tee "$L/deadline_disarm.txt"

{
  echo "# teardown verification $C $(date -u +%FT%TZ)"
  echo "cluster: [$(aws eks list-clusters --query "clusters[?@=='$C']" --output text)]"
  echo "stacks not DELETE_COMPLETE: [$(aws cloudformation list-stacks \
    --query "StackSummaries[?starts_with(StackName,'eksctl-$C-') && StackStatus!='DELETE_COMPLETE'].StackName" --output text)]"
  echo "instances: [$(aws ec2 describe-instances --filters "Name=tag:eks:cluster-name,Values=$C" \
    Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].InstanceId' --output text)]"
  echo "tagged volumes: [$(aws ec2 describe-volumes --filters "Name=tag-key,Values=kubernetes.io/cluster/$C" \
    --query 'Volumes[].VolumeId' --output text)]"
  # the EBS CSI driver does not always tag the cluster; list every unattached volume in the region
  echo "unattached volumes (region): [$(aws ec2 describe-volumes --filters Name=status,Values=available \
    --query 'Volumes[].[VolumeId,CreateTime]' --output text | tr '\t\n' '@ ')]"
  echo "schedules: [$(aws scheduler list-schedules --name-prefix "$C-deadline-" --query 'Schedules[].Name' --output text)]"
  # NAT_VPC_ID narrows to the benchmark VPC; otherwise every non-deleted NAT in the region
  echo "NAT gateways: $(aws ec2 describe-nat-gateways ${NAT_VPC_ID:+--filter Name=vpc-id,Values=$NAT_VPC_ID} \
    --query "NatGateways[?State!='deleted'].[NatGatewayId,State]" --output text | tr '\t\n' ': ')"
} | tee "$L/teardown_verify.txt"
