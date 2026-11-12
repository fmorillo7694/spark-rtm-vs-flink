#!/usr/bin/env bash
# Hard teardown deadline for the benchmark cluster, independent of this laptop, of any agent
# turn and of the in-cluster runner: one-shot EventBridge Scheduler schedules that call the EKS
# API directly (universal targets), run by a role that can do nothing else.
#
#   DEADLINE           eks:DeleteNodegroup  <cluster>/<each nodegroup>   (instances terminate)
#   DEADLINE + 45 min  eks:DeleteCluster    <cluster>                     (control plane)
#
# The role (rtm-bench-deadline-scheduler) may only call eks:DeleteNodegroup / eks:DeleteCluster /
# eks:TagResource on this cluster's ARNs. It has no EC2, VPC, IAM or CloudFormation permission,
# so the NAT gateway, its EIP and the S3 endpoint cannot be touched, and neither 00_egress.sh nor
# 99_teardown.sh is involved. eksctl's CloudFormation stacks (IAM roles, security groups: no
# hourly cost) stay until `eksctl delete cluster`, which the operator still runs to clean up.
# Schedules delete themselves after firing (ActionAfterCompletion=DELETE).
#
# Usage: eks/stateful/arm_deadline.sh arm|selftest|disarm|status
#   arm       DEADLINE_EPOCH=<unix s> (required) or DEADLINE_IN_S=<s from now>
#   selftest  needs the cluster: schedules eks:TagResource on it 2 min out and waits for the
#             tag, proving schedule + role + input format end to end (same path as `arm`)
#   disarm    deletes every pending <cluster>-deadline-* schedule. Run it after a manual
#             teardown, or a later cluster with the same name would be deleted at the old deadline
#   status    lists pending schedules
# Env: CLUSTER [rtm-flink-bench4] NODEGROUPS [engine-ng bench-ng] INPUT_CASE [pascal|camel; pascal: CreateSchedule validates PascalCase field names]
#      DRY_RUN=1 prints every AWS call instead of making it (no credentials needed)
set -euo pipefail
export AWS_REGION="${AWS_REGION:-us-east-1}" PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"
MODE="${1:?usage: arm_deadline.sh arm|selftest|disarm|status}"
CLUSTER="${CLUSTER:-rtm-flink-bench4}"
NODEGROUPS="${NODEGROUPS:-engine-ng bench-ng}"
INPUT_CASE="${INPUT_CASE:-pascal}"
DRY_RUN="${DRY_RUN:-0}"
ROLE=rtm-bench-deadline-scheduler
PREFIX="${CLUSTER}-deadline-"

aws_() {  # aws, or print it in dry-run
  if [ "$DRY_RUN" = 1 ]; then { printf 'DRY aws'; printf ' %q' "$@"; echo; } >&2; return 0; fi
  aws "$@"
}
if [ "$DRY_RUN" = 1 ]; then ACCOUNT=000000000000; else ACCOUNT=$(aws sts get-caller-identity --query Account --output text); fi
CLUSTER_ARN="arn:aws:eks:${AWS_REGION}:${ACCOUNT}:cluster/${CLUSTER}"
ROLE_ARN="arn:aws:iam::${ACCOUNT}:role/${ROLE}"

ensure_role() {
  local trust policy
  trust=$(printf '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole","Condition":{"StringEquals":{"aws:SourceAccount":"%s"}}}]}' "$ACCOUNT")
  policy=$(printf '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["eks:DeleteNodegroup","eks:DeleteCluster","eks:TagResource"],"Resource":["%s","arn:aws:eks:%s:%s:nodegroup/%s/*/*"]}]}' \
    "$CLUSTER_ARN" "$AWS_REGION" "$ACCOUNT" "$CLUSTER")
  if [ "$DRY_RUN" = 1 ] || ! aws iam get-role --role-name "$ROLE" >/dev/null 2>&1; then
    aws_ iam create-role --role-name "$ROLE" --assume-role-policy-document "$trust" \
      --description "One-shot EKS teardown deadline for ${CLUSTER} (eks/stateful/arm_deadline.sh)" >/dev/null
    [ "$DRY_RUN" = 1 ] || sleep 12   # IAM propagation before Scheduler validates the role
  fi
  # one inline policy per cluster: two clusters armed at once share the role (final run 6a/6b),
  # and a single policy name made the second arm overwrite the first cluster's permission
  aws_ iam put-role-policy --role-name "$ROLE" --policy-name "eks-teardown-only-${CLUSTER}" --policy-document "$policy"
}

input() {  # api -> JSON request for that EKS call, in the configured parameter casing
  local ng=${2:-}
  case "$INPUT_CASE:$1" in
    camel:deleteNodegroup)  printf '{"clusterName":"%s","nodegroupName":"%s"}' "$CLUSTER" "$ng" ;;
    pascal:deleteNodegroup) printf '{"ClusterName":"%s","NodegroupName":"%s"}' "$CLUSTER" "$ng" ;;
    camel:deleteCluster)    printf '{"name":"%s"}' "$CLUSTER" ;;
    pascal:deleteCluster)   printf '{"Name":"%s"}' "$CLUSTER" ;;
    camel:tagResource)      printf '{"resourceArn":"%s","tags":{"deadline-selftest":"camel"}}' "$CLUSTER_ARN" ;;
    pascal:tagResource)     printf '{"ResourceArn":"%s","Tags":{"deadline-selftest-pascal":"pascal"}}' "$CLUSTER_ARN" ;;
  esac
}

schedule() {  # name epoch api input
  local at target
  at=$(date -u -r "$2" +%Y-%m-%dT%H:%M:%S 2>/dev/null || date -u -d "@$2" +%Y-%m-%dT%H:%M:%S)
  target=$(printf '{"Arn":"arn:aws:scheduler:::aws-sdk:eks:%s","RoleArn":"%s","Input":%s,"RetryPolicy":{"MaximumRetryAttempts":20,"MaximumEventAgeInSeconds":3600}}' \
    "$3" "$ROLE_ARN" "$(printf '%s' "$4" | python3 -c 'import json,sys;print(json.dumps(sys.stdin.read()))')")
  aws_ scheduler delete-schedule --name "$1" >/dev/null 2>&1 || true
  aws_ scheduler create-schedule --name "$1" --schedule-expression "at($at)" \
    --schedule-expression-timezone UTC --flexible-time-window Mode=OFF \
    --action-after-completion DELETE --target "$target" >/dev/null
  echo "armed $1: eks:$3 at ${at}Z $4"
}

case "$MODE" in
  arm)
    D="${DEADLINE_EPOCH:-}"
    [ -n "$D" ] || D=$(( $(date +%s) + ${DEADLINE_IN_S:?set DEADLINE_EPOCH or DEADLINE_IN_S} ))
    ensure_role
    for ng in $NODEGROUPS; do schedule "${PREFIX}ng-${ng}" "$D" deleteNodegroup "$(input deleteNodegroup "$ng")"; done
    schedule "${PREFIX}cluster" $((D + 2700)) deleteCluster "$(input deleteCluster)"
    ;;
  selftest)
    ensure_role
    T=$(( $(date +%s) + 120 ))
    schedule "${PREFIX}selftest" "$T" tagResource "$(input tagResource)"
    [ "$DRY_RUN" = 1 ] && exit 0
    for _ in $(seq 1 40); do
      tags=$(aws eks describe-cluster --name "$CLUSTER" --query 'cluster.tags' --output json)
      if grep -qE '"deadline-selftest(-pascal)?": "(camel|pascal)"' <<<"$tags"; then
        echo "SELFTEST OK: schedule fired eks:TagResource with the $INPUT_CASE input"; exit 0
      fi
      sleep 15
    done
    echo "SELFTEST FAILED: no tag after 10 min"; exit 1
    ;;
  disarm)
    for n in $(aws_ scheduler list-schedules --name-prefix "$PREFIX" --query 'Schedules[].Name' --output text); do
      aws_ scheduler delete-schedule --name "$n" && echo "disarmed $n"
    done
    ;;
  status)
    aws_ scheduler list-schedules --name-prefix "$PREFIX" --query 'Schedules[].[Name,State]' --output text
    ;;
  *) echo "unknown mode $MODE" >&2; exit 64 ;;
esac
