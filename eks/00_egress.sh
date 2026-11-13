#!/usr/bin/env bash
# Egress for the private node subnets of the reused VPC, built to keep NAT data near zero:
#   - S3 *gateway* endpoint on the private route tables (free, no per-GB charge). Carries
#     the heavy traffic: S3 checkpoints and ECR image layers (ECR serves layers from S3).
#   - ONE NAT gateway (+ EIP) in the first public subnet, default route of the private route
#     tables. Only the small leftovers use it: EKS/STS/ECR API calls, and the one-time
#     operator/Kafka image + helm chart pulls (quay.io, ghcr.io, ...).
# Kafka, engine, producer and consumer traffic never leaves the VPC.
# Everything created is tagged created-by=rtm-flink-bench4 so `down` removes exactly that.
#
# IDs come from the gitignored eks/cluster.local.yaml (vpc.id + private/public subnets).
# Usage: eks/00_egress.sh up|down|status
set -euo pipefail
cd "$(dirname "$0")"
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1 PATH="/usr/local/bin:$PATH"
TAG_KEY=created-by; TAG_VAL=rtm-flink-bench4
CFG=cluster.local.yaml
VPC=$(awk '/^vpc:/{f=1} f&&/ id: vpc-/{print $2; exit}' "$CFG")
PUB=$(awk '/public:/{f=1;next} /private:/{f=0} f&&/subnet-/{gsub(/[{},]/,"");for(i=1;i<=NF;i++) if($i ~ /^subnet-/){print $i; exit}}' "$CFG")
PRIV=$(awk '/private:/{f=1;next} f&&/subnet-/{gsub(/[{},]/,"");for(i=1;i<=NF;i++) if($i ~ /^subnet-/) print $i} f&&/clusterEndpoints/{exit}' "$CFG")
[ -n "$VPC" ] && [ -n "$PUB" ] && [ -n "$PRIV" ] || { echo "!! could not read VPC/subnets from $CFG"; exit 1; }
TAGSPEC() { echo "ResourceType=$1,Tags=[{Key=$TAG_KEY,Value=$TAG_VAL},{Key=Name,Value=rtm-flink-bench4-$2}]"; }
rts() { for s in $PRIV; do aws ec2 describe-route-tables --filters Name=association.subnet-id,Values="$s" --query 'RouteTables[0].RouteTableId' --output text; done | sort -u; }
mine_nat() { aws ec2 describe-nat-gateways --filter Name=vpc-id,Values="$VPC" Name=tag:$TAG_KEY,Values=$TAG_VAL Name=state,Values=pending,available --query 'NatGateways[0].NatGatewayId' --output text; }

case "${1:-status}" in
up)
  RTS=$(rts | tr '\n' ' ')
  echo "vpc=$VPC nat-subnet=$PUB private-route-tables=$RTS"
  EP=$(aws ec2 describe-vpc-endpoints --filters Name=vpc-id,Values="$VPC" Name=service-name,Values=com.amazonaws.us-east-1.s3 Name=vpc-endpoint-type,Values=Gateway --query 'VpcEndpoints[0].VpcEndpointId' --output text)
  if [ "$EP" = None ]; then
    EP=$(aws ec2 create-vpc-endpoint --vpc-id "$VPC" --vpc-endpoint-type Gateway --service-name com.amazonaws.us-east-1.s3 \
      --route-table-ids $RTS --tag-specifications "$(TAGSPEC vpc-endpoint s3)" --query 'VpcEndpoint.VpcEndpointId' --output text)
  fi
  echo "s3 gateway endpoint: $EP"
  NAT=$(mine_nat)
  if [ "$NAT" = None ]; then
    ALLOC=$(aws ec2 allocate-address --domain vpc --tag-specifications "$(TAGSPEC elastic-ip nat)" --query AllocationId --output text)
    NAT=$(aws ec2 create-nat-gateway --subnet-id "$PUB" --allocation-id "$ALLOC" --tag-specifications "$(TAGSPEC natgateway nat)" --query 'NatGateway.NatGatewayId' --output text)
  fi
  aws ec2 wait nat-gateway-available --nat-gateway-ids "$NAT"
  echo "nat gateway: $NAT (available)"
  for rt in $RTS; do
    aws ec2 replace-route --route-table-id "$rt" --destination-cidr-block 0.0.0.0/0 --nat-gateway-id "$NAT" 2>/dev/null \
      || aws ec2 create-route --route-table-id "$rt" --destination-cidr-block 0.0.0.0/0 --nat-gateway-id "$NAT" >/dev/null
    echo "route $rt 0.0.0.0/0 -> $NAT"
  done
  ;;
down)
  NAT=$(mine_nat)
  if [ "$NAT" != None ]; then
    ALLOC=$(aws ec2 describe-nat-gateways --nat-gateway-ids "$NAT" --query 'NatGateways[0].NatGatewayAddresses[0].AllocationId' --output text)
    aws ec2 delete-nat-gateway --nat-gateway-id "$NAT" >/dev/null
    aws ec2 wait nat-gateway-deleted --nat-gateway-ids "$NAT"
    aws ec2 release-address --allocation-id "$ALLOC" && echo "deleted $NAT, released $ALLOC"
  fi
  for ep in $(aws ec2 describe-vpc-endpoints --filters Name=vpc-id,Values="$VPC" Name=tag:$TAG_KEY,Values=$TAG_VAL --query 'VpcEndpoints[].VpcEndpointId' --output text); do
    aws ec2 delete-vpc-endpoints --vpc-endpoint-ids "$ep" >/dev/null && echo "deleted endpoint $ep"
  done
  # The private default routes go back to blackhole, as they were before `up`.
  ;;
status)
  echo "nat=$(mine_nat)"
  for rt in $(rts); do aws ec2 describe-route-tables --route-table-ids "$rt" --query 'RouteTables[0].Routes[].[DestinationCidrBlock,DestinationPrefixListId,NatGatewayId,GatewayId,State]' --output text | sed "s/^/$rt /"; done
  ;;
esac
