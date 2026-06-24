#!/usr/bin/env bash
# Build and push the engine + load-generator images to ECR for the EKS run.
# Images:
#   rtm-bench/spark   apache/spark:4.1.2 + the Scala RTM jar + kafka connector jars
#   rtm-bench/flink   flink:2.2.0 + the Java DataStream shaded jar
#   rtm-bench/pyflink flink:2.2.0 + python + apache-flink + sql-kafka connector + job
#   rtm-bench/load    python + confluent-kafka + producer/consumer (distributed load)
set -euo pipefail
cd "$(dirname "$0")/.."

export AWS_PROFILE="${AWS_PROFILE:-default}"
export AWS_REGION=us-east-1
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
REGISTRY="${ACCOUNT}.dkr.ecr.${AWS_REGION}.amazonaws.com"
TAG="${TAG:-v1}"

echo "==> ECR login ($REGISTRY)"
aws ecr get-login-password --region "$AWS_REGION" | docker login --username AWS --password-stdin "$REGISTRY"

ensure_repo() { aws ecr describe-repositories --repository-names "$1" >/dev/null 2>&1 || aws ecr create-repository --repository-name "$1" >/dev/null; }
for r in rtm-bench/spark rtm-bench/flink rtm-bench/pyflink rtm-bench/load; do ensure_repo "$r"; done

# EKS nodes are arm64? No — m5 = amd64. Build amd64 images explicitly.
PLATFORM=linux/amd64

echo "==> [1/4] spark image (Scala + Java RTM jars baked in)"
( cd spark/scala-rtm && sbt -batch package >/tmp/sbt_build.log 2>&1 )
( cd spark/java-rtm && mvn -q -B package >/tmp/mvn_java_build.log 2>&1 )
# fail fast if either jar is missing (the Dockerfile COPYs both)
test -f spark/scala-rtm/target/scala-2.13/scala-rtm_2.13-0.1.0.jar || { echo "scala jar missing"; exit 1; }
test -f spark/java-rtm/target/java-rtm.jar || { echo "java jar missing"; exit 1; }
docker buildx build --platform $PLATFORM -t "$REGISTRY/rtm-bench/spark:$TAG" -f eks/images/spark.Dockerfile . --push

echo "==> [2/4] flink image (Java DataStream jar baked in)"
( cd flink/java-datastream && mvn -q -B package >/tmp/mvn_build.log 2>&1 )
docker buildx build --platform $PLATFORM -t "$REGISTRY/rtm-bench/flink:$TAG" -f eks/images/flink.Dockerfile . --push

echo "==> [3/4] pyflink image"
docker buildx build --platform $PLATFORM -t "$REGISTRY/rtm-bench/pyflink:$TAG" -f flink/pyflink/Dockerfile flink/pyflink --push

echo "==> [4/4] load image (producer/consumer)"
docker buildx build --platform $PLATFORM -t "$REGISTRY/rtm-bench/load:$TAG" -f eks/images/load.Dockerfile . --push

echo "==> pushed to $REGISTRY (tag $TAG)"
echo "REGISTRY=$REGISTRY" > eks/.registry.env
echo "TAG=$TAG" >> eks/.registry.env
