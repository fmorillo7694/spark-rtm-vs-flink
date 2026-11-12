# Load-generator image: producer + latency consumer, run as K8s Jobs for the EKS run.
# Also the in-cluster stateful matrix runner (eks/stateful/runner-job.yaml): kubectl (in-cluster
# service-account auth) + boto3 (IRSA uploads to S3) + topic_capture/s3_sync helpers.
# Build context = repo root.
FROM python:3.12-slim

ARG TARGETARCH=amd64
ARG KUBECTL_VERSION=v1.36.0
RUN apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends curl ca-certificates \
 && curl -fsSLo /usr/local/bin/kubectl "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${TARGETARCH}/kubectl" \
 && curl -fsSLo /tmp/kubectl.sha256 "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${TARGETARCH}/kubectl.sha256" \
 && echo "$(cat /tmp/kubectl.sha256)  /usr/local/bin/kubectl" | sha256sum -c - \
 && chmod +x /usr/local/bin/kubectl && rm -f /tmp/kubectl.sha256 \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir "confluent-kafka>=2.5,<3" "boto3==1.43.126" "numpy==2.3.5"
COPY common/producer.py common/joindata.py common/latency_consumer.py common/soak_consumer.py \
     common/topic_capture.py common/s3_sync.py /app/
WORKDIR /app
