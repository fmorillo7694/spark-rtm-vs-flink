# Load-generator image: producer + latency consumer, run as K8s Jobs for the EKS run.
# Build context = repo root.
FROM python:3.12-slim

RUN pip install --no-cache-dir "confluent-kafka>=2.5,<3"
COPY common/producer.py common/latency_consumer.py /app/
WORKDIR /app
