#!/usr/bin/env bash
# Install operators + Kafka on the EKS cluster:
#   - Strimzi (Kafka) operator + 3-broker KRaft cluster
#   - Flink Kubernetes operator (+ cert-manager dependency)
#   - Spark Kubernetes operator
# Run after the cluster is up and kubectl context points at it.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

export AWS_PROFILE="${AWS_PROFILE:-default}"
export KUBECONFIG="$HERE/kubeconfig"
export AWS_REGION=us-east-1

echo "==> [1/5] metrics-server (pod CPU/mem). Installed via EKS addon (see cluster.yaml)."
# EKS kubelet serving certs aren't signed by the cluster CA -> metrics-server needs this.
# Patch whichever metrics-server deployment exists (addon-managed); ignore if absent.
kubectl patch deployment metrics-server -n kube-system --type=json \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]' 2>/dev/null || true

echo "    default gp3 (encrypted) StorageClass for EBS-CSI dynamic volumes"
cat <<'EOF' | kubectl apply -f -
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: gp3
  annotations:
    storageclass.kubernetes.io/is-default-class: "true"
provisioner: ebs.csi.aws.com
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
parameters:
  type: gp3
  encrypted: "true"
EOF
kubectl patch storageclass gp2 -p '{"metadata":{"annotations":{"storageclass.kubernetes.io/is-default-class":"false"}}}' 2>/dev/null || true

echo "==> [2/5] Strimzi operator"
kubectl create namespace kafka --dry-run=client -o yaml | kubectl apply -f -
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka 2>/dev/null || \
  kubectl apply -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
echo "    waiting for Strimzi operator..."
kubectl -n kafka rollout status deploy/strimzi-cluster-operator --timeout=300s
echo "    waiting for Strimzi CRDs to register..."
kubectl wait --for=condition=Established crd/kafkas.kafka.strimzi.io crd/kafkanodepools.kafka.strimzi.io crd/kafkatopics.kafka.strimzi.io --timeout=120s

echo "==> [3/5] Kafka cluster (3 brokers, KRaft)"
kubectl apply -f 02_kafka_strimzi.yaml
echo "    waiting for Kafka to be ready (this takes a few min)..."
kubectl -n kafka wait kafka/bench --for=condition=Ready --timeout=600s

echo "==> [4/5] cert-manager (Flink operator dependency)"
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.16.2/cert-manager.yaml
kubectl -n cert-manager rollout status deploy/cert-manager-webhook --timeout=300s

echo "==> [5/5] Flink + Spark Kubernetes operators (helm)"
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.10.0/ 2>/dev/null || true
helm repo add spark-operator https://kubeflow.github.io/spark-operator 2>/dev/null || true
helm repo update
kubectl create namespace flink --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace spark --dry-run=client -o yaml | kubectl apply -f -
helm upgrade --install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
  -n flink --set watchNamespaces="{flink}"
helm upgrade --install spark-operator spark-operator/spark-operator \
  -n spark --set spark.jobNamespaces="{spark}" --set webhook.enable=true

echo "==> operators installed:"
kubectl get pods -n kafka
kubectl get pods -n flink
kubectl get pods -n spark
