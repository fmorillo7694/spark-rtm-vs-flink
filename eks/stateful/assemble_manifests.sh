#!/usr/bin/env bash
# Assemble the flat manifest directory the in-cluster runner reads (the `stateful-runner`
# ConfigMap, or MANIFESTS for a dry run): the stateful templates plus the UNCHANGED stateless p6
# manifests under a p6- prefix, and the two p6 KafkaTopics cut out of eks/p6_kafka_strimzi.yaml
# (the Kafka cluster itself is never re-applied by the runner).
#
# Usage: eks/stateful/assemble_manifests.sh OUT_DIR
set -euo pipefail
cd "$(dirname "$0")/../.."
OUT="${1:?usage: assemble_manifests.sh OUT_DIR}"
mkdir -p "$OUT"
cp eks/stateful/run_matrix_incluster.sh eks/stateful/spark_progress.py eks/stateful/sampler.py eks/jobs/stateful/*.yaml "$OUT/"
for f in spark-rtm flink-datastream flink-sql pyflink pyspark-rtm; do cp "eks/jobs/p6/$f.yaml" "$OUT/p6-$f.yaml"; done
cp eks/jobs/load.yaml "$OUT/p6-load.yaml"
python3 - eks/p6_kafka_strimzi.yaml "$OUT/p6-topics.yaml" <<'PY'
import re, sys
docs = re.split(r"(?m)^---\s*$", open(sys.argv[1]).read())
topics = [d.strip("\n") for d in docs if re.search(r"(?m)^kind:\s*KafkaTopic\s*$", d)]
assert len(topics) == 2, f"expected 2 KafkaTopics, found {len(topics)}"
open(sys.argv[2], "w").write("# cut from eks/p6_kafka_strimzi.yaml by assemble_manifests.sh\n"
                             + "\n---\n".join(topics) + "\n")
PY
ls "$OUT" | tr '\n' ' '; echo
