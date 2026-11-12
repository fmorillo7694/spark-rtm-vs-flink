#!/usr/bin/env bash
# Random +50% load spikes (~10 min each) woven through the running 4h soak, with FINE-GRAINED
# sampling (every 30s) so the spike signature isn't averaged away by the soak's 30-min windows.
#
# Measures, every 30s:
#   - input-events end offset (to derive ingest rate)
#   - each engine's CONSUMER-GROUP LAG on input-events (how far behind it is = backpressure)
#   - each engine's output offset (to derive output throughput / drain)
# And logs exactly when each spike starts/ends so we can correlate latency + lag to the surge.
#
# Spikes: pick a random gap (8-20 min) between spikes, each spike = +4 burst producers
# (+50k/s → 150k/s) for ~10 min, then delete them. Runs for SPIKE_WINDOW_SECS total.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; cd "$HERE/../.."
export KUBECONFIG="$HERE/../kubeconfig" AWS_PROFILE="${AWS_PROFILE:-default}" AWS_REGION=us-east-1
ACCOUNT="${ACCOUNT:-$(aws sts get-caller-identity --query Account --output text)}"
source eks/.registry.env
RES=results/eks/soak; mkdir -p "$RES"
BROKER=bench-brokers-0
SPIKE_WINDOW_SECS="${SPIKE_WINDOW_SECS:-11000}"   # ~3h of the 4h soak (leave tail clean)
SPIKE_DUR="${SPIKE_DUR:-600}"                     # ~10 min per spike
SAMPLES="$RES/spike_samples.tsv"
EVENTS="$RES/spike_events.tsv"
RESFINE="$RES/spike_resources.tsv"   # CPU/mem at the SAME 30s cadence (not 30-min)
: > "$SAMPLES"; echo -e "ts\tinput_end\tspark_grp_lag\tflink_grp_lag\tspark_out\tflink_out\tspiking" >> "$SAMPLES"
: > "$EVENTS"; echo -e "ts\tevent" >> "$EVENTS"
: > "$RESFINE"; echo -e "ts\tns\tcores\tmem_gb\tspiking" >> "$RESFINE"

# Flink uses a Kafka consumer group on input-events (flink-stateless) → group LAG is directly
# observable. Spark Structured Streaming manages offsets in its own checkpoint, NOT a Kafka
# group, so Spark has no visible group lag — for Spark we infer keep-up from OUTPUT RATE
# (Δoutput/Δt) vs input rate. So we track BOTH: Flink group-lag (direct) + both engines'
# output offsets (rate-derived backpressure, works for both).
grp() { kubectl exec -n kafka "$BROKER" -- bash -c \
  "/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list 2>/dev/null" 2>/dev/null; }
SPARK_GRP=""   # none — Spark uses checkpoint offsets, not a Kafka group
FLINK_GRP=$(grp | grep -x "flink-stateless" | head -1)
echo "==> consumer groups: spark='(checkpoint-managed, no group)' flink='$FLINK_GRP'"
echo -e "$(date +%s)\tgroups spark=$SPARK_GRP flink=$FLINK_GRP" >> "$EVENTS"

end_offset() { kubectl exec -n kafka "$BROKER" -- bash -c \
  "/opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic $1 2>/dev/null | awk -F: '{s+=\$3} END{print s+0}'" 2>/dev/null; }
group_lag() { [ -z "$1" ] && { echo 0; return; }
  kubectl exec -n kafka "$BROKER" -- bash -c \
    "/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group $1 2>/dev/null | awk 'NR>1 && \$6 ~ /^[0-9]+\$/ {s+=\$6} END{print s+0}'" 2>/dev/null; }

start=$(date +%s); spiking=0; spike_end=0; next_spike=$(( start + (RANDOM % 300) + 120 ))
while :; do
  now=$(date +%s); [ $(( now - start )) -ge "$SPIKE_WINDOW_SECS" ] && break
  # ---- spike control ----
  if [ "$spiking" -eq 0 ] && [ "$now" -ge "$next_spike" ]; then
    echo "==> [$(date +%H:%M:%S)] SPIKE START (+50% for ${SPIKE_DUR}s)"
    sed -e "s#__REGISTRY__#$REGISTRY#g" -e "s#__TAG__#$TAG#g" -e "s#__SECS__#$SPIKE_DUR#g" \
      eks/soak/burst.yaml | kubectl apply -f - >/dev/null 2>&1
    echo -e "${now}\tSPIKE_START" >> "$EVENTS"; spiking=1; spike_end=$(( now + SPIKE_DUR ))
  fi
  if [ "$spiking" -eq 1 ] && [ "$now" -ge "$spike_end" ]; then
    echo "==> [$(date +%H:%M:%S)] SPIKE END"
    kubectl delete job soak-burst -n kafka --ignore-not-found >/dev/null 2>&1
    echo -e "${now}\tSPIKE_END" >> "$EVENTS"; spiking=0
    next_spike=$(( now + (RANDOM % 720) + 480 ))   # next spike in 8-20 min
  fi
  # ---- sample lag/throughput ----
  ie=$(end_offset input-events); so=$(end_offset output-spark); fo=$(end_offset output-flink)
  sl=$(group_lag "$SPARK_GRP"); fl=$(group_lag "$FLINK_GRP")
  echo -e "${now}\t${ie}\t${sl}\t${fl}\t${so}\t${fo}\t${spiking}" >> "$SAMPLES"
  # ---- sample CPU/mem at the SAME 30s cadence (summed engine pods per namespace) so the
  #      spike's resource signature isn't averaged away by the 30-min soak sampler ----
  for ns in spark flink; do
    kubectl top pods -n "$ns" --no-headers 2>/dev/null | grep -E "rtm-bench|flink-bench" \
      | awk -v t="$now" -v n="$ns" -v sp="$spiking" \
        '{c+=$2; m+=$3} END{printf "%s\t%s\t%.2f\t%.2f\t%s\n", t, n, c/1000, m/1024, sp}' >> "$RESFINE"
  done
  sleep 30
done
kubectl delete job soak-burst -n kafka --ignore-not-found >/dev/null 2>&1
echo "==> spike window done $(date). samples=$SAMPLES events=$EVENTS"
