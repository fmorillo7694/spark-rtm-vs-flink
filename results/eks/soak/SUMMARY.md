# 2-hour streaming soak — Spark RTM vs Flink (Java), concurrent, EKS

Both engines ran **simultaneously** for 2 hours, 6 partitions / 7 cores each, 60s durable S3
checkpoints, shared input topic, separate output topics. ~100k/s baseline with **four random
+50% spikes (~10 min each → 150k/s)**. Latency snapshot every 30 min; lag/throughput/CPU/mem
sampled every 30 s. (A first attempt crashed a Kafka broker on disk-full at ~100 min — fixed
with aggressive retention + 250Gi volumes; this is the clean re-run.)

## Steady-state stability (pipeline latency = the engine's own work)

| window (~30min each) | Spark p50 / p99 / max | Flink p50 / p99 / max | eps |
|---|--:|--:|--:|
| 0 | 6.7 / 613 / 1773 ms | 6.6 / 60 / 703 ms | 33k |
| 1 | 7.1 / 541 / 1452 ms | 7.0 / 63 / 427 ms | 40k |
| 2 | 7.1 / 664 / 2014 ms | 7.0 / 60 / 593 ms | 40k |
| 3 | 7.1 / 655 / 2435 ms | 7.0 / 60 / 424 ms | 42k |

**Verdict — no drift over 2 hours:**
- **Median dead flat**: Spark 6.7→7.1 ms, Flink 6.6→7.0 ms. Zero creep.
- **Tail flat**: Spark p99 540–664 ms (no upward trend), Flink p99 **59.7–62.8 ms** (essentially
  constant). The ~10× tail gap is **structural and persistent**, not a warm-up artifact.
- Throughput rose +26% (33k→42k eps) purely from spike traffic; both tracked it identically.
- **No memory leak**: Spark steady ~8.1 GB, Flink ~5.5 GB across the whole run (Flink ~30% less).
  CPU flat (~1.3c Spark, ~1.1c Flink mean).

## Spike resilience (4× +50%, sampled every 30s)

Every spike: input 100k→~150k/s. Both engines' **output rate immediately rose 33k→~50k/s** to
match — neither built a sustained backlog. Records processed stayed **tied within ~0.1%** the
entire run (same input, same filter).
- **Flink consumer-group lag**: baseline ~2.8M, spike peak ~7.8M, then **drained back to <1M
  within minutes of each spike ending** — bounded backpressure, full recovery, every time.
- **Spark** (no Kafka group; offset-derived backlog): flat throughout — absorbed each spike.
- CPU/mem during spikes barely moved (Spark ~8 GB, Flink ~5.5 GB) — no resource blowup.

## ⚠️ Honest caveat: end-to-end p99 is a CONSUMER artifact, not an engine result
e2e p99 jumped to ~44–68 s in windows 1–3 — but **identically for BOTH engines**, while pipeline
latency stayed fine (Spark ~650 ms, Flink ~60 ms). e2e = `consumer_read_time − event_ts`, so when
the single measuring consumer couldn't keep up with the spike's 50k/s output burst, records
queued in the output topic and *read* late — inflating e2e equally on both sides. It measures the
**benchmark's consumer**, not the engines. The fair, engine-attributable metric is **pipeline
latency** (out_ts − ts, stamped by the engine itself), which is what the stability table above
uses. (Fix for any future soak: parallelize the latency consumer to match peak output rate.)

## Bottom line
Over 2 hours of continuous load plus four +50% spikes, **both engines are stable — no drift, no
leak, full spike recovery, tied throughput** — and the headline 5-minute findings hold at
duration: **median tied (~7 ms), Flink's tail ~10× tighter (~60 ms vs ~600 ms), Flink ~30% less
memory.** The tail gap is a persistent structural property of synchronous (RTM) vs asynchronous
(Flink) checkpointing, confirmed across 2 hours and repeated surges.
