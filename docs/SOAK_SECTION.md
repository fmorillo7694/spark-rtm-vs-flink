<!--
  PASTE-READY Medium section: the 2-hour soak + spike test. Drop this in after the
  "Why the tail diverges" / before "So, which one?" section of the streaming article.
  Images to upload at the marked spots (from docs/charts/):
    12_soak_stability.png   T10_soak.png   13_spike_resilience.png   T11_spike.png
-->

## But does it hold for hours? A 2-hour soak

Five-minute benchmarks hide the things that actually page you at 3 a.m.: latency creeping up,
memory leaking, GC degrading, throughput decaying. A median that's tied at minute 5 and a tail
that's tied at hour 2 are different claims. So I ran both engines **concurrently for 2 hours**
on the same EKS setup — Java Spark RTM and Java Flink, 6 partitions / 7 cores each, ~100k
events/sec, durable S3 checkpoints — and snapshotted the latency distribution every 30 minutes.

![2-hour soak: pipeline p99 per 30-min window](docs/charts/12_soak_stability.png)

Nothing drifts. Over four 30-minute windows the median stays flat (~7 ms for both), and the
tail stays exactly where the short runs put it: **Spark RTM ~540–660 ms, Flink ~60 ms.** The
~10× gap isn't a warm-up artifact — it's a structural property of synchronous (RTM) vs
asynchronous (Flink) checkpointing, and it's rock-steady for two hours.

![Soak latency per window](docs/charts/T10_soak.png)

Memory was equally boring, which is the point: **no leak** on either side — Spark held ~8 GB,
Flink ~5.5 GB the whole run (Flink ~30% lighter, as in the short runs). CPU flat at ~1 core each.

## Throwing spikes at it

Steady load is the easy case. So I also injected **four random +50% traffic spikes** (~10
minutes each, pushing ingest from 100k to 150k events/sec) and watched how each engine coped.

![Spike resilience: +50% bursts, Flink lag drains each time](docs/charts/13_spike_resilience.png)

Both absorbed every spike. The instant load jumped, each engine's **output rate rose from ~33k
to ~50k/sec** to match — neither built a permanent backlog, and across the whole run they
processed the same number of records to within 0.1% (same input topic, same filter). Flink's
consumer lag climbed to ~7.8M records at each spike's peak, then **drained back under 1M within
minutes** of the spike ending — bounded backpressure, full recovery, every time.

![Spike resilience summary](docs/charts/T11_spike.png)

One honest measurement note: my single latency *consumer* couldn't always keep up with the 50k/s
output bursts, which inflated the *end-to-end* numbers during spikes — but it did so identically
for both engines, because that's the benchmark's reader lagging, not the engines. The
engine-attributable metric is **pipeline latency** (stamped by the engine itself, `out_ts − ts`),
which is what the charts above use.

**The takeaway:** everything the 5-minute runs showed — median tie, ~10× tail gap, Flink's
lighter memory — holds across 2 hours of continuous load and survives repeated +50% surges with
full recovery. The difference between these engines is stable and structural, not a transient.
