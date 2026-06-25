# Spark RTM — validation findings (probe runs, native arm64)

Probe = 30s producer @ 10k/s, 35s consumer, 5s warmup. Local Docker, Apple Silicon
(native, NOT emulated). Filter passes ~33% (purchase/add_to_cart) → ~3k evt/s output.

## Latency (ms)

| mode             | pipeline p50 | pipeline p95 | pipeline p99 | e2e p50 | e2e p95 | e2e p99 |
|------------------|-------------:|-------------:|-------------:|--------:|--------:|--------:|
| RTM (5s trigger) |         5.42 |         7.62 |        56.85 |    7.90 |   10.55 |   59.86 |
| micro-batch      |        57.91 |        92.74 |       114.92 |   60.48 |   95.67 |  118.10 |

**Floor reduction: e2e p50 60ms → 8ms ≈ 7.6×.** Confirms the article's "100ms+ batch
floor → tens of ms" claim. Our RTM e2e p50 (8ms) is BETTER than the article's 31ms —
consistent with running native arm64 vs the author's amd64-under-emulation laptop, and
our tail is far tighter (p99 ~60ms vs his ~2040ms).

## Claims validated
- RTM headline (sub-100ms, tens-of-ms median): **TRUE**, reproduced.
- Trigger duration is a checkpoint cadence, not a latency target: **TRUE** — 5s trigger,
  yet records flow in ~8ms.
- PySpark native `realTime=` kwarg absent on stable 4.1.2: **TRUE** (trigger params =
  processingTime, once, continuous, availableNow).
- py4j bridge reaches RTM on 4.1.2: **TRUE** — `Trigger.RealTime('5 seconds')` →
  `RealTimeTrigger(5000)`, live query started.
- Stateless-only / update mode / no AQE / checkpoint required: **TRUE** — confirmed by
  `maxOffsetsPerTrigger is not compatible with real time mode` and update-mode requirement.

## New gotcha discovered (not in the article)
`current_timestamp()` is **frozen at epoch/batch start**. Under RTM's long-running tasks
that timestamp is stamped when the task launched, BEFORE rows are written → it produced
out_ts ~2.3s EARLIER than ts (negative pipeline latency). Fix: stamp out_ts with a
per-row non-deterministic UDF (`System.currentTimeMillis()`), which the Scala job uses.
Anyone measuring RTM write-time latency with `current_timestamp()` will get garbage.

## Resource note
RTM holds one core per input partition continuously (3 partitions → 3 cores pinned even
when idle). Submitted with spark.cores.max=6, executor.cores=3.
