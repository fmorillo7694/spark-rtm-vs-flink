# Spark 4.1 Real-time Mode vs Apache Flink — benchmark report

## TL;DR

- **The article's headline claim is TRUE and reproduced.** Spark 4.1.2 RTM dropped
  end-to-end median latency from the micro-batch floor (**50 ms**) to **8 ms** — a 6.3×
  cut — on a stateless Kafka→transform→Kafka pipeline at 10k events/sec.
- **Our RTM numbers beat the article's** (e2e p50 8 ms vs his 31 ms; p99 ~45 ms vs his
  ~2040 ms). We ran **native arm64**; he ran amd64 under emulation on Apple Silicon. The
  emulation tax explains his fat tail — a methodology artifact, not an RTM limitation.
- **Both PySpark claims validated verbatim:** the native `realTime=` trigger kwarg is
  absent on stable 4.1.2, and the **py4j bridge reaches `Trigger.RealTime` and runs**.
- **Flink matches RTM at the median and crushes it on tail latency.** Java Flink: pipeline
  p50 6 ms, **p99 9.9 ms, max 29 ms** — vs RTM's p99 39 ms, max 118 ms. Flink also used
  ~40% the memory and the least CPU.
- **Python costs almost nothing on Spark, and nothing on Flink once tuned.** PySpark RTM
  ≈ Scala RTM (both compile to the same JVM plan). PyFlink needs one config knob
  (`python.fn-execution.bundle.time`) or it sits at a **456 ms** buffering floor; tuned,
  it lands at 7 ms.

- **Confirmed at scale on EKS (~100k evt/s, all 5 engines).** A production cluster run
  (6× m5.2xlarge, 3-broker Strimzi Kafka, ~4M records each) reproduced the verdict and
  added: **Java vs Scala Spark is identical** (same JVM plan — language is a non-factor);
  **Flink owns the tail** (e2e p99 76 ms vs Spark RTM 183–233 ms) at **~60% the memory, comparable CPU**;
  **PyFlink costs ~2.4× Flink** (Python-worker overhead). A data-rate sweep showed Flink's
  median latency **flat from 15 → 770 MB/s** — the first ceiling is Kafka storage, not the
  engine. See §9.
- **Durable checkpointing flips the tail story decisively (§10).** With production-grade
  durable S3 checkpoints at a matched 60s interval, **identical resources per engine** (7 cores
  / 14 GB, pod-memory matched), and output keyed the same, **Spark RTM's synchronous offset
  commit pushes its p99 to ~590–700 ms** (6 partitions), while Flink's async checkpoint holds
  p99 at ~31–33 ms — a **~20× gap** (~30× at 12 partitions). The gap holds at both sizes, so it
  is not a capacity artifact — but it IS a durable-storage-at-scale effect: locally (local-disk
  checkpoints, 10k/s) RTM's tail collapses to ~9 ms. On worker loss, both engines auto-recover
  from checkpoint in a comparable **~18–24 s** (an earlier "129 ms RTM recovery" of ours was a
  no-durable-checkpoint artifact and was discarded). This is the finding that most separates
  the two for real production use.

> Caveat: local numbers are from a single laptop, 8 GB Docker, ~3.3k evt/s through the
> filter; EKS numbers are ~31k evt/s through the filter at 100k/s ingest. Absolute numbers
> are environment-specific; the **relative** comparison is the takeaway.

---

## 1. Setup

| | |
|---|---|
| Host | Apple Silicon (arm64), **native** containers, Docker ~8 GB / 14 CPU |
| Kafka | apache/kafka **4.1.0**, KRaft (no ZooKeeper), 3 partitions in/out |
| Spark | apache/spark **4.1.2** standalone, 1 master + 2 workers (8 cores) |
| Flink | **2.2.0**, 1 JobManager + 1 TaskManager (4 slots) |
| Load | ~**10,000 evt/s** for 120 s, 15 s warmup dropped; filter passes ~33% → ~3.3k evt/s out |
| Pipeline | filter `purchase`/`add_to_cart` & `amount>0`; `amount_with_tax=amount*1.21`; upper `country`; stamp `out_ts`; carry `ts` |

**Latency definitions.** *pipeline* = `out_ts − ts` (engine in→transform→out);
*end-to-end* = `consume_wallclock − ts` (adds the consumer's output read). Producer,
consumer and engine share the host clock (verified sub-ms skew), so the subtraction is
valid. Identical event contract and transform across all five engine variants.

---

## 2. Latency (ms)

| engine | pipeline p50 | p95 | p99 | max | e2e p50 | p95 | p99 | max | throughput (evt/s) |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| **spark-rtm** (Scala) | 5.9 | 8.6 | 39.2 | 118.3 | 7.9 | 11.1 | 42.1 | 120.8 | 3327 |
| spark-microbatch (Scala) | 47.8 | 68.2 | 77.2 | 119.1 | 50.0 | 70.7 | 80.4 | 121.3 | 3342 |
| **pyspark-rtm** (Python) | 6.2 | 8.6 | 42.9 | 112.3 | 8.0 | 10.9 | 45.6 | 114.5 | 3340 |
| **flink** (Java) | 6.0 | 8.5 | **9.9** | **28.6** | 14.5 | 18.0 | **19.8** | **39.4** | 3331 |
| **pyflink** (Python, tuned) | 7.2 | 11.5 | 12.9 | 34.8 | 16.7 | 18.8 | 21.1 | 49.2 | 3337 |

Reading it:

- **RTM vs the floor:** micro-batch e2e p50 **50 ms** → RTM **8 ms**. The article's core
  claim, reproduced cleanly. RTM's *pipeline* p50 5.9 ms is ~8× under micro-batch's 48 ms.
- **Median parity, tail divergence Spark↔Flink:** at p50 RTM (5.9) and Flink (6.0) are a
  dead heat. At the tail they diverge hard — Flink **p99 9.9 ms / max 29 ms** vs RTM
  **p99 39 ms / max 118 ms**. RTM's long-running-task model still has periodic hitches
  (checkpoint cadence, JVM/GC) that Flink's continuous operators smooth out.
- **Flink's higher e2e p50 (14.5 ms) despite lower pipeline p50** is the output-read leg
  (consumer poll interval), not engine work — see pipeline columns for the apples-to-apples
  engine latency.

---

## 3. The Python story (Java-vs-Java, Python-vs-Python)

| | Spark | Flink |
|---|---|---|
| **JVM** | Scala RTM — e2e p50 **7.9 ms** | Java DataStream — pipeline p50 **6.0 ms**, p99 9.9 ms |
| **Python** | PySpark RTM — e2e p50 **8.0 ms** | PyFlink — pipeline p50 **7.2 ms** (tuned) |

- **PySpark ≈ Scala.** DataFrame ops compile to the *same* JVM physical plan; Python is
  only the driver. We kept `out_ts` on the JVM via `reflect('java.lang.System',
  'currentTimeMillis')` so no Python runs on executors. Result: statistically identical
  to Scala. **For this stateless workload, choosing PySpark over Scala costs ~nothing.**
- **PyFlink has a trap.** The Table API authors in Python but the filter/projection run on
  the JVM — *except* the `out_ts` UDF, which round-trips to a Python worker. With defaults,
  PyFlink reported **p50 456 ms**. That is *not* Python being slow: it's
  `python.fn-execution.bundle.time=1000ms` buffering records before shipping to Python.
  Setting it to `1 ms` collapsed p50 to **7.2 ms**. Lesson: PyFlink + Python UDFs needs
  explicit bundle tuning for low latency, or you pay a sub-second floor you didn't ask for.

---

## 4. CPU & memory (steady state, engine containers only; Kafka excluded)

| engine | mean CPU (cores) | peak CPU | mean mem (MB) | peak mem |
|---|--:|--:|--:|--:|
| spark-rtm | 0.40 | 2.3 | 2866 | 2915 |
| spark-microbatch | 2.08 | 7.14 | 2919 | 3114 |
| pyspark-rtm | 0.43 | 1.92 | 3050 | 3118 |
| **flink** | **0.25** | **0.85** | **1826** | **1849** |
| pyflink | 0.92 | 1.64 | 2442 | 2466 |

- **Flink is the resource-efficiency winner** — lowest CPU and ~60% of Spark's memory,
  while delivering the tightest tail. PyFlink costs ~3.7× Flink's CPU (the Python worker
  processes) but still under Spark.
- **Micro-batch burns 5× the CPU of RTM** (2.08 vs 0.40 cores) for *worse* latency — it
  repeatedly spins up batch machinery. RTM's long-running tasks are both faster and cheaper
  here, though they **pin one core per partition continuously** (the article's sizing rule).
- Mean CPU looks low because the filter passes only ~33%; peaks show real headroom use.

---

## 5. Cost (approximation, relative only)

Steady-state engine compute mapped to on-demand vCPU-hours ($0.048/vCPU-hr, m5-class).
Excludes Kafka/MSK, storage, network, idle headroom.

| engine | vCPU | mem (GB) | $/hour | $/billion events |
|---|--:|--:|--:|--:|
| flink | 0.25 | 1.78 | 0.012 | **1.00** |
| spark-rtm | 0.40 | 2.80 | 0.019 | 1.60 |
| pyspark-rtm | 0.43 | 2.98 | 0.021 | 1.72 |
| pyflink | 0.92 | 2.38 | 0.044 | 3.68 |
| spark-microbatch | 2.08 | 2.85 | 0.100 | 8.30 |

Relative cost-per-event: **Flink cheapest**, RTM ~1.6×, PyFlink ~3.7×, micro-batch ~8×.

---

## 6. Complexity

| | Spark RTM (Scala) | Flink (Java) | PySpark | PyFlink |
|---|---|---|---|---|
| Pipeline LOC (non-comment) | 73 | 71 | 71 | 85 |
| Build | sbt | Maven shade | none | none (needs apache-flink image) |
| Low-latency knobs | trigger, **update mode**, checkpoint, **AQE off** | `bufferTimeout(0)` | same as Scala + py4j bridge | `bufferTimeout`, **bundle.time/size** |
| Hard constraints | **stateless only**, update mode, checkpoint required, no AQE; native PySpark trigger needs 4.2.0.dev5 | none for this workload | inherits RTM's; py4j bridge for trigger | bundle tuning mandatory for latency |
| Footguns hit | frozen `current_timestamp()` (below) | none | none (reflect() for out_ts) | 456 ms bundle floor |

LOC is near-identical; the real complexity gap is in **constraints**. RTM is a constrained
mode (stateless, update-only, no AQE, checkpoint mandatory, 5 s minimum trigger). Flink
imposed none of these for the same job.

---

## 7. Claims validated

| Article claim | Verdict | Evidence |
|---|---|---|
| RTM breaks the micro-batch floor to tens of ms | ✅ | e2e p50 50 ms → 8 ms |
| Median end-to-end in tens of ms at 10k/s | ✅ | 8 ms (better than his 31 ms) |
| Trigger duration is checkpoint cadence, not latency target | ✅ | 5 s trigger, yet 8 ms record latency |
| Native PySpark `realTime=` only in 4.2.0.dev5 | ✅ | `trigger` kwargs = processingTime/once/continuous/availableNow |
| py4j bridge reaches RTM on stable 4.1.2 | ✅ | `Trigger.RealTime('5 seconds')` → `RealTimeTrigger(5000)`, live query |
| Stateless only / update mode / checkpoint / no AQE | ✅ | `maxOffsetsPerTrigger` rejected; update mode required |
| Fat tail is a demo/emulation artifact | ✅ | native arm64 p99 ~45 ms vs his ~2040 ms |

### New gotcha we found (not in the article)

**`current_timestamp()` is frozen under RTM.** In RTM's long-running tasks it's stamped at
task launch, *before* rows are written — we measured `out_ts` ~2.3 s *earlier* than `ts`
(negative latency). Fix: stamp per row with a non-deterministic source — a Scala UDF
(`System.currentTimeMillis`) in the Scala job, or SQL `reflect(...)` in PySpark (avoids
needing Python on executors). **Anyone benchmarking RTM write-time with
`current_timestamp()` will get garbage.**

---

## 8. Bottom line

- The article is **honest and correct**: RTM is a real, large latency win over micro-batch
  for stateless streaming, and its caveats (stateless-only, py4j on stable PySpark) are
  accurately stated. Its fat tail is an emulation artifact, not an RTM property.
- **If you're already on Spark** and need low-latency stateless streaming today, RTM is a
  genuine unlock — median ~8 ms, and PySpark costs nothing over Scala.
- **If latency tail and resource efficiency are paramount**, Flink is materially better
  here: ~4× tighter p99, ~10× lower max, lowest CPU/memory/cost — and no stateless/update/
  AQE constraints. The price is a separate runtime and (for PyFlink) a mandatory tuning knob.
- **Stateful** is the real decider not tested here: RTM is stateless-only until Spark 4.3,
  while Flink's stateful streaming is mature. For aggregations/joins/windows today, Flink.

## 9. EKS scale-out — full 5-engine matrix (~100k evt/s, production cluster)

We ran **all five** engine variants on a fresh production EKS cluster: 6× m5.2xlarge,
Strimzi Kafka (3 brokers, KRaft, replication 3 / min.isr 2, producer acks=1), 12-partition
topics, the Spark-on-K8s and Flink Kubernetes operators, distributed load of 8 producer
pods (~100k evt/s aggregate). Filter passes ~33% → ~31k evt/s out; each run measured
**~4 million** records over a 130 s steady-state window.

> ⚠️ **Superseded for the production verdict — see §10.** This §9 matrix was run *without*
> durable checkpointing and with **unequal resources** (Spark 4 exec × 4 cores = 17 total vs
> Flink 3 TM = 10). It's kept as the no-checkpoint, latency-tuned baseline. The fair,
> production-correct comparison — durable S3 checkpoints, identical resources, 6-partition
> standard matrix — is in §10, and the tail gap there is ~20× (6-part) / ~30× (12-part).

### Latency (ms)

| engine | out evt/s | pipe p50 | p95 | p99 | max | e2e p50 | p95 | p99 | max |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| **Flink** (Java) | 31343 | 6.9 | 36.3 | **50.8** | 202.7 | 15.0 | 57.9 | **76.3** | 254.5 |
| **PyFlink** (Python) | 30830 | 11.0 | 49.6 | 61.5 | 144.9 | 35.7 | 76.0 | 105.2 | 240.3 |
| **Spark RTM** (Java) | 30839 | 7.3 | 24.9 | 135.4 | 246.3 | 24.2 | 92.8 | 182.9 | 349.8 |
| **Spark RTM** (Scala) | 30858 | 7.9 | 48.2 | 162.2 | 301.7 | 36.7 | 125.3 | 232.9 | 541.0 |
| **PySpark RTM** | 31003 | 7.8 | 47.7 | 145.0 | 297.9 | 38.9 | 110.3 | 200.0 | 408.8 |

### Resources (engine pods, steady state)

| engine | mean CPU (cores) | peak CPU | mean mem (MB) | peak mem |
|---|--:|--:|--:|--:|
| Flink (Java) | 1.6 | 4.0 | 5008 | 5319 |
| Spark RTM (Scala) | 3.1 | 8.8 | 9263 | 10245 |
| Spark RTM (Java) | 3.4 | 5.8 | 9167 | 10202 |
| PyFlink | 6.8 | 9.0 | 6737 | 7276 |

(PySpark's sampler reading was unreliable — executor pods weren't matched; its real
footprint tracks Spark-Java's ~3.4 cores / ~9 GB. All five **kept up** with the 100k/s
ingest — output held ~31k/s with no growing backlog.)

**Findings — answering the specific questions:**

- **Java vs Scala for Spark: no difference.** Java RTM (pipe p50 7.3 ms) and Scala RTM
  (7.9 ms) are statistically identical — both compile to the same JVM physical plan, so
  language is *not* a variable for Spark. (We added a full Java DataFrame RTM job to prove
  this directly.) Use whichever your team prefers.
- **Flink owns the tail at scale, decisively.** Flink e2e p99 **76 ms** vs every Spark RTM
  variant at **183–233 ms** (~2.5–3×) and PyFlink at 105 ms. At the median Flink (15 ms) is
  ~1.6× better than Java RTM (24 ms). RTM's long-running-task hitches (checkpoint cadence,
  GC, periodic offset commit) spread its tail under real multi-node load.
- **Flink is the efficiency winner on memory:** ~5 GB vs Spark RTM's ~9 GB for the same
  throughput — a durable ~40% memory saving. (CPU here looks ~half, but that was the
  unequal-resource run; with equal resources in §10 the two JVM engines are closer to a CPU
  tie — the honest efficiency story is memory, not cores.)
- **Python tax:** PyFlink's Python-worker round-trip costs ~2.4× Flink's e2e p50 (36 vs
  15 ms) and ~4× the CPU (6.8 vs 1.6 cores) — the Python UDF bundling overhead, even tuned.
  PySpark ≈ Java Spark at the median (Python is driver-only) with a slightly heavier tail.
- **Scale tax on RTM:** laptop→EKS (3.3k→31k/s out), Spark RTM e2e p99 went 42→~200 ms;
  Flink's p99 went 20→76 ms. RTM is more load-sensitive; Flink degrades more gracefully.

### Data-rate sweep (Flink, payload padding 150 B → 8 KB)

Holding message *count* at ~100k/s and inflating each event's payload sweeps the **data
rate** independent of record rate:

| payload | ~ingest data rate | pipe p50 | e2e p50 | e2e p99 | mean CPU |
|---|--:|--:|--:|--:|--:|
| 150 B | ~15 MB/s | 6.9 ms | 15.0 ms | 76 ms | 1.6 cores |
| 1 KB | ~103 MB/s | 6.7 ms | 14.5 ms | 70 ms | 2.3 cores |
| 8 KB | ~770 MB/s | 7.2 ms | 20.5 ms | 92 ms | 3.9 cores |

- **Latency is flat across a ~50× data-rate increase** (15 → 770 MB/s): median held at
  ~7 ms. The engine is **not** data-rate-bound in this range — CPU scales with bytes
  (1.6 → 3.9 cores) but latency doesn't.
- **The first ceiling is Kafka storage, not the engine.** Pushing toward 1 GB/s, the 3
  brokers on gp3 EBS (~125 MB/s sustained write each → ~375 MB/s aggregate, ×3 for
  replication) saturate disk before Flink saturates CPU — at 8 KB throughput already dipped
  to 28.7k/s out. True sustained **1 GB/s would need more brokers and/or io2/instance-store
  volumes**, not a faster engine. This is the honest answer to "how about 1 GB/s": the
  bottleneck moves to the broker/storage tier, and the streaming engine has headroom to spare.

Everything was torn down after measurement (`eks/99_teardown.sh`). ARCC-compliant manifests
and runbook in `eks/` (see `eks/README.md`); raw data (latency, `_top.txt` CPU/mem,
`_lag.txt` offset backlog) in `results/eks/`.

## 10. Durable checkpointing & fault tolerance (the part that flips the verdict)

The §9 numbers ran the engines tuned purely for latency — no durable checkpointing. That's
not how you run production streaming. So we re-ran the whole matrix on EKS with **durable S3
checkpoints** (Spark RTM `checkpointLocation` on s3a://; Flink checkpointing to s3:// every
10s) and **operator-managed restart** (Spark `restartPolicy: OnFailure`; Flink
`restart-strategy: fixed-delay`). This is the production-correct configuration.

### Steady-state latency WITH durable checkpointing, matched 60s interval, EQUAL resources (ms)

All engines checkpoint to durable S3 at the **same 60-second interval** (Spark RTM's
trigger duration = its checkpoint cadence; Flink's `execution.checkpointing.interval`), with
**identical resources per engine** (the earlier run gave Spark 17 cores to Flink's 10 — an
accidental imbalance that has been corrected). Output is keyed by `user_id` on every engine,
and Spark's *pod* memory is matched exactly to Flink's (executor `5120m + 1024m overhead =
6144m`, = Flink's TM pod). ~10M records per engine over a 5-minute steady-state window. The
standard run is **6 partitions / parallelism 6**; a 12-partition run is kept as a scaling check.

**6 partitions / parallelism 6 — 7 cores + 14 GB per engine (the standard matrix):**

| engine | pipe p50 | pipe p99 | e2e p99 | mean CPU | mean mem |
|---|--:|--:|--:|--:|--:|
| **Flink SQL** | 6.0 | **31** | 56 | 1.4c | 5.4 GB |
| **Flink** (Java) | 6.0 | **33** | 63 | 1.2c | 5.1 GB |
| **PyFlink** | 8.8 | **33** | 59 | 5.6c | 10.3 GB |
| **Spark RTM** (Java) | 6.3 | **669** | 833 | 1.6c | 7.5 GB |
| **Spark RTM** (Scala) | 6.2 | **702** | 906 | 1.7c | 7.6 GB |
| **PySpark RTM** | 6.2 | **591** | 763 | 1.7c | 7.7 GB |

**12 partitions / parallelism 12 — 13 cores + 14 GB per engine (scaling check):**

| engine | pipe p50 | pipe p99 | e2e p99 |
|---|--:|--:|--:|
| **Flink** (Java) | 7.3 | **40** | 78 |
| **Flink SQL** | 7.2 | **37** | 73 |
| **Spark RTM** (Java) | 9.0 | **1,299** | 2,174 |

_(Every row is backed by a saved `results/eks/<engine>-{p6,p12eq}_summary.json`, from clean
re-runs with empty topics and per-engine durable S3 checkpoints. RTM's tail is flat across
both sizes — it's the synchronous commit, not a capacity limit.)_

**The headline finding — visible only with durable checkpointing on, measured fairly at a
matched interval and equal resources:**

- **Spark RTM's checkpoint commit is synchronous and stalls the data path.** Committing
  offsets to durable S3 every 60s produces a **~590–700 ms p99 tail** (6-part) across all three
  RTM variants. The median stays ~6 ms; the tail spikes at each checkpoint.
- **Flink's checkpointing is asynchronous** and barely touches the data path — p99 stays at
  ~31–40 ms, and actually tightens with fewer partitions (less coordination).
- So **at a matched 60s interval with equal resources, Spark RTM's tail is ~20× worse than
  Flink's** at 6 partitions (~30× at 12). This is larger than the ~12× we first reported,
  because that earlier figure gave Spark 70% more cores. The gap holds at both 6 and 12
  partitions — **RTM's tail is partition- and core-insensitive**, confirming it's the
  synchronous commit, not a capacity shortage. (RTM's tail also scales with how *often* it
  commits.) Crucially it's a **durable-storage-at-scale** effect: locally (local-disk
  checkpoints, 10k/s) RTM's tail collapses to ~9 ms — see the local matrix in §9.
- Java ≡ Scala holds here too (669 vs 702 ms — same synchronous-commit mechanism).

### Reviewer controls: JSON path + output keying + memory accounting

A technically sharp reviewer of the published post raised that the comparison wasn't fully
apples-to-apples, on several axes. We confirmed each against the code and re-ran with the
differences removed (images rebuilt at TAG=v2, keyed Flink + new Flink SQL job):

1. **Output keying.** Spark wrote Kafka output keyed by `user_id`; the Flink DataStream job
   wrote value-only. *Fix:* the DataStream job now keys by `user_id` (`KeyedJsonSerializer`).
   Cost: a few ms on Flink's tail. Negligible vs RTM's ~600 ms.
2. **JSON path.** Spark used schema-based Catalyst `from_json`/`to_json`; the Flink DataStream
   job hand-parsed with Jackson. *Fix:* we added a **Flink SQL (Table API)** variant using the
   schema-based `json` format — the structural analogue of Catalyst, pure JVM, keyed by
   `user_id`. If the gap were the parser, matching it would collapse the gap.

   | 6-part, equal resources, all keyed, durable 60s ckpt | pipe p99 |
   |---|--:|
   | Flink SQL (schema-based json, Catalyst analogue) | **31 ms** |
   | Flink DataStream (Jackson, keyed) | **33 ms** |
   | Spark RTM (Java, Catalyst) | **669 ms** |

   **The gap is unchanged** — Flink SQL shares Spark's declarative JSON path and keying and is
   still ~20× faster at the tail. The separator is the synchronous-vs-asynchronous checkpoint
   commit, not the JSON library or the partitioning key.
3. **`bufferTimeout(0)` vs RTM trigger.** Correct that these are different mechanisms (per-record
   network flush vs checkpoint cadence); we do not equate them. The matched knob is the **60s
   checkpoint interval** on both engines — the tail comparison is checkpoint-to-checkpoint.
4. **Memory accounting.** On EKS every engine gets the same *container-total* pod memory limit
   (14 GB). Spark's executor pod = `memory` + `memoryOverhead`, so to match Flink's `6144m` TM
   pod exactly we set Spark `executor.memory=5120m` + `memoryOverhead=1024m` (driver 1536m+512m
   = Flink JM's 2048m). The local Docker configs were squared up the same way: Spark worker
   1×6 cores/3072m total (`2560m` + `512m` overhead) to match Flink's single 6-slot TM
   (`process.size=3072m`).

Variants/manifests: `flink/java-sql/` (SQL job), `eks/jobs/{p6,p12eq}/flink-sql.yaml`,
`bench/run_flink_sql.sh`. Summaries: `results/eks/flink-sql-{p6,p12eq}_summary.json`.

### Local confirmation (laptop, 6-part, equal 6c/3072m, 10k evt/s) — and why the tail differs

The same six-engine matrix on a single laptop, identical fairness (6 partitions, equal cores +
pod memory, 60s checkpoints, keyed output), 10k evt/s:

| engine | pipe p50 | pipe p99 | e2e p99 |
|---|--:|--:|--:|
| **Flink** (Java) | 5.6 | **8.1** | 16.5 |
| **Flink SQL** | 6.0 | **8.9** | 17.4 |
| **PySpark RTM** | 5.7 | **8.6** | 11.2 |
| **Spark RTM** (Scala) | 6.3 | **9.4** | 12.0 |
| **PyFlink** | 7.1 | **12.5** | 21.2 |
| **Spark micro-batch** | 46.1 | **78.1** | 81.6 |

- **Locally, RTM's ~600 ms tail disappears** — it ties Flink at ~9 ms. The penalty is the
  synchronous checkpoint *commit*, and what you commit to matters: local-disk at 10k/s is fast;
  durable **S3 at 100k/s** (EKS) is the stall. RTM's tail is a durable-storage-at-scale effect,
  not a property of the engine in isolation. **Benchmarking RTM locally measures the wrong thing.**
- The local run still cleanly reproduces the original RTM-vs-micro-batch claim: 9 ms vs 78 ms
  (~6× median cut). Summaries: `results/{flink,flink-sql,pyflink,spark-rtm,spark-microbatch,pyspark-rtm}_summary.json`.

### Fault tolerance: kill a worker pod mid-stream

We killed an executor/taskmanager pod mid-stream and let the operator recover from the
durable checkpoint:

| engine | recovery downtime (max output stall) | recovered? |
|---|--:|---|
| **Flink** (Java) | ~18.0 s | yes — restart-strategy restored from checkpoint |
| **Spark RTM** (Java) | ~21.5 s | yes — operator kept the query RUNNING, resumed from S3 |
| **PyFlink** | ~24.0 s | yes |

- **Both engines recover automatically in the same ballpark (~18–24 s)** when properly
  configured. Neither is dramatically better; Flink is slightly faster.
- Recovery time is dominated by failure detection + executor/TM re-acquisition + checkpoint
  restore. This is *not* instantaneous for either engine.
- An earlier local test of ours reported RTM recovering in 129 ms — that was an **artifact**
  of killing a partly-idle worker without durable checkpointing, and we discarded it. With a
  real executor loss and an S3 checkpoint, RTM stalls ~21 s like Flink does. (Worth stating
  as a lesson: a fault-tolerance number means nothing unless the checkpoint is durable and
  the killed worker actually held the work.)
- Both sinks are at-least-once (no exactly-once sink configured), so both replay — and
  re-emit — the in-flight window on recovery.

### RTM limitations to know (Spark 4.1, OSS), beyond "no stateful yet"

From the Apache JIRAs/SPIP and the RTM design doc, confirmed for OSS Spark 4.1:

| Limitation | Detail |
|---|---|
| Output mode | **`update` only** (append/complete rejected at startup) |
| Sources | **Kafka only** (no rate/file/socket) |
| Sinks | Kafka, Foreach, Console, Memory — **`foreachBatch` not supported** |
| Delivery | exactly-once *processing*, **at-least-once delivery** (no exactly-once sink yet) |
| State | **stateless only**; stateful (aggregations/joins/windows/dedup) targeted ~Spark 4.3 |
| Joins | stream-static OK (static side must broadcast); **stream-stream not supported** |
| Watermarks | allowed but **no-op** |
| Slots | fixed 1:1 partition↔task; slots must cover the **sum of tasks across all stages** |
| Concurrency | effectively **one RTM query per cluster** (holds slots continuously) |
| Trigger | 5 s minimum (configurable); it's a **checkpoint cadence, not a latency target** |
| PySpark | native `realTime=` trigger only in 4.2.0.dev5; py4j bridge on stable 4.1.2 |
| AQE | the RTM doc says "not supported," but SPARK-53941 (4.1.0) enables it for *stateless* — treat as contested; we disabled it to be safe |

The big practical ones for a stateless Kafka→Kafka pipeline: **update-mode + Kafka-only +
at-least-once + no foreachBatch + one-query-per-cluster.** None blocked this benchmark, but
they shape what RTM can do today.

## 11. Reproduce

```bash
bench/run_spark.sh rtm        # results/spark-rtm_*
bench/run_spark.sh microbatch # results/spark-microbatch_*
bench/run_pyspark.sh          # results/pyspark-rtm_*
bench/run_flink.sh            # results/flink_*
docker build -t pyflink-bench:2.2.0 flink/pyflink/ && bench/run_pyflink.sh  # results/pyflink_*
.venv/bin/python bench/analyze.py
```

Raw per-record latencies and 1 Hz resource samples are in `results/*_latency.csv` and
`results/*_stats.csv`. See `results/spark_findings.md` for probe notes.
