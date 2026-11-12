# EKS stateful matrix: Spark 4.3.0 RTM vs Flink 2.3.0 (2026-11-08/09, session A)

Cluster `rtm-flink-bench4` (EKS 1.36, 6x m5.2xlarge, us-east-1), images tag `v43s` (linux/amd64),
Strimzi 0.51.0 / Kafka 4.1.0 (3 brokers, RF 3), Flink operator 1.16.1, Spark operator (kubeflow chart).
Same p6 sizing as the stateless round: 6 partitions / parallelism 6, per engine 1 coordinator
(1c / 2 GB) + 2 workers (3c / 6144m pod-total). Spark has 12 task slots on those 6 cores
(source + state tasks run together; see `STATEFUL_CONFIG.md`). RocksDB state on both, durable S3
checkpoints every 60 s (RTM trigger 60 s / Flink interval 60 s), 1M user keys, watermark 5 s,
window 10 s, session gap 5 s. 8 producer pods, 60 s warm-up discarded, 300 s measured. Duplicates
(10 %, re-sent 0-3 s later) on S2 only. Strictly one engine at a time, fresh topics and checkpoint
path per run. The whole matrix ran in-cluster (`eks/stateful/run_matrix_incluster.sh` as a
Kubernetes Job); the laptop only launched it and pulled the results.

Every setting and the engine semantics are in [`STATEFUL_CONFIG.md`](../../STATEFUL_CONFIG.md).
Regenerate: `python bench/analyze_eks_stateful.py > results/eks-stateful/tables_50k.md` and
`... --rate 100k > results/eks-stateful/tables_100k.md` (per-run rows are in those files).
Raw per-run artifacts: `runs/<label>/` (summary, histogram, evidence, verify counts, kubectl top).

## Plan change versus the brief: base rate 50k evt/s, 100k as a probe

The brief kept the stateless rate, 100k evt/s. The first two runs at 100k (now labelled
`*-at100k-r0`) showed that rate saturates the stateful jobs on 7 cores:

- **Spark RTM S1 fell behind**: 5.5-6.0 engine cores (all of them), pipeline p50 14.7 s, a 3-25 s
  sawtooth. Each synchronous end-of-batch commit added a backlog the saturated executors drained
  only slowly. Throughput and counts stayed exact (38,000,750 updates for 38,000,750 inputs).
- **Flink S1 kept up only just**: p50 6.9 ms, p99 2.2 s, ~5 of 6 cores busy.

At saturation a flush-timer 2x2 measures backlog, not latency. So the headline matrix runs at
**50k evt/s for both engines** (6,250 per producer pod x 8; the same producers, seed and keys
for every arm), and every headline arm gets one more **`-at100k`** capacity-probe run, last in
priority. Plan: 16 headline arms x 3 runs + 4 other arms x 2 runs + 16 probe runs = **72 runs**.
All 72 completed on the first attempt; **nothing was cut by the budget** (10.1 h of the 11 h
runner budget). The two early 100k runs are kept as extra probe runs (`r0`).

## Correctness on EKS (whole-run counts, `verify.json`)

| invariant | arms | result |
|---|---|---|
| S1 update mode: output records == input records (one update per row, no late row) | Spark RTM 50 ms / 1 ms, Flink DataStream `s1-update` 0 / 100 ms, all rates | **PASS, every run** (e.g. 18,848,496 == 18,848,496) |
| S2 dedup: output == unique events produced | Spark RTM 50 ms / 1 ms, all rates | **PASS, every run** |
| S2 dedup, Flink, 50k | DataStream 0 / 100 ms, SQL 100 ms, EO DataStream / SQL | PASS (EO: surplus = 252 commit markers = 6 sink subtasks x 6 partitions x 7 checkpoints) |
| S2 dedup, Flink SQL 0 ms, 50k | 3 runs | 2 exact, **r1: +6 records** (3x10⁻⁷) |
| S2 dedup, Flink, 100k probe | DataStream 0 / 100 ms, SQL 0 / 100 ms | **+319 / +598 / +9,813 / +4,775 records** (up to 2.6x10⁻⁴) |
| S1 final-only windows, S3 sessions | Flink window/TUMBLE, both S3 | not checkable from counts; covered by the exact local replay (`results/local-stateful/`) |

No Flink run restarted (`restored: 0`, 7/7 checkpoints completed). The extra Flink S2 records are
duplicates that arrived after the 5 s **processing-time** TTL had expired: a duplicate is up to 3 s
late at the producer, and Flink stalled up to 2.8 s (50k) / 6.3 s (100k), which together pass 5 s.
Spark's dedup is **event-time** (watermark), so the same stalls cannot leak. This is the retention
difference listed in `STATEFUL_CONFIG.md` § Semantic differences, now measured.

Stateful RTM ran with synchronous commits in every Spark run: 0 `async-log-write` threads, 0
`AsyncProgressTrackingMicroBatchExecution` frames, and the driver logs `Submitting ResultStage N
concurrently with its running pipelined producer(s)` (pipelined shuffle) each batch.

## Default vs tuned flush (50k evt/s; pipe p50 / p99 ms, geomean of runs, n = runs)

Default = Spark `spark.shuffle.streaming.networkBufferMaxWaitTimeMs` 50 ms / Flink
`execution.buffer-timeout` 100 ms. Tuned = Spark 1 ms / Flink 0 ms.

**S1 10 s tumbling count + sum per user** (Flink DataStream arm = `s1-update`, Spark's update-mode semantics)

| engine | default flush | tuned flush |
|---|---|---|
| Spark RTM | 39.3 / 6,839 (n=3) | 243.8 / 21,364 (n=3) |
| Flink DataStream | 55.5 / 1,473 (n=3) | **6.9 / 1,421** (n=3) |
| Flink SQL TUMBLE (final-only, different product) | 11,234 / 16,913 (n=3) | 11,229 / 16,944 (n=3) |

**S2 dedup on event_id**

| engine | default flush | tuned flush |
|---|---|---|
| Spark RTM | 38.3 / 7,908 (n=3) | 329.2 / 23,208 (n=3) |
| Flink DataStream | 51.8 / 259 (n=3) | **6.8 / 321** (n=3) |
| Flink SQL | 50.9 / 566 (n=3) | 6.9 / 689 (n=3) |

**S3 per-user sessions with timers**

| engine | default flush | tuned flush |
|---|---|---|
| Spark RTM (Java transformWithState) | 109,292 / 181,564 (n=3) | 130,082 / 183,945 (n=3) |
| Flink DataStream | 69.2 / 4,403 (n=3) | **8.5 / 4,441** (n=3) |

## Per arm at 50k evt/s (geomean of runs; window = mean of runs, can be negative)

| workload | arm | runs | pipe p50 | pipe p99 | pipe max | e2e p50 | e2e p99 | window p50 | window p99 | p99 near boundary | p99 elsewhere | CPU cores | mem GB | count invariant |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| s1-update | Flink DataStream, buffer timeout 100 ms (default) | 3 | 55.5 | 1472.8 | 2642.0 | 80.1 | 1498.7 | -4903.0 | -10.3 | 2295.8 | 249.9 | 2.0 | 5.7 | PASS |
| s1-update | Flink DataStream, buffer timeout 0 ms | 3 | 6.9 | 1420.5 | 2745.1 | 27.5 | 1447.2 | -4992.3 | 136.7 | 2298.5 | 206.6 | 3.4 | 5.7 | PASS |
| s1-window | Flink DataStream, buffer timeout 0 ms | 2 | 10077 | 15732 | 17560 | 11294 | 17639 | 5639.5 | 6995.5 | 13191 | 15700 | 3.1 | 6.0 | n/a |
| s1 | Spark RTM, shuffle flush 1 ms | 3 | 243.8 | 21364 | 23284 | 299.6 | 21396 | -2162.3 | 18210 | 22525 | 20715 | 5.6 | 8.4 | PASS |
| s1 | Spark RTM, shuffle flush 50 ms (default) | 3 | 39.3 | 6838.8 | 7963.6 | 65.7 | 6869.5 | -4485.7 | 3688.7 | 5884.3 | 6796.6 | 3.7 | 8.2 | PASS |
| s1 | Spark micro-batch | 2 | 3368.3 | 5220.2 | 6142.6 | 3764.4 | 5791.9 | -1779.0 | 4367.0 | n/a | n/a | 3.3 | 8.4 | n/a |
| s1 | Flink SQL TUMBLE, buffer timeout 100 ms (default) | 3 | 11234 | 16913 | 18476 | 12376 | 18750 | 6822.3 | 7952.0 | 13027 | 16900 | 2.1 | 6.1 | n/a |
| s1 | Flink SQL TUMBLE, buffer timeout 0 ms | 3 | 11229 | 16944 | 18318 | 12365 | 18774 | 6854.0 | 7905.0 | 12923 | 16900 | 2.1 | 6.1 | n/a |
| s2 | Spark RTM, shuffle flush 1 ms | 3 | 329.2 | 23208 | 25302 | 552.5 | 23260 | | | 17547 | 23056 | 4.7 | 8.2 | PASS |
| s2 | Spark RTM, shuffle flush 50 ms (default) | 3 | 38.3 | 7907.6 | 9317.5 | 64.0 | 7938.9 | | | 5666.5 | 7931.5 | 3.4 | 8.2 | PASS |
| s2 | Flink DataStream, buffer timeout 100 ms (default) | 3 | 51.8 | 259.3 | 1546.6 | 76.2 | 287.2 | | | 1133.1 | 100.0 | 1.6 | 5.3 | PASS |
| s2 | Flink DataStream, EXACTLY_ONCE sink (read_committed) | 2 | 6.8 | 321.8 | 1728.1 | 43906 | 61818 | | | 1319.8 | 21.5 | 3.3 | 5.4 | PASS |
| s2 | Flink DataStream, buffer timeout 0 ms | 3 | 6.8 | 321.2 | 1756.4 | 26.7 | 343.7 | | | 1269.7 | 15.7 | 3.2 | 5.3 | PASS |
| s2 | Flink SQL, buffer timeout 100 ms (default) | 3 | 50.9 | 565.5 | 1998.6 | 74.4 | 586.3 | | | 1486.3 | 100.0 | 2.0 | 5.8 | PASS |
| s2 | Flink SQL, EXACTLY_ONCE sink (read_committed) | 2 | 6.9 | 705.1 | 3230.5 | 44667 | 62186 | | | 1708.6 | 26.0 | 3.7 | 5.9 | PASS |
| s2 | Flink SQL, buffer timeout 0 ms | 3 | 6.9 | 689.2 | 2795.3 | 27.0 | 712.5 | | | 1679.7 | 19.9 | 3.6 | 5.8 | FAIL (r1 +6, TTL) |
| s3 | Spark RTM, shuffle flush 1 ms | 3 | 130082 | 183945 | 193082 | 130113 | 184005 | | | 152781 | 184000 | 5.6 | 8.6 | n/a |
| s3 | Spark RTM, shuffle flush 50 ms (default) | 3 | 109292 | 181564 | 185289 | 109327 | 181606 | | | 121616 | 181594 | 5.5 | 8.5 | n/a |
| s3 | Flink DataStream, buffer timeout 100 ms (default) | 3 | 69.2 | 4402.9 | 6252.0 | 94.1 | 4451.3 | | | 4500.0 | 4300.0 | 3.4 | 6.0 | n/a |
| s3 | Flink DataStream, buffer timeout 0 ms | 3 | 8.5 | 4441.3 | 5801.7 | 34.4 | 4481.6 | | | 4464.9 | 4331.6 | 4.9 | 6.0 | n/a |

ms throughout. "near boundary" = event second within -1..+3 s of a Spark batch start or a Flink
checkpoint trigger; percentiles from 1 ms / 10 ms / 100 ms histogram buckets (lower bounds).
CPU / mem = engine pods only, mean of `kubectl top` samples, mean over runs.

## Capacity probe at 100k evt/s (pipe p50 / p99 ms, n = runs; `tables_100k.md`)

| workload | arm | n | pipe p50 | pipe p99 | CPU cores | count invariant |
|---|---|---|---|---|---|---|
| s1-update | Flink DataStream 100 ms | 1 | 54.9 | 2,214 | 3.0 | PASS |
| s1-update | Flink DataStream 0 ms | 2 | 6.9 | 2,214 | 4.7 | PASS |
| s1 | Spark RTM 50 ms | 2 | 17,814 | 27,162 | 5.5 | PASS |
| s1 | Spark RTM 1 ms | 1 | 65,691 | 104,229 | 5.4 | PASS |
| s1 | Flink SQL TUMBLE 100 / 0 ms | 1 / 1 | 12,332 / 12,449 | 19,092 / 19,260 | 3.2 | n/a |
| s2 | Spark RTM 50 ms / 1 ms | 1 / 1 | 11,305 / 68,465 | 22,254 / 106,175 | 5.5 | PASS |
| s2 | Flink DataStream 100 / 0 ms | 1 / 1 | 47.9 / 6.7 | 1,291 / 1,377 | 2.6 / 4.5 | FAIL (+319 / +598, TTL) |
| s2 | Flink SQL 100 / 0 ms | 1 / 1 | 47.6 / 6.9 | 2,253 / 2,472 | 3.3 / 5.0 | FAIL (+4,775 / +9,813, TTL) |
| s3 | Spark RTM 50 / 1 ms | 1 / 1 | 175,194 / 179,651 | 260,710 / 264,485 | 5.6 | n/a |
| s3 | Flink DataStream 100 / 0 ms | 1 / 1 | 263 / 627 | 4,975 / 5,146 | 5.5 | n/a |

## What the matrix shows

1. **Medians, tuned vs tuned:** Flink stays at ~7 ms on all three workloads at both rates. Spark
   RTM's best median is its *default* 50 ms flush (38-39 ms at 50k), which sits just under Flink's
   *default* 100 ms buffer timeout (51-56 ms). The flush timer sets the median on both engines.
2. **The 1 ms Spark flush backfires at scale.** It was level with Flink in the 3k evt/s local smoke
   (7 vs 6 ms). At 50k it costs CPU (5.6 vs 3.7 cores on S1, tiny streaming-shuffle messages) and
   pushes Spark into backlog: p50 244 ms (S1) / 329 ms (S2), p99 21-23 s. Flink's 0 ms costs ~1.4
   cores more than 100 ms but stays in budget.
3. **The Spark RTM tail is the synchronous stateful commit.** Batches run 66-67 s commit-to-commit
   for a 60 s trigger (S1/S2), so each boundary stalls the pipeline ~6-7 s; with 1M keys the
   backlog from that stall lasts tens of seconds, which is why p99 "elsewhere" (6.8-7.9 s) is as
   high as "near boundary". Flink's S1/S2 tail sits at checkpoints (p99 near 1.1-2.3 s vs
   elsewhere 16-250 ms).
4. **Spark RTM `transformWithState` (S3) does not sustain 50k evt/s** with a per-event processing-
   time timer re-arm and 4 value states on 1M keys: p50 109-130 s and growing, batches stretch
   to 211-232 s, all 6 cores busy. Flink's KeyedProcessFunction runs the same logic at p50 8.5 ms
   (p99 4.4 s, at checkpoints). Same code paths, verified identical on the local replay.
5. **Final-only windows**: Flink window / SQL TUMBLE emit once per window, so their "latency" is
   window close + watermark (window p50 +5.6 s / +6.8 s). Spark micro-batch S1 is 3.4 s p50.
6. **EXACTLY_ONCE costs visibility, not processing**: pipe p50 6.8-6.9 ms, but read_committed e2e
   p50 44 s (records appear at the 60 s checkpoint commit). RTM has no exactly-once sink.
7. **Dedup exactness**: Spark's event-time dedup was exact in every run at both rates; Flink's
   processing-time TTL dedup leaked duplicates whenever stalls + duplicate delay passed the 5 s
   TTL (6 records at 50k once, 319-9,813 at 100k). A longer TTL, or event-time cleanup, would
   close it; we kept TTL = watermark bound so both engines retain the same nominal horizon.
8. Memory: Spark 8.2-8.6 GB vs Flink 5.3-6.1 GB for the same pods (the stateless ratio held).

## Caveats

- One cluster, one night; 3 runs per headline cell (2 for the others, 1-2 at 100k). Session B
  adds the third run of the 2-run arms.
- The boundary window (-1..+3 s) is narrow for Spark, whose backlog from one stall lasts longer;
  read "near" vs "elsewhere" for Spark as "both inside the backlog".
- Latency anchors per workload are defined in `STATEFUL_CONFIG.md` (S1 = max ts of the window so
  far, S3 timeout = inactivity deadline).
- Cluster hours: created 21:44, deleted ~09:20 local = ~11.6 h of cluster time (runner 10.1 h).
