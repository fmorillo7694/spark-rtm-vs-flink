# Session C plan: join workloads (EKS, not launched)

A separate session from B (B ran as approved: 85 runs, `BUDGET_S` 45000; see
`SESSION_B_PLAN.md`). Runner: `eks/stateful/run_matrix_incluster.sh` with `PLAN_SET=C`, launched by
`PLAN_SET=C eks/stateful/deploy_runner.sh` (needs separate approval). Offline check:
`bash eks/stateful/check.sh` dry-runs plans B, A and C. All three reach `MATRIX DONE`, and
every C engine manifest is pinned to the dedicated engine nodes.

Same cluster, sizing and placement as session B: 6x m5.2xlarge, 2 tainted engine nodes,
p6 pods (Spark driver 2 GB + 2 executors x 6 GB / 3 cores; Flink JM 2 GB + 2 TMs x 6 GB /
3 cores), parallelism 6, 8 producer pods, 60 s checkpoint/trigger. Workload definitions and
the Flink S4 design choice are in `STATEFUL_CONFIG.md` ("Join workloads").

- **S4, stream-static enrichment join:** RTM vs Flink. Base 50k evt/s; 980,000-row reference
  table, reloaded into the compacted `st-ref` topic before every S4 run (`ref-load.yaml`).
- **S5, stream-stream interval join:** **Spark micro-batch vs Flink.** It measures what you
  give up by leaving RTM, because OSS Spark 4.3 RTM has no stream-stream join. Base 27.2k
  orders/s + ~23k payments/s, about 50k input records/s.

## Priority order

| P | what | runs | est. h |
|---|---|---:|---:|
| P1 | S4 x3: `spark-rtm` (Spark default), `flink-ds-bt100` (Flink default 100 ms), `flink-ds` (Flink tuned 0 ms) | 9 | 1.4 |
| P2 | S5 x3: `flink-ds-bt100`, `flink-ds`, `spark-mb` (micro-batch, default trigger), `spark-mb1s` (micro-batch, 1 s trigger) | 12 | 1.7 |
| P3 | capacity probes x1: S4 at 100k evt/s (3 arms), S5 at 2x (54.4k orders/s, ~100k records/s, 4 arms) | 7 | 1.0 |
| | total | 28 | 4.1 |

Per-run estimate: 8.4 min (session A's measured average), plus ~1 min per S4 run for the
reference load (9.4 min). The 4.1 h total includes that.

No vendor-tuned Spark arm for S4. The join has no shuffle and no state store, so the 10 ms
flush and `trackTotalNumberOfRows=false` would change nothing. For the same reason Flink's
100 ms vs 0 ms should be a no-op on S4 (source → join → sink chain into one task). Those two
arms are kept to show it, at the cost of 3 runs.

## Budget, hours, cost

- Launch with `BUDGET_S=18000` (5 h backstop; the runner default is now B's 45000):
  nothing is cut, 28 runs, ~4.1 h of runner time.
- Cluster time ≈ 4.1 h + ~1.5 h create, image pulls and teardown ≈ 5.6 h, about **$23** at
  ~$4/h.
- Running it right after session B on the same cluster would save the ~1.5 h setup (~$6),
  but it is planned as its own session, as asked.

If the budget were ever cut, the priority order drops the probes first, then the last S5
rounds (Flink and Spark arms alternate within each round), and keeps S4.

## Local evidence (no AWS)

**Correctness replay** (`bench/stateful_correctness.sh`): ground truth from captured inputs,
`common/joindata.py` rules. All PASS.

| workload | arms | truth | missing | extra / spurious | other |
|---|---|---:|---:|---:|---|
| S4 | spark-rtm, flink-ds, flink-ds-bt100 | 129,535 enriched records (of 131,886 events; users with no row dropped) | 0 | 0 | 0 wrong tier/region |
| S5 | flink-ds, flink-ds-bt100, spark-mb, spark-mb1s | 96,014 pairs (120,000 orders, 101,995 payments, 5,981 outside the 10 s bound) | 0 | 0 | 0 duplicates |

**Smokes:** 3k evt/s (S5: 3k orders/s + ~2.5k payments/s), 60 s, one local Docker host.
**SMOKE ONLY, not results.**

| arm | p50 ms | p99 ms | max ms |
|---|---:|---:|---:|
| S4 spark-rtm | 6.1 | 371.5 | 765.8 |
| S4 flink-ds (0 ms) | 5.6 | 7.7 | 27.7 |
| S4 flink-ds-bt100 | 5.8 | 8.6 | 15.4 |
| S5 flink-ds (0 ms) | 6.5 | 8.8 | 227.8 |
| S5 flink-ds-bt100 | 56.5 | 106.1 | 228.7 |
| S5 spark-mb (default trigger) | 357.8 | 1,564.0 | 1,768.4 |
| S5 spark-mb1s (1 s trigger) | 687.3 | 1,177.9 | 1,219.2 |

Notes on the smokes:

- **S4 Spark p99:** the tail sits entirely in one second, about 61 s after the query started,
  which is the first RTM batch boundary (60 s trigger). 2,011 records took >100 ms there. The
  long-running tasks restart at the boundary, and the broadcast relation is re-planned per
  batch (the cached table is not re-read). The 5-minute EKS trigger would make this rarer but
  not remove it. Session C's 300 s measured window crosses about 5 boundaries at the 60 s
  trigger.
- **S4 Flink:** 100 ms vs 0 ms is within noise, as expected for a fully chained job.
- **S5 Flink:** `keyBy` adds a network exchange, so the buffer timeout shows up (p50 56 ms
  at 100 ms).
- **S5 Spark micro-batch:** the default trigger runs back-to-back batches (p50 0.36 s); the
  1 s trigger waits for the boundary (p50 0.69 s).

## Before launch

1. **Images:** rebuild and push `v43s` (or a new tag) for the spark, flink and load images.
   Session C needs the new `StatefulPipelineJava` (s4/s5), `bench.Joins`, and
   `producer.py` + `joindata.py`. The running images predate them. Session B needs the
   rebuild anyway for its own new arms.
2. **Spark S4 driver memory:** the broadcast is built on the driver (1536m heap). Locally,
   1M rows fit in a 1 GB driver. Not checked on EKS.
3. **S5 consumer volume:** ~22k output records/s, below S1/S2 volumes, so the consumer is not
   a concern.
4. Not checked: either join on EKS, the Spark stream-stream join with
   `checkpointFormatVersion=2` on S3 (it worked locally on the file system), and the 100k
   probes.
