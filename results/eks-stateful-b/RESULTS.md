# Session B results (EKS, Spark 4.3.0 RTM vs Flink 2.3.0, stateful + stateless leftovers)

Plan: `results/eks-stateful/SESSION_B_PLAN.md` (approved revision). Runner
`eks/stateful/run_matrix_incluster.sh`, `PLAN_SET=B`, `BUDGET_S=45000`. Artifacts per run under
`runs/<label>/` (S3 `stateful-b/20261109T132757Z/`). Per-arm table: `tables.md`
(`bench/analyze_eks_stateful_b.py`). Runner log: `logs/runner.log`.

Setup: same as session A (6x m5.2xlarge; p6 pods: Spark driver 2 GB + 2 executors x 6 GB /
3 cores, Flink JM 2 GB + 2 TMs x 6 GB / 3 cores; parallelism 6; 8 producer pods; 50k evt/s
base; RocksDB; 60 s checkpoint/trigger unless a knob changes it). New in B: the 2 engine
nodes are tainted, and every stateful engine pod is pinned there, one worker per node.

## Run summary

- **85 of 85 runs completed, 0 failures, 0 retries, nothing cut by the budget.** Runner time
  43,098 s (12.0 h) of 45,000 s. Runs were a little faster than the estimate, so P5 (the 3rd
  runs) fit too.
- **Placement:** all 69 stateful runs on the 2 engine nodes with no kafka-namespace pod on
  them (`placement.txt`: `shared_with_kafka_ns=[]` in every run). The Spark operator webhook
  applied the nodeSelector, toleration and anti-affinity. The 16 stateless runs used the
  shared 6-node pool as approved: their engine and load pods landed on any node.
- **Probe** (before the full run, `probe/`): it found a runner bug. The end-of-run output
  count stopped after one unchanged 10 s sample, which can fall inside Spark RTM's 7–10 s
  batch-boundary pause; Spark S2 then read 210,300 short. Fixed before the full run (3 equal
  samples, commit `c99bc51`). Every Spark S2 count in the full run is exact.

## Correctness (whole-run counts, `verify.json`)

| invariant | arms | result |
|---|---|---|
| S2 dedup: output == unique events produced | Flink DS `s2-et` 0 / 100 ms, Flink SQL `s2-ttl60` 0 / 100 ms (x3 each, and x1 at 100k), Spark RTM default and vendor-tuned | **exact in all 19 runs** |
| S2 dedup, EO sinks | `flink-ds-eo`, `flink-sql-eo` (3rd runs) | exact + 252 transaction markers (as session A) |
| S1 update mode: output == input | Spark RTM (default, vendor-tuned, ckpt 5/30/300 s, hot keys), Flink `s1-update` (same knobs) | **exact in every run** |
| S1 after a worker kill | Spark RTM, Flink `s1-update` | at-least-once re-emits only (see Recovery) |
| S3, final-only windows, Spark micro-batch S1 | | not checkable from counts; covered by the exact local replays |

**Dedup fix verdict: fixed, with a tail cost on the DataStream arm.** Session A's 5 s
processing-time TTL leaked 6–9,813 duplicates (worst at 100k). With event-time cleanup
(`s2-et`) and the 60 s TTL SQL arm (`s2-ttl60`), Flink emits exactly the unique events in all
16 runs, including 100k. Medians stay the same, but the DataStream arm's p99 rises ~5x:

| arm | p50 ms | p99 ms | session A old dedup, p50 / p99 |
|---|---:|---:|---|
| Flink DS `s2-et`, 0 ms | 6.7 | 1,774 | 6.8 / 321 (`s2`) |
| Flink DS `s2-et`, 100 ms | 54.5 | 1,731 | 51.8 / 259 (`s2`) |
| Flink SQL `s2-ttl60`, 0 ms | 6.8 | 764 | 6.9 / 689 (`s2`) |
| Flink SQL `s2-ttl60`, 100 ms | 51.1 | 614 | 50.9 / 566 (`s2`) |
| Spark RTM, default (control, run 4) | 38.5 | 7,922 | 38.3 / 7,908 |
| Flink DS `s2-et` 0 ms, 100k | 6.8 | 1,464 | 6.7 / 1,377, leaked 319 |
| Flink SQL `s2-ttl60` 0 ms, 100k | 6.7 | 2,468 | 6.9 / 2,472, leaked 9,813 |

- `s2-et` registers one event-time timer per event_id (~55k/s), and the watermark fires them in
  bursts, which shows up at p99 (~1.7 s vs 0.3 s). Not profiled; that is the likely cause.
- `s2-ttl60` keeps the old cost profile (p99 within 10 %), so it is the leak-free Flink dedup
  with no tail penalty; it holds 12x the 5 s state.
- Either fixed arm is still well under Spark RTM's S2 p99 (7.9 s default, 4.3 s vendor-tuned).

Medians over runs (n = 3, 100k n = 1).

## Vendor-tuned Spark (`spark-rtm-vt`: 10 ms shuffle flush + `trackTotalNumberOfRows=false`)

| workload | default Spark RTM p50 / p99 ms (session A) | vendor-tuned p50 / p99 ms | Flink DS 0 ms p50 / p99 ms |
|---|---|---|---|
| S1 | 39.3 / 6,839 | **13.0 / 4,046** | 6.9 / 1,421 (A) |
| S2 | 38.5 / 7,922 | **12.7 / 4,268** | 6.7 / 1,774 (`s2-et`); SQL `s2-ttl60` 6.8 / 764 |
| S3-lt | 97,168 / 161,374 (B, default) | 79,382 / 103,594 | 635 / 4,034 |

The two vendor settings cut Spark's median about 3x and its p99 by ~40 % on S1/S2, with exact
counts and the same ~6 cores / ~9.2 GB. Flink at 0 ms is still ~2x lower at p50 and ~2.5–5x
lower at p99. Spark still cannot sustain S3 at 50k evt/s even tuned (p50 79 s = backlog).

## S3 light timer (`s3-lt`, 1 s deadline buckets)

| arm | p50 ms | p99 ms | kept | session A `s3` p50 / p99 / kept |
|---|---:|---:|---:|---|
| Spark RTM default | 97,168 | 161,374 | 7.1M | 109,292 / 181,564 / 6.4M |
| Spark RTM vendor-tuned | 79,382 | 103,594 | 9.2M | — |
| Flink DS 0 ms | 635 | 4,034 | 11.7M | 8.5 / 4,441 / 11.7M |
| Flink DS 100 ms | 679 | 4,226 | 11.7M | 69 / 4,403 / 11.7M |

- **Spark:** the lighter timer helps a little (11 % more sessions out, p50 −11 %), not
  enough; S3 still saturates Spark RTM at 50k.
- **Flink:** p50 rises from 8.5 to ~635 ms. That is the documented semantics, not a
  slowdown: timeouts fire at the rounded-up second (0–1 s late, 0.5 s on average) and the
  latency anchor stays `last + timeout`. Output volume and p99 are unchanged.

## Recovery: one worker pod deleted at load + 180 s, S1 (`recovery.json`)

| arm | outage (output stops) | time to recover (≥ 90 % rate, e2e < 1 s) | re-emitted records | p50 over the run |
|---|---|---|---|---|
| Flink DS `s1-update` (TaskManager) | 8 s, 8 s, 8 s | 55 s, 56 s, 56 s | +1.98M, +2.00M, +1.99M | 7.0 ms |
| Spark RTM (executor) | 75 s, 73 s, 71 s | 155 s, 150 s, 148 s | +0, +108k, +81k | 4,806 ms |

- Flink restarts the job from the last checkpoint within ~8 s and replays up to 60 s of
  input, so it re-emits ~2M updates (at-least-once sink) and catches up in under a minute.
- Spark loses ~72 s of output and needs ~2.5 min to catch up. It re-emits far fewer records,
  because a Spark batch is replayed only from its own start and its output resumes later.
- **Not measured on EKS:** lost/overcounted windows. The producers' `TRUTHJSON` line did
  not reach `truth.jsonl`: the runner read only the last 3 producer log lines, and the long
  line was most likely split by the kubelet. The runner now reads the whole log (fix is
  not yet re-verified on a cluster). The local recovery smoke had 0 lost, 0 overcount.

## Hot keys (10 % of events on 10 users), S1

| arm | p50 / p99 ms, hot | p50 / p99 ms, uniform (session A) |
|---|---|---|
| Spark RTM default | 37.4 / 6,289 | 39.3 / 6,839 |
| Flink DS `s1-update` 0 ms | 6.8 / 1,243 | 6.9 / 1,421 |

No measurable effect at this skew on either engine; counts exact.

## Checkpoint / trigger interval (Spark RTM trigger = Flink checkpoint interval)

| interval | Spark S1 p50 / p99 | Flink S1 p50 / p99 | Spark S2 p50 / p99 | Flink S2 p50 / p99 |
|---|---|---|---|---|
| 5 s | 251 / 2,876 | 6.9 / 380 | 74 / 2,572 | 6.8 / 320 |
| 30 s | 40.7 / 4,668 | 6.8 / 1,000 | 39.9 / 4,350 | 6.8 / 1,018 |
| 60 s (session A / B base) | 39.3 / 6,839 | 6.9 / 1,421 | 38.5 / 7,922 | 6.7 / 1,774 |
| 300 s, 720 s measured | 36.6 / 17,307 | 6.8 / 415 | — | — |

- **Spark:** the interval is a trade-off. Short batches raise the median (5 s: p50 251 ms on
  S1), long batches raise the tail: the boundary pause has more to catch up, p99 17 s at 5 min.
- **Flink:** p50 does not move. Its p99 is driven by checkpoint alignment, not by interval
  length: 5 min gives the lowest p99, because only 2 checkpoints fall in the window.

## Stateless leftovers (100k evt/s, published p6 setup, v43 images, shared pool)

| engine | runs | p50 / p99 ms (B) | published p50 / p99 ms |
|---|---:|---|---|
| Flink DataStream | 3 | 6.1 / 26.9 | 5.9 / 29.8 |
| Flink SQL | 3 | 6.1 / 27.0 | 6.0 / 31.4 |
| PyFlink (parallelism 6) | 3 | 9.2 / 41.3 | 8.8 / 33.1 |
| PySpark RTM | 3 | 6.2 / 36.9 | 6.2 / 591 (older round) |
| Spark RTM Java, async progress off (control) | 3 | 6.2 / 606 | Java default 6.2 / 38.4 (4.3) |
| Spark RTM Scala (3rd run) | 1 | 6.2 / 33.3 | 6.2 / 35.1 (2 runs) |

- The new runs reproduce the published numbers within noise.
- The async-off control confirms that Spark 4.3's p99 gain (642 → 38 ms) comes from async
  progress tracking: turning it off gives 606 ms again.
- PySpark RTM on 4.3 matches Java at p99 37 ms.

## Third runs of session A's 2-run arms

| arm | p50 / p99 ms | correctness |
|---|---|---|
| Spark micro-batch S1 | 3,640 / 5,572 | n/a (one update per key per batch) |
| Flink DS `s1-window` (final-only) | 10,236 / 16,410 | n/a (final-only; window + 5 s watermark) |
| Flink DS EO S2 | 6.9 / 344 | exact + markers |
| Flink SQL EO S2 | 6.8 / 668 | exact + markers |

All in line with sessions A's two runs.

## Correctness misses

- None in the full run.
- Probe: one undercount caused by the runner's count wait, fixed before launch.
- Recovery truth not captured (above).

## Cluster hours and cost

| period (UTC) | hours |
|---|---:|
| create + install + images + probe (11-09 12:45 → 13:28) | 0.7 |
| runner (13:28 → 01:26) | 12.0 |
| **idle after MATRIX DONE (01:26 → 10:09)** | **8.7** |
| teardown (10:09 → 10:25) | 0.3 |
| **total** | **21.7** |

At ~$4/h that is about **$87**, against the approved ~14.5 h / ~$57. The excess is the 8.7
idle hours (~$35): my polling stopped at 19:36 UTC and nothing tore the cluster down when
the runner finished. For session C the runner should end by deleting the engines and a
local watcher should run the teardown, rather than relying on an agent turn being active.

Teardown (11-10 10:25 UTC): `eksctl delete cluster rtm-flink-bench4 --wait` completed; no EKS
clusters, no `eksctl-rtm-flink-bench4-*` stacks, no instances; IRSA role
`rtm-bench-stateful-runner` gone. NAT `<NAT_ID>` (EIP <EIP>) and endpoint
`<VPCE_ID>` are still `available`. **Left over:** 6 unattached 100 GB gp3 volumes
(Kafka broker PVCs, 3 from session A, 3 from B, tagged
`kubernetes.io/cluster/rtm-flink-bench4=owned`), ~$48/month. Not deleted, pending a decision.
