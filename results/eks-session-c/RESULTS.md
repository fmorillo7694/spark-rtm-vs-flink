# Session C results (EKS, joins: Spark 4.3.0 vs Flink 2.3.0)

Plan: `results/eks-stateful/SESSION_C_PLAN.md`, plus 2 recovery-truth kill runs (S1).
Runner `eks/stateful/run_matrix_incluster.sh`, `PLAN_SET=C`, `BUDGET_S=18000`.
Artifacts are under `runs/<label>/` (S3 `stateful-c/20261110T111415Z/`). Tables:

- `tables.md`: latency and counts (`bench/analyze_eks_stateful_b.py`).
- `resources.md` / `resources.json`: CPU, memory, checkpoints (`bench/resource_summary.py`).
- Runner log: `logs/runner.log`. Teardown evidence: `logs/teardown_verify.txt`.

Setup is the same as session B:

- 6x m5.2xlarge; 2 tainted engine nodes, and all 30 runs show no Kafka or load pod on them
  (`placement.txt`).
- p6 pods, parallelism 6, 8 producer pods, 60 s checkpoint / trigger. The images are `v43b`,
  the same as session B.

Workload definitions and the choice of Flink's S4 analogue are in `STATEFUL_CONFIG.md`
("Join workloads").

## Run summary

**30 of 30 runs completed: 0 failures, 0 retries, nothing cut.** Runner time was 16,382 s
(4.6 h) of the 18,000 s budget.

## S4: stream-static enrichment join (Spark RTM vs Flink), 50k evt/s

The reference table has 980,000 rows. It is reloaded into the compacted `st-ref` topic before
every run.

| arm | n | p50 ms | p99 ms | max ms | CPU mean / peak (cores) | mem mean / peak (GiB) |
|---|---:|---:|---:|---:|---|---|
| Spark RTM (`join(broadcast(ref))`), default | 3 | 6.6 | 505.5 | 1,137.8 | 1.8 / 5.0 | 8.7 / 9.6 |
| Flink DS in-memory map, 0 ms | 3 | 6.3 | 18.4 | 230.6 | 1.0 / 3.0 | 5.3 / 5.5 |
| Flink DS in-memory map, 100 ms (default) | 3 | 6.4 | 17.9 | 225.8 | 1.0 / 3.0 | 5.3 / 5.5 |
| Spark RTM, 100k | 1 | 6.5 | 574.3 | 1,145.7 | 2.5 / 5.3 | 8.9 / 9.5 |
| Flink DS 0 ms, 100k | 1 | 6.1 | 27.2 | 606.3 | 1.3 / 2.9 | 5.3 / 5.5 |
| Flink DS 100 ms, 100k | 1 | 6.0 | 18.2 | 147.0 | 1.3 / 2.3 | 5.4 / 5.5 |

Medians over runs.

- **Median:** the same on both engines (~6.5 ms). Neither engine is near saturation at
  100k.
- **p99:** Spark RTM ~28x higher, 0.5 s vs 18 ms. The local smoke placed Spark's tail at
  the RTM batch boundary (60 s trigger): the long-running tasks restart and the broadcast
  is re-planned. On EKS this is inferred, not separately measured.
- **Flink 100 ms vs 0 ms:** no difference, as expected. Source → map → sink chain into one
  task with no network exchange, so the buffer timeout never applies.
- **Resources:** Flink uses about half the CPU and 3.4 GiB less memory. Flink checkpoints are
  trivial (~30 ms, no state). Spark has no state store here, so `progress.jsonl` has nothing
  to report.

**Correctness.** Every run has seeded identical input. The expected output is every event whose
user has a reference row: 18,471,225 events at 50k and 37,239,531 at 100k.

| run | output | vs expected |
|---|---:|---|
| Flink, all 8 runs | 18,471,225 / 37,239,531 | exact |
| Spark RTM r2 | 18,471,225 | exact |
| Spark RTM r3 | 18,471,165 | exact (one producer pod sent 62 fewer events: 18,848,434 originals) |
| **Spark RTM r1** | **18,168,996** | **−302,229 (1.6 %)** |
| Spark RTM 100k | 37,239,529 | −2 |

**Cause of the r1 shortfall.** It is the start-up race, not the join:

- Spark resolves `startingOffsets=latest` only when `query.start()` runs. For S4 that call
  comes after the driver has read and counted the 980k-row reference table.
- r1's consumer saw output only from about second 1–4 (4,276 records in the first second,
  then ramping). That fits the producers starting before the query had resolved its offsets:
  the first ~6 s of input were never read.
- The 100k run's −2 has the same shape at a smaller size.
- r2, watched live, started the query 7 s before the producers and is exact.

**Fix: `1f06ad2`.** For Spark arms, the runner now starts the load only after the driver logs
the started query. It is not yet exercised on a cluster; this session ran the pre-fix runner.
The fix is a test-harness change; the RTM join itself is not at fault.

## S5: stream-stream interval join (Flink vs Spark **micro-batch**)

This measures **what leaving RTM costs**, not RTM vs Flink: OSS Spark 4.3 RTM has no
stream-stream join. The base rate is 27.2k orders/s plus ~23k payments/s, ~50k input records/s.

| arm | n | p50 ms | p99 ms | CPU mean / peak | mem mean / peak GiB | checkpoint / commit |
|---|---:|---:|---:|---|---|---|
| Flink DS `intervalJoin`, 0 ms | 3 | 8,381 | 81,482 | 5.4 / 6.0 | 5.9 / 6.7 | avg 11.7 s, max 27.2 s; upload 88 MB; state ≤ 297 MB |
| Flink DS `intervalJoin`, 100 ms | 3 | 5,925 | 73,145 | 5.3 / 6.1 | 5.9 / 6.6 | avg 11.3 s, max 25.8 s; upload 88 MB; state ≤ 287 MB |
| Spark micro-batch, default trigger | 3 | 10,529 | 17,387 | 4.4 / 5.1 | 9.0 / 10.3 | state commit avg 9.8 s, max 21.1 s per batch; state ≤ 1.5 GB |
| Spark micro-batch, 1 s trigger | 3 | 10,317 | 17,125 | 4.3 / 5.0 | 8.9 / 10.3 | state commit avg 9.8 s, max 22.1 s |
| Flink 0 / 100 ms, 2x (54.4k orders/s) | 1 each | ~126–130 s | ~227 s | 5.5 / 6.1 | 6.2 / 7.0 | avg 13–16 s |
| Spark mb / mb1s, 2x | 1 each | ~69 s | ~157 s | 5.2 / 6.0 | 9.6 / 11.8 | avg 11.1–11.4 s |

**Both engines saturate at 50k, so none of these latencies is a "real-time" comparison.**

- **Flink** runs at ~6 cores (all it has). In the measured window it emits ~15k pairs/s
  against ~23k/s offered, so its backlog grows; its checkpoints take 11–27 s.
- **Spark micro-batch** keeps up better at this rate: 6.5M pairs kept vs Flink's 4.8–4.9M,
  and p99 17 s vs 73–81 s. Its batches stretch to ~10 s, each dominated by the state-store
  commit, and that sets its ~10 s p50.
- The default trigger and the 1 s trigger are the same once batches take ~10 s.
- At 2x both are far past capacity, and the output had not drained by the end of the run.

**Correctness at 50k.** After the run drained, every arm and run emitted the same number of
pairs for the same seeded input: 8,268,451 for 10,336,272 orders, or 8,268,424 / 8,268,428 /
8,268,430 when a producer pod sent a few fewer. This is agreement between engines, not a
ground-truth diff, because the inputs were not captured on EKS. The exact local replay
(0 missing / 0 spurious, `results/local-stateful/correctness/s5.txt`) is the ground truth.

**Rate for a meaningful (unsaturated) S5 comparison.**

- The local smoke at ~5.5k records/s had Flink at p50 6.5 ms. On EKS, Flink's measured S5
  ceiling is ~15k pairs/s, which is ~35k input records/s at 6 cores.
- **Proposal for the final run:** `S5_RATE=1000` per pod, i.e. 8k orders/s + ~6.8k
  payments/s ≈ **15k records/s**. That is ~40 % of Flink's measured ceiling and leaves the
  same kind of headroom the S1–S3 runs had at 50k (3.5 of 6 cores).
- Add one 2x step (`S5_RATE=2000`, ~30k records/s) to show where each engine bends.
- Both rates are estimates from this session's ceiling, not measured.

## Recovery truth: one worker pod deleted at load + 180 s, S1 (fixed on EKS)

`truth.jsonl` was captured this time (the whole-log read). `recovery.json` reports, per
sampled (user, window):

| arm | outage | time to recover | settled windows checked | lost | overcounted | re-emitted records |
|---|---|---|---:|---:|---:|---:|
| Flink DS `s1-update` (TaskManager) | 9 s | 70 s | 125,086 | **0** | **0** | +2,350,516 |
| Spark RTM (executor) | 77 s | 155 s | 121,311 | **0** | **0** | +0 |

- **Both engines recover with exactly-once state:** no window lost an update, and none counted
  an event twice.
- **Flink** recovers fast but replays up to 60 s of input, re-emitting ~2.35M updates through
  the at-least-once sink.
- **Spark** loses ~77 s of output and takes ~2.5 min to catch up, with no re-emits.
- These match session B's outage and recover times (B had no truth check).

## Teardown and cost

- **Self-teardown did not fire.** The runner reached its scale-down step at 15:48:20 UTC, but
  the EKS call failed with `NoRegionError`: boto3 reads `AWS_DEFAULT_REGION`, not the
  `AWS_REGION` the Job sets. S3 worked only because it falls back to a global endpoint.
  Fixed in the runner and `runner-job.yaml` (explicit `region_name`, `AWS_DEFAULT_REGION`).
  The hard deadline (EventBridge Scheduler, armed for 17:15 / 18:00 UTC) was still
  standing. I tore down by hand 3 minutes later.
- **Kafka volumes:** this run's 3 broker PVCs were deleted before the cluster (Kafka CR,
  node pool, then PVCs; gp3 reclaim policy `Delete`). Their EBS volumes are gone
  (`InvalidVolume.NotFound`). The runner's self-teardown now does the same before scaling
  nodes down (`2996ed3`), so `eksctl delete cluster` can no longer leave broker volumes behind.
- **Verified 16:06:50 UTC** (`logs/teardown_verify.txt`):
  - no EKS clusters, no `rtm-flink-bench4` stacks, no instances;
  - no volumes tagged `kubernetes.io/cluster/rtm-flink-bench4`;
  - no deadline schedules (disarmed after `eksctl delete`);
  - NAT `<NAT_ID>` (EIP <EIP>) and `<VPCE_ID>` are
    `available`.

| period (UTC, 2026-11-10) | hours |
|---|---:|
| create + install + selftest (10:48 → 11:15) | 0.45 |
| runner (11:15 → 15:48) | 4.55 |
| wrap-up before delete (15:48 → 15:51) | 0.05 |
| `eksctl delete` (15:51 → 16:06) | 0.25 |
| **total** | **5.3** |

At ~$4/h that is about **$21**, within the approved ~5.6 h / ~$23.
