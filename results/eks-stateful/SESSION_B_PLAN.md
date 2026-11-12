# Session B plan (EKS)

## Approved revision (launched)

Four changes to the plan below, approved with the launch:

1. The 8 key-cardinality runs (10M/50M keys on S2) are dropped. State is bounded by time,
   so they measure nothing new. The `keys=` knob stays in the runner.
2. P4 order: recovery kill (6), hot keys (4), checkpoint 5 s / 30 s (16), 5-minute
   cadence (4).
3. `BUDGET_S=45000` (12.5 h runner time; session cap ~14.5 cluster hours, ~$57).
4. The 16 stateless p6 runs are no longer pinned (`SL_NODES=pool`). Their engine and load
   pods get only the toleration for the engine-node taint: no nodeSelector, no affinity. The
   scheduler sees all 6 nodes as one pool, as on the untainted 6-node cluster of the
   published stateless runs. Kafka brokers stay on the 4 untainted nodes, as before. All
   stateful arms stay pinned to the engine nodes.

Dry-run (`eks/stateful/check.sh`): **85 runs, 12.9 h estimated**. The budget stops before
P5, so its 4 runs are cut: 81 runs, ~12.4 h.

The text below is the pre-approval plan; where it differs, this section wins.

Runner: `eks/stateful/run_matrix_incluster.sh`, `PLAN_SET=B` (the default). Launch with
`eks/stateful/deploy_runner.sh` once the cluster exists (needs separate approval). Offline
check: `bash eks/stateful/check.sh` (shellcheck, YAML render of every manifest, DRY_RUN of
plans B and A, placement assertions; both plans reach `MATRIX DONE`).

Same pod sizing as session A: Spark driver 2 GB + 2 executors x 6 GB / 3 cores, Flink JM 2 GB +
2 TMs x 6 GB / 3 cores, parallelism 6, 8 producer pods, 50k evt/s base, RocksDB on both, 60 s
checkpoint/trigger unless a knob changes it. `BUDGET_S=39600` (11 h). Flink-side tuning
(changelog backend, unaligned checkpoints, ForSt) is left out; that is the Part 2 post.

Per-run estimates: session A (8.4 min stateful, 12.2 min Spark S3, 8.1 min Flink S3), the 4.3
stateless round (9 min, PyFlink 10 min), plus the extra load time of longer runs.

## Priority order

| P | what | runs | est. h |
|---|---|---:|---:|
| P1 | S2 with the fixed Flink dedup: `flink-ds{,-bt100}:s2-et`, `flink-sql{,-bt100}:s2-ttl60` x3; Spark RTM default S2 x1 control (run 4); the 4 fixed Flink arms at 100k x1 | 17 | 2.4 |
| P1 | vendor-tuned Spark `spark-rtm-vt` (10 ms shuffle flush + `trackTotalNumberOfRows=false`) on S1, S2, S3-lt x3 | 9 | 1.5 |
| P2 | S3 light timer `s3-lt`: Spark RTM default, Flink DS tuned (0 ms), Flink DS default (100 ms) x3 | 9 | 1.4 |
| P3 | stateless leftovers, 100k evt/s, unchanged v43 images and manifests: Flink DS, PySpark RTM, Flink SQL, Spark RTM async-off control, PyFlink (parallelism 6) x3; third Spark RTM Scala run | 16 | 2.5 |
| P4 | gap tests, Spark RTM default vs Flink DS tuned: ckpt 5 s / 30 s on S1 + S2 x2 (16, 2.2 h); 5-min cadence on S1 x2 (4, 1.0 h); 10M / 50M keys on S2 x2 (8, 1.1 h); worker kill on S1 x3 (6, 0.8 h); 10 % hot keys on S1 x2 (4, 0.6 h) | 38 | 5.8 |
| P5 | third runs of session A's 2-run arms: `spark-mb:s1`, `flink-ds:s1-window`, `flink-ds-eo:s2`, `flink-sql-eo:s2` | 4 | 0.6 |
| | total | 93 | 14.0 |

Within each round Spark and Flink arms alternate, so a stop leaves comparable pairs.

New arms in this revision:

- `spark-rtm-vt`: only two settings differ from `spark-rtm`. The S3 runs use `s3-lt`, so they
  pair with P2's Flink `s3-lt` and P2's default Spark `s3-lt`, and isolate the tuning effect.
  `spark.sql.streaming.stateStore.rocksdb.trackTotalNumberOfRows` is checked in the OSS 4.3.0
  `spark-sql_2.13` jar (`RocksDBConf`: prefix `spark.sql.streaming.stateStore.rocksdb` +
  `trackTotalNumberOfRows`). All other arms now set it explicitly to Spark's default `true`
  (no behaviour change).
- 5-min cadence: `ckpt=300,secs=720` sets Spark's RTM trigger and Flink's checkpoint interval
  both to 300 s. It measures 720 s after the 60 s warmup, so each run crosses at least 2
  batch/checkpoint boundaries (they fall at about load+240–270 s and load+540–570 s). Estimate
  15.4 min per run.

## What the 11 h budget cuts

The runner stops at the first run that no longer fits (13 min of budget per run, 20 min for a
cadence run), so everything after it is cut. With the order above, 70 runs fit (~10.7 h) and
23 are cut:

- P4 `flink-ds-s1-update-ckpt300s-720s-r2` (the cadence test ends with 2 Spark runs and 1 Flink);
- P4 all 8 key-cardinality runs, all 6 recovery (kill) runs, all 4 hot-key runs;
- all of P5 (4 runs).

P1 to P3, the checkpoint 5 s / 30 s tests and 3 of 4 cadence runs fit. Real run times vary by
about ±10 %, so the cut line can move one or two runs.

Ways to keep the recovery and hot-key tests (not applied, your call):

1. Reorder P4 to cadence, kill, hot, ckpt, keys: kill and hot fit, and the cut moves into the
   second ckpt round and the keys runs.
2. Drop the S2 key-cardinality runs (they barely change state size, see caveat 2). This saves
   1.1 h but frees nothing unless P4 is also reordered, because they come before kill and hot.
3. Raise `BUDGET_S` to ~52200 (14.5 h) to run everything.

## Cost

At the 11 h budget: ~11 h runner time + ~1.5 h cluster create, image pulls and teardown, about
12.5 cluster hours, ~$50 at ~$4/h. Full plan with no cut: ~15.5 h, ~$62.

## Node placement (every session B arm)

`eks/cluster.yaml` (and the gitignored `cluster.local.yaml`) now splits the same 6x
m5.2xlarge into two managed node groups:

- `engine-ng`: 2 nodes, label + taint `bench/role=engine:NoSchedule`. Only engine pods
  tolerate it.
- `bench-ng`: 4 nodes for the Kafka brokers, producers, consumer, operators and the runner.

At render time the runner (`ENGINE_NODES=dedicated`, the default for plan B) adds a
nodeSelector and toleration to every Spark driver/executor and Flink JM/TM. It also adds
required pod anti-affinity, so the two workers always land one per node, with the driver/JM
next to one of them. The manifest files themselves are unchanged. Each run records
`placement.txt` (node of every engine and kafka-namespace pod) and logs a warning if any
engine node also hosts a kafka-namespace pod. Plan A keeps shared nodes, as session A ran.

Fit check from the pod requests (m5.2xlarge allocatable ≈ 7.75 vCPU / ~29 GiB after
daemonsets):

- **Engine nodes:** 4 vCPU (worker + driver/JM) and 3 vCPU used, out of 15.5. Both engines
  are capped by their own 3-core worker limits, so the spare cores help neither side.
- **Bench nodes:** Kafka 3 x 2 vCPU, stateful producers 8 x 2 vCPU, consumer 1, runner,
  operators and coredns ~1.5, about 24.5 of 31 vCPU. Memory is about 36 of ~116 GiB. It
  packs, with less headroom than before: brokers can burst to 3 cores and producers to 2.

So it fits on 6x m5.2xlarge; no extra node is needed. Check `placement.txt` and the producer
`DONE sent=` rate on the first runs.

## Caveats to decide before launch

1. **SQL dedup arm.** `s2-rt` is correct (0 leaks in the stall test), but Flink's rowtime
   keep-first holds each first row until the watermark passes it. Local smoke at 3k/s for
   60 s: `s2-rt` p50 5,168 ms, `s2-ttl60` p50 6.3 ms, `ds s2-et` p50 5.9 ms
   (`results/local-stateful/smoke-b/`). Spark's watermarked `dropDuplicates("event_id",
   "event_time")` emits the first row at once, so P1 uses `s2-ttl60` for SQL. To run `s2-rt`
   instead, change the `p1=` line in `plan_b`.
2. **Key cardinality on S2 doesn't test state size.** S2 state is keyed by `event_id`;
   `KEYS` only changes `user_id` (the Kafka key). In all three workloads, resident state is
   bounded by time, not key count:
   - S2 holds roughly the ids seen in the last ~5 s;
   - S1 holds at most one 10 s window plus lateness (≤ 500k keys at 50k evt/s);
   - S3 sessions close 5 s after their last event.

   Going from 1M to 10M users changes distinct users per S1 window from ~393k to ~488k; 50M
   gives ~497k. Moving the arms to S1 is a one-line change with a small real effect.
3. **50M resident keys** (if a workload with long-lived per-key state existed; not built,
   not measured): ~3–5 GB of RocksDB state per job on 100 GB node disks.
   - Flink: managed memory (~2.1 GB per 6 GB TM) keeps it bounded, and the rest spills to
     disk.
   - Spark: RocksDB native memory is unbounded by default and comes out of the 1 GB executor
     overhead, so it could be OOM-killed. The fair fix is `boundedMemoryUsage` +
     `maxMemoryUsageMB`, not a bigger pod.
4. **Stateless arms with dedicated nodes.** The images, manifests and resources are the
   published v43 ones, but the published runs shared nodes with Kafka and producers. The 3
   new runs per arm are a consistent isolated set, not a strict repeat of the published
   placement. To match the published placement, run P3 with `ENGINE_NODES=shared`; that needs
   a per-arm switch, which is not built.
5. **Recovery runs** delete one Spark executor or Flink TaskManager at load+180 s.
   - With one-worker-per-node anti-affinity, the replacement waits until the old pod is gone,
     so `recover_s` includes that termination for both engines.
   - The local Flink smoke gave outage 17.1 s, recover 18.1 s, 0 lost, 0 overcount. Spark
     executor loss in RTM has not been tried locally.
6. **Spark placement depends on the operator webhook** (`webhook.enable=true` in
   `eks/02_install.sh`). If it does not apply nodeSelector/tolerations, the Spark pods land
   on `bench-ng` and `placement.txt` flags it. Check it on the first Spark run.
