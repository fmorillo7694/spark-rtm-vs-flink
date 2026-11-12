# EKS p6 rerun: Spark 4.3.0 / Flink 2.3.0 (2026-11-06/07) — PARTIAL

Cluster `rtm-flink-bench4` (EKS 1.36, 6x m5.2xlarge, us-east-1), images tag `v43` (linux/amd64),
Strimzi 0.51.0 / Kafka 4.1.0 (3 brokers, RF 3), Flink operator 1.16.1, Spark operator 2.5.2.
Same fair p6 setup as the published round: 6 partitions / parallelism 6, 1 coordinator 1c/2 GB +
2 workers 3c/6144m pod-total per engine, keyed by user_id, durable S3 checkpoints every 60 s,
~100k evt/s in (8 producer pods), 60 s warm-up discarded, 300 s measured, ~10M records per run,
one engine at a time, fresh topics + fresh checkpoint path per run.

## Status: incomplete

Only 5 full runs completed (Spark RTM Java x3, Spark RTM Scala x2). The operator laptop lost
network repeatedly and then ran at load average ~150-180 from other workloads, so the run
driver stalled (one gap of ~2.4 h, later runs failed with rc=255 on connectivity). The cluster
reached the 9.5 h budget, so the matrix was stopped and the cluster torn down. Not run on EKS:
PySpark RTM, the async-OFF control, Flink DataStream, Flink SQL, PyFlink (full runs), the 5 s sweep,
micro-batch. Those arms only have the 90 s smoke runs below (3M records each, 1 run, NOT
comparable to the 300 s matrix for p99 — shorter window, fewer boundaries).

## Full runs (300 s window, ~10M records)

### Geomean of runs vs previous EKS p6 (Spark 4.1.2 / Flink 2.2.0, single run)

| engine | runs | pipe p50 | pipe p99 | pipe max | e2e p50 | e2e p99 | p99 near boundary | p99 elsewhere | CPU cores | mem GB | prev pipe p50 / p99 | prev e2e p99 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Spark RTM Java | 3 | 6.2 | 38.4 | 183.9 | 17.2 | 76.3 | 77.3 | 34.9 | 1.7 | 7.7 | 6.2 / 642.3 | 840.0 |
| Spark RTM Scala | 2 | 6.2 | 35.1 | 205.1 | 16.9 | 68.5 | 104.9 | 31.0 | 1.6 | 7.6 | 6.2 / 701.5 | 905.7 |

### Per run

| run | records | eps | pipe p50 | pipe p99 | pipe max | e2e p50 | e2e p99 | e2e max | near p99 / max | elsewhere p99 / max | CPU mean / peak | mem GB mean / peak | pods | evidence |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| spark-rtm-java-p6-r1 | 10001162 | 33337 | 6.2 | 39.2 | 182.1 | 17.2 | 77.3 | 231.8 | 42.0 / 180.0 | 39.0 / 170.0 | 1.6 / 3.6 | 7.6 / 8.1 | 3 | async-log-write=1, AsyncExec=1, offsets/commits=6/6 |
| spark-rtm-java-p6-r2 | 9997369 | 33325 | 6.1 | 37.9 | 202.2 | 17.3 | 78.2 | 306.8 | 110.0 / 200.0 | 33.0 / 150.0 | 1.8 / 3.9 | 7.7 / 8.0 | 3 | async-log-write=1, AsyncExec=1, offsets/commits=6/6 |
| spark-rtm-java-p6-r3 | 10003486 | 33345 | 6.2 | 38.0 | 169.0 | 17.2 | 73.6 | 229.4 | 100.0 / 160.0 | 33.0 / 160.0 | 1.8 / 4.7 | 7.7 / 8.0 | 3 | async-log-write=1, AsyncExec=1, offsets/commits=6/6 |
| spark-rtm-scala-p6-r1 | 10002840 | 33343 | 6.2 | 34.9 | 181.7 | 17.0 | 70.2 | 215.6 | 100.0 / 180.0 | 31.0 / 140.0 | 1.7 / 4.3 | 7.6 / 8.0 | 3 | async-log-write=1, AsyncExec=1, offsets/commits=6/6 |
| spark-rtm-scala-p6-r2 | 10000268 | 33334 | 6.3 | 35.3 | 231.6 | 16.7 | 66.8 | 270.6 | 110.0 / 190.0 | 31.0 / 230.0 | 1.6 / 3.5 | 7.7 / 8.1 | 3 | async-log-write=0, AsyncExec=0, offsets/commits=24/24 |

## Smoke runs (90 s window, 30 s warm-up, ~3M records, one run each) — indicative only

### Geomean of runs vs previous EKS p6 (Spark 4.1.2 / Flink 2.2.0, single run)

| engine | runs | pipe p50 | pipe p99 | pipe max | e2e p50 | e2e p99 | p99 near boundary | p99 elsewhere | CPU cores | mem GB | prev pipe p50 / p99 | prev e2e p99 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Spark RTM Java | 1 | 6.1 | 57.7 | 312.2 | 16.8 | 116.8 | 280.0 | 31.0 | 2.0 | 7.0 | 6.2 / 642.3 | 840.0 |
| PySpark RTM (native trigger) | 1 | 6.3 | 60.8 | 350.1 | 16.7 | 128.9 | 180.0 | 40.0 | 2.0 | 7.2 | 6.2 / 591.2 | 763.1 |
| Spark RTM Java, async tracking OFF | 1 | 6.2 | 1031.5 | 1516.5 | 17.1 | 1100.2 | 1480.0 | 44.0 | 1.9 | 7.0 | n/a | n/a |
| Flink DataStream (Java) | 1 | 6.0 | 32.6 | 138.3 | 12.2 | 60.6 | 63.0 | 27.0 | 1.3 | 4.7 | 5.9 / 29.8 | 52.6 |
| Flink SQL (Java) | 1 | 6.0 | 33.0 | 134.9 | 12.2 | 59.6 | 37.0 | 32.0 | 1.5 | 5.2 | 6.0 / 31.4 | 55.8 |
| PyFlink | 1 | 8.4 | 38.7 | 118.6 | 17.0 | 62.3 | 61.0 | 34.0 | 4.4 | 5.9 | 8.8 / 33.1 | 58.6 |

### Per run

| run | records | eps | pipe p50 | pipe p99 | pipe max | e2e p50 | e2e p99 | e2e max | near p99 / max | elsewhere p99 / max | CPU mean / peak | mem GB mean / peak | pods | evidence |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| spark-rtm-java-p6-r0 | 2999798 | 33331 | 6.1 | 57.7 | 312.2 | 16.8 | 116.8 | 325.0 | 280.0 / 310.0 | 31.0 / 300.0 | 2.0 / 4.1 | 7.0 / 7.9 | 3 | async-log-write=1, AsyncExec=1, offsets/commits=2/2 |
| pyspark-rtm-p6-r0 | 2999518 | 33328 | 6.3 | 60.8 | 350.1 | 16.7 | 128.9 | 391.3 | 180.0 / 250.0 | 40.0 / 350.0 | 2.0 / 3.4 | 7.2 / 8.1 | 3 | async-log-write=1, AsyncExec=1, offsets/commits=2/2 |
| spark-rtm-syncckpt-p6-r0 | 3000681 | 33341 | 6.2 | 1031.5 | 1516.5 | 17.1 | 1100.2 | 1527.5 | 1480.0 / 1510.0 | 44.0 / 1120.0 | 1.9 / 3.5 | 7.0 / 7.9 | 3 | async-log-write=0, AsyncExec=0, offsets/commits=2/2 |
| flink-p6-r0 | 3001984 | 33355 | 6.0 | 32.6 | 138.3 | 12.2 | 60.6 | 178.9 | 63.0 / 100.0 | 27.0 / 130.0 | 1.3 / 4.0 | 4.7 / 5.3 | 3 | ckpt completed=3 failed=1, TMs=2 |
| flink-sql-p6-r0 | 3001183 | 33346 | 6.0 | 33.0 | 134.9 | 12.2 | 59.6 | 251.6 | 37.0 / 57.0 | 32.0 / 130.0 | 1.5 / 3.2 | 5.2 / 5.5 | 3 | ckpt completed=3 failed=1, TMs=2 |
| pyflink-p6-r0 | 2999281 | 33325 | 8.4 | 38.7 | 118.6 | 17.0 | 62.3 | 172.7 | 61.0 / 79.0 | 34.0 / 110.0 | 4.4 / 6.1 | 5.9 / 6.4 | 3 | ckpt completed=3 failed=1, TMs=2 |

## Async progress tracking evidence

- Spark RTM Java/Scala/PySpark default arms: driver jstack shows one `async-log-write` thread and
  `AsyncProgressTrackingMicroBatchExecution`; commit log entries are `{"nextBatchWatermarkMs":0}`.
- Control arm (`asyncProgressTrackingEnabled=false`, smoke only): no `async-log-write` thread, no
  async execution class.
- Offsets/commits in S3 are written once per 60 s batch in both modes.

## Notes

- Old EKS numbers are the single-run 4.1.2/2.2.0 summaries (`results/eks/*-p6_summary.json` of the
  original checkout): RTM Java p99 642 ms, Scala 702, PySpark 591, Flink DS 30, Flink SQL 31, PyFlink 33.
- The old PyFlink p6 run had job parallelism 12, so the operator started 4 TaskManagers (~13 cores
  vs 7 for everyone else). Fixed to 6 here (smoke run shows 2 TMs).
- Flink smoke runs each show `failed=1` checkpoint in the REST counts with no failed entry in the
  history; completed checkpoints are 60 s apart and land in S3. Most likely the first trigger before
  all tasks were RUNNING. Not verified; it happens before the measured window.
- Boundary split percentiles come from 1 ms/10 ms histogram buckets (lower bounds).
