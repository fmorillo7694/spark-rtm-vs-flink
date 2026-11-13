# Local p6 rerun: Spark 4.3.0 / Flink 2.3.0 (2026-11-06)

Raw per-run files sit next to this doc (`<engine>-r<N>_summary.json`, `_stats.csv`, `_clock.jsonl`,
`_async_evidence.txt` for Spark RTM arms, `_checkpoints.json` for Flink arms; `matrix_summary.json`
is the machine-readable aggregate). Regenerate the tables with `python bench/analyze_local43.py`.
Per-record dumps (`*_latency.csv.gz`) and runner logs (`logs/`) are local only (gitignored).

## Setup

| item | value |
|---|---|
| Spark | 4.3.0, image `apache/spark:4.3.0-scala2.13-java17-ubuntu` (arm64), Scala 2.13.18, `spark-sql-kafka-0-10_2.13:4.3.0` |
| Flink | 2.3.0, image `flink:2.3.0-scala_2.12-java17` (arm64), PyFlink `apache-flink==2.3.0` in `pyflink-bench:2.3.0` |
| Flink Kafka connector | `flink-connector-kafka` / `flink-sql-connector-kafka` **5.0.0-2.2**. Maven Central has no 2.3-targeted release yet (newest: 5.0.0-2.1, 5.0.0-2.2); the 2.2 build ran on the 2.3 runtime with no errors. |
| Kafka | `apache/kafka:4.1.0`, KRaft single node, 6 partitions |
| Resources | 6 cores / 3072m per engine (Spark: 1 worker, executor 2560m + 512m overhead; Flink: 1 TM, 6 slots, process.size 3072m) |
| Workload | 10k evt/s, 300 s per run, first 15 s discarded (consumer warm-up), output keyed by user_id, 60 s checkpoints (RTM trigger 60 s / Flink interval 60 s) |
| Runs | 3 per engine, strictly one engine at a time (`bench/run_matrix_local.sh`) |

Engines: Spark RTM Java, Spark RTM Scala, PySpark RTM (now native `trigger(realTime=...)`),
Spark RTM Scala with `asyncProgressTrackingEnabled=false` (control arm), Spark micro-batch
(floor), Flink DataStream, Flink SQL, PyFlink. The previous numbers come from the single
Spark 4.1.2 / Flink 2.2.0 local p6 run (`results/<engine>_summary.json`). Spark RTM Java was
not part of that earlier local matrix.

## Read this first: the host was disturbed for about half the runs

The macOS host was swapping heavily during the matrix (`sysctl vm.swapusage`: about 28.0 of 28.7 GB
used), and other containers were running in the same Docker VM, including one stuck at
~100% CPU. That isn't part of this benchmark. The symptoms: multi-second stalls with no link
to any commit boundary (up to 11 s in pyflink-r3), Docker-VM clock jumps of 5-67 ms against
the host (from `bench/clock_probe.py`, sampled every 10 s), and producer rate dropping to
~9.6k/s in flink-r2. A run is flagged **host-disturbed** if the VM clock moved >5 ms or a
>1 s stall happened away from a commit boundary. 11 of 24 runs are flagged.

- **p50s are solid.** They agree within ~1 ms across all runs, flagged or not.
- **The p99 geomeans for Flink DataStream, PyFlink, micro-batch and the sync-ckpt arm are not
  usable as engine numbers.** Every Flink DataStream run was flagged. Fixing this means
  re-running those arms on a quiet host.
- The idle VM clock offset was -0.3 to -0.9 ms (VM slightly behind the host), so it moves
  ~5 ms latencies by under 1 ms. No correction was applied.

## Async progress tracking: evidence

| arm | `async-log-write` thread in driver jstack | `AsyncProgressTrackingMicroBatchExecution` frames | latest commit-log entry |
|---|---|---|---|
| Spark RTM Java / Scala / PySpark (default) | 1 in every run (9/9) | 1 in every run | `{"nextBatchWatermarkMs":0}` (AsyncCommitLog) |
| Spark RTM Scala, `asyncProgressTrackingEnabled=false` | 0 in every run (3/3) | 0 | `{"nextBatchWatermarkMs":0,"stateUniqueIds":{}}` (sync CommitLog) |

Both modes write one offsets/ and one commits/ entry per 60 s batch (5 each per run), so
the checkpoint cadence matches across arms. In Spark 4.3 the switch is
`StreamingQueryManager.useAsyncProgressTracking`, gated by
`spark.sql.streaming.realTimeMode.asyncProgressTrackingByDefault.enabled`. I found that in
the 4.3.0 jars. When on, it builds `AsyncProgressTrackingMicroBatchExecution`, which runs the
single-thread `async-log-write` executor.

## Finding: the RTM tail sits at batch boundaries, with or without async tracking

In every Spark RTM run, latency jumps to 200-860 ms in the 1-2 s around each 60 s batch
transition, when the long-running batch ends and the next one launches. The rest of the
time the tail stays near 8-10 ms (see the boundary table below). Async progress tracking
narrows the spike (near-boundary p99 210-430 ms async vs 480-700 ms sync) but does not
remove it. Only ~7% of records fall in those windows, so p99 crosses into the spike when a
spike runs a bit longer. That is why run-to-run RTM p99 swings between 9 and 23 ms on clean
runs. The same 60 s-periodic spikes were already in the 4.1.2 raw dump (110-260 ms, under
~1 s, so p99 stayed 9.4). In 4.3.0 they are larger and wider on this host. Host noise could
be inflating them, so this needs a quiet-host rerun before anyone calls it a regression.
Flink checkpoint triggers show no matching spike (near-checkpoint p99 15-30 ms on undisturbed runs).

Micro-batch p50 moved from 46 to ~66 ms, the same in all 3 runs. I didn't investigate it.

### Geomean of 3 runs vs previous local p6 (Spark 4.1.2 / Flink 2.2.0, single run)

| engine | pipe p50 | pipe p99 | e2e p50 | e2e p99 | CPU cores | mem MB | pipe p99, clean runs only | prev pipe p50 / p99 | prev e2e p99 |
|---|---|---|---|---|---|---|---|---|---|
| Spark RTM Java | 5.8 | 13.9 | 8.1 | 21.0 | 0.30 | 2175 | 17.1 (2/3) | n/a (new arm) | n/a |
| Spark RTM Scala | 6.3 | 21.5 | 8.4 | 29.8 | 0.30 | 2320 | 10.6 (2/3) | 6.3 / 9.4 | 12.0 |
| PySpark RTM (native trigger) | 6.0 | 9.9 | 8.0 | 16.1 | 0.26 | 2472 | 9.9 (3/3) | 5.7 / 8.6 | 11.2 |
| Spark RTM Scala, async tracking OFF | 5.8 | 173.9 | 8.1 | 246.1 | 0.35 | 2368 | 39.2 (1/3) | n/a (new arm) | n/a |
| Spark micro-batch (floor) | 65.7 | 166.4 | 69.0 | 193.6 | 1.75 | 2389 | 133.0 (1/3) | 46.1 / 78.1 | 81.6 |
| Flink DataStream (Java) | 5.6 | 156.2 | 13.3 | 418.6 | 0.47 | 1762 | n/a (0/3) | 5.6 / 8.1 | 16.5 |
| Flink SQL (Java) | 5.8 | 30.6 | 13.2 | 50.6 | 0.48 | 2131 | 30.6 (3/3) | 6.0 / 8.9 | 17.4 |
| PyFlink | 8.0 | 272.8 | 17.5 | 386.9 | 2.32 | 3226 | 15.8 (1/3) | 7.1 / 12.5 | 21.2 |

### Per run

| run | records | pipe p50 | pipe p99 | pipe max | e2e p50 | e2e p99 | CPU | mem MB | VM clock offset ms (mean, min..max) | evidence |
|---|---|---|---|---|---|---|---|---|---|---|
| spark-rtm-java-r1 ⚠ host-disturbed | 950427 | 6.1 | 9.3 | 352.0 | 8.3 | 13.3 | 0.29 | 2036 | -0.18 (-0.93..5.56) | async-log-write=1, AsyncExec frames=1, offsets/commits=5/5 |
| spark-rtm-java-r2 | 949894 | 5.9 | 12.7 | 444.0 | 8.2 | 21.7 | 0.31 | 2317 | -0.62 (-1.53..-0.25) | async-log-write=1, AsyncExec frames=1, offsets/commits=5/5 |
| spark-rtm-java-r3 | 950429 | 5.5 | 22.9 | 592.2 | 7.8 | 32.0 | 0.31 | 2171 | -0.68 (-1.25..-0.34) | async-log-write=1, AsyncExec frames=1, offsets/commits=5/5 |
| spark-rtm-scala-r1 ⚠ host-disturbed | 947870 | 6.8 | 88.1 | 590.9 | 9.2 | 114.8 | 0.40 | 2482 | 1.24 (-0.89..23.61) | async-log-write=1, AsyncExec frames=1, offsets/commits=5/5 |
| spark-rtm-scala-r2 | 948385 | 6.0 | 9.8 | 334.6 | 8.0 | 13.5 | 0.23 | 2106 | -0.46 (-0.83..-0.27) | async-log-write=1, AsyncExec frames=1, offsets/commits=5/5 |
| spark-rtm-scala-r3 | 949277 | 6.0 | 11.5 | 405.4 | 8.1 | 17.1 | 0.26 | 2371 | -0.5 (-0.91..-0.27) | async-log-write=1, AsyncExec frames=1, offsets/commits=5/5 |
| pyspark-rtm-r1 | 948453 | 6.1 | 9.9 | 309.0 | 8.0 | 20.8 | 0.28 | 2474 | -0.41 (-1.04..0.43) | async-log-write=1, AsyncExec frames=1, offsets/commits=5/5 |
| pyspark-rtm-r2 | 949861 | 6.1 | 10.0 | 330.6 | 8.0 | 13.9 | 0.27 | 2533 | -0.59 (-2.42..-0.26) | async-log-write=1, AsyncExec frames=1, offsets/commits=5/5 |
| pyspark-rtm-r3 | 950537 | 6.0 | 10.0 | 402.0 | 8.0 | 14.5 | 0.24 | 2410 | -0.46 (-0.73..-0.29) | async-log-write=1, AsyncExec frames=1, offsets/commits=5/5 |
| spark-rtm-syncckpt-r1 ⚠ host-disturbed | 947970 | 6.1 | 109.2 | 1178.6 | 8.3 | 129.9 | 0.31 | 2512 | -0.67 (-2.58..-0.28) | async-log-write=0, AsyncExec frames=0, offsets/commits=5/5 |
| spark-rtm-syncckpt-r2 ⚠ host-disturbed | 940107 | 5.6 | 1229.3 | 2509.9 | 8.2 | 1620.9 | 0.42 | 2438 | -1.75 (-18.6..-0.24) | async-log-write=0, AsyncExec frames=0, offsets/commits=5/5 |
| spark-rtm-syncckpt-r3 | 948242 | 5.7 | 39.2 | 857.4 | 7.8 | 70.8 | 0.33 | 2154 | -0.71 (-3.58..-0.3) | async-log-write=0, AsyncExec frames=0, offsets/commits=5/5 |
| spark-microbatch-r1 | 948679 | 65.9 | 133.0 | 314.5 | 69.2 | 139.2 | 1.77 | 2411 | -0.67 (-1.71..-0.33) |  |
| spark-microbatch-r2 ⚠ host-disturbed | 949869 | 66.4 | 147.4 | 321.6 | 69.7 | 153.1 | 1.76 | 2382 | 0.49 (-1.42..12.82) |  |
| spark-microbatch-r3 ⚠ host-disturbed | 945288 | 64.9 | 235.1 | 1608.5 | 68.1 | 340.5 | 1.73 | 2375 | -0.49 (-1.05..-0.26) |  |
| flink-r1 ⚠ host-disturbed | 928122 | 5.5 | 301.6 | 2580.1 | 13.1 | 617.3 | 0.44 | 1786 | -1.01 (-4.74..-0.33) | ckpt completed=7 failed=0 |
| flink-r2 ⚠ host-disturbed | 916891 | 5.5 | 58.5 | 2616.5 | 13.3 | 502.6 | 0.42 | 1731 | -1.41 (-12.77..-0.34) | ckpt completed=7 failed=0 |
| flink-r3 ⚠ host-disturbed | 948717 | 5.8 | 216.3 | 508.7 | 13.6 | 236.4 | 0.54 | 1768 | 0.86 (-5.25..58.32) | ckpt completed=7 failed=0 |
| flink-sql-r1 | 947465 | 6.0 | 111.3 | 338.0 | 13.4 | 136.6 | 0.53 | 2123 | -0.68 (-3.77..-0.12) | ckpt completed=6 failed=1 |
| flink-sql-r2 | 950848 | 5.6 | 16.7 | 289.3 | 13.2 | 34.6 | 0.45 | 2160 | -0.63 (-2.42..-0.12) | ckpt completed=6 failed=1 |
| flink-sql-r3 | 949701 | 5.8 | 15.4 | 114.6 | 13.2 | 27.5 | 0.45 | 2109 | -0.44 (-0.96..-0.09) | ckpt completed=6 failed=1 |
| pyflink-r1 ⚠ host-disturbed | 948202 | 8.1 | 161.7 | 1092.3 | 17.7 | 224.1 | 2.43 | 3115 | -0.77 (-3.07..-0.07) | ckpt completed=7 failed=0 |
| pyflink-r2 | 938720 | 7.7 | 15.8 | 121.9 | 16.7 | 28.1 | 2.09 | 3283 | -0.42 (-0.95..-0.05) | ckpt completed=7 failed=0 |
| pyflink-r3 ⚠ host-disturbed | 927410 | 8.0 | 7929.6 | 11345.2 | 18.3 | 9207.7 | 2.45 | 3281 | -5.71 (-67.23..-0.24) | ckpt completed=7 failed=0 |

### Tail near commit boundaries (record ts within -1s..+3s of a Spark batch start / Flink checkpoint trigger)

| run | boundaries | % records near | near p99 | near max | elsewhere p99 | elsewhere max |
|---|---|---|---|---|---|---|
| spark-rtm-java-r1 | 5 | 7.15 | 260.9 | 352.0 | 8.3 | 27.0 |
| spark-rtm-java-r2 | 5 | 7.08 | 286.8 | 444.0 | 9.8 | 70.4 |
| spark-rtm-java-r3 | 5 | 7.09 | 430.2 | 592.2 | 10.5 | 149.9 |
| spark-rtm-scala-r1 | 5 | 7.09 | 389.8 | 590.9 | 52.7 | 406.0 |
| spark-rtm-scala-r2 | 5 | 7.07 | 239.9 | 334.6 | 8.6 | 67.3 |
| spark-rtm-scala-r3 | 5 | 7.08 | 291.3 | 405.4 | 8.8 | 77.8 |
| pyspark-rtm-r1 | 5 | 7.12 | 209.1 | 309.0 | 8.7 | 89.2 |
| pyspark-rtm-r2 | 5 | 7.09 | 262.4 | 330.6 | 8.7 | 64.8 |
| pyspark-rtm-r3 | 5 | 7.12 | 275.2 | 402.0 | 8.6 | 42.5 |
| spark-rtm-syncckpt-r1 | 5 | 7.14 | 543.0 | 716.4 | 11.0 | 1178.6 |
| spark-rtm-syncckpt-r2 | 5 | 7.13 | 483.3 | 2509.9 | 1274.2 | 2432.6 |
| spark-rtm-syncckpt-r3 | 5 | 7.14 | 697.3 | 857.4 | 10.2 | 525.5 |
| flink-r1 | 7 | 7.2 | 109.3 | 252.4 | 357.8 | 2580.1 |
| flink-r2 | 7 | 7.04 | 20.7 | 82.8 | 67.8 | 2616.5 |
| flink-r3 | 7 | 7.07 | 16.9 | 87.4 | 241.3 | 508.7 |
| flink-sql-r1 | 6 | 5.93 | 28.5 | 50.7 | 121.6 | 338.0 |
| flink-sql-r2 | 6 | 6.01 | 15.2 | 53.1 | 16.9 | 289.3 |
| flink-sql-r3 | 6 | 5.96 | 17.7 | 43.6 | 15.3 | 114.6 |
| pyflink-r1 | 7 | 5.67 | 34.5 | 107.5 | 168.0 | 1092.3 |
| pyflink-r2 | 7 | 5.71 | 16.1 | 46.4 | 15.9 | 121.9 |
| pyflink-r3 | 7 | 5.88 | 108.4 | 166.1 | 8053.6 | 11345.2 |

## Gotchas hit (and fixes)

- The `apache/spark:4.3.0` image still has **no Python** and still runs with **HOME=/nonexistent**.
  The existing runner fixes (`apt-get install python3` on the driver, `HOME=/opt/spark` +
  `spark.jars.ivy`) are still needed and still work.
- `.env` set `RUN_SECONDS=300` unconditionally, so smoke runs couldn't shorten it. It is now
  `${RUN_SECONDS:-300}`.
- No 2.3-targeted Flink Kafka connector exists yet. I used 5.0.0-2.2 (see above).
- The Flink SQL / PyFlink blocking submit was handled by the existing backgrounded submit.
  The in-script consumer captured data on every run, so no manual fallback was needed.
- Flink SQL reports `failed=1` checkpoint per run, but there's no failed entry in the REST
  history. Most likely the in-flight checkpoint got aborted by the cancel at teardown. Not verified.
- The RTM `current_timestamp()` frozen-timestamp gotcha still applies. All Spark arms stamp
  `out_ts` per row (Scala UDF / `reflect(System.currentTimeMillis)`).
