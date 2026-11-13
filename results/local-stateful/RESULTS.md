# Local stateful runs: correctness + smoke (2026-11-08)

Spark 4.3.0 RTM (Java) and micro-batch vs Flink 2.3.0 DataStream and SQL, on the three
stateful workloads defined in [`STATEFUL_CONFIG.md`](../../STATEFUL_CONFIG.md). Local Docker on
an arm64 Mac under memory pressure, one engine stack at a time, Kafka 4.1 single node,
6 partitions. Spark: 1 worker, 12 task slots, executor 2560m + 512m. Flink: 1 TM, 6 slots,
3072m.

**The latency numbers below are SMOKE ONLY**: 3k evt/s, 90 s per arm, 20 s warm-up, one run
each, a laptop that also runs other containers. They show that each pipeline works and how it
emits. They are not benchmark results; the EKS matrix (`eks/stateful/`) is where results come
from.

Reproduce:

```bash
bench/stateful_input.sh                         # shared input -> st-corr-input + captures/input.jsonl
PHASE=replay bench/run_stateful.sh flink        # correctness replays (captures/*.jsonl)
PHASE=replay bench/run_stateful.sh spark
bench/stateful_correctness.sh                   # -> correctness/s{1,2,3}.{json,txt}
PHASE=smoke SMOKE_SECONDS=90 bench/run_stateful.sh flink
PHASE=smoke SMOKE_SECONDS=90 bench/run_stateful.sh spark
PHASE=smoke SMOKE_SECONDS=90 SHUFFLE_MAX_WAIT_MS=1 bench/run_stateful.sh spark rtm:s1 rtm:s2 rtm:s3
.venv/bin/python bench/analyze_stateful_smoke.py > results/local-stateful/smoke_table.md
```

## Correctness: every arm matches the ground truth

One deterministic input (`--seed 42`, 2k evt/s for 60 s, 5000 keys, 10 % hot-key share on
10 keys, 10 % duplicates re-sent 0-3 s later, then one heartbeat per partition at
last ts + 20 s so every window closes) was written once to `st-corr-input`: 131,892 records,
120,000 distinct events + 11,886 duplicates + 6 heartbeats. Each arm replayed it from
`earliest` into its own output topic (RTM trigger 10 s, Flink checkpoints 10 s, S3 inactivity
timeout 30 s so timers only close the sessions still open at the end).
`bench/stateful_check.py` computes the truth from the captured input and diffs every output
(`correctness/*.txt`, full detail in `correctness/*.json`; checker unit tests:
`python3 bench/test_stateful_check.py`).

| workload | arm | output records | result |
|---|---|---|---|
| S1 (31,649 user x window keys) | Spark RTM | 131,892 updates (up to 259 per key) | PASS: final count + sum(amount) of every key exact |
| | Spark micro-batch | 31,650 (1 per key + heartbeat) | PASS |
| | Flink DataStream `s1-update` | 131,892 updates | PASS |
| | Flink DataStream `s1-window` | 31,649 (exactly 1 per key) | PASS |
| | Flink SQL TUMBLE | 31,649 (exactly 1 per key) | PASS |
| S2 (120,000 distinct event_ids) | Spark RTM `dropDuplicates` | 120,000 | PASS: every id exactly once |
| | Flink DataStream TTL state | 120,000 | PASS |
| | Flink SQL ROW_NUMBER | 120,000 | PASS |
| | Flink DataStream EXACTLY_ONCE sink, read_committed | 120,000 | PASS |
| S3 (20,578 sessions) | Spark RTM `transformWithState` | 20,578 (15,578 gap + 5,000 timeout) | PASS: every (user, start, end, count) exact |
| | Flink DataStream `KeyedProcessFunction` | 20,578 (15,578 gap + 5,000 timeout) | PASS |

S1 counts include duplicates on both engines (a duplicate is just another event to an
aggregation). No row was late on any arm: the 3 s duplicate delay sits inside the 5 s
watermark bound. What this replay does not exercise, and where the engines would diverge on
real data, is listed under "Semantic differences" in `STATEFUL_CONFIG.md` (late rows, dedup
retention, idle-partition timers).

## Spark RTM evidence: synchronous commits, pipelined shuffle, RocksDB + changelog

From every Spark RTM smoke (`smoke/spark-rtm-*_evidence.txt`), taken while the query ran:

- driver `jstack`: **0** `async-log-write` threads and 0 `AsyncProgressTrackingMicroBatchExecution`
  frames (the stateless RTM runs show 1 and 1). The commit log entry is the synchronous v2
  format: `{"nextBatchWatermarkMs":...,"stateUniqueIds":{...}}`. Driver log:
  `MicroBatchExecution: Committed offsets for batch 0`.
- pipelined shuffle: driver log `DAGScheduler: Submitting ResultStage N concurrently with its
  running pipelined producer(s) List(ShuffleMapStage N)`, and the driver REST API shows the
  source stage and the stateful stage ACTIVE at the same time, 6 running tasks each.
- state store: RocksDB (`stateStore=...RocksDBStateStoreProvider` printed by the job),
  checkpoint format v2, changelog checkpointing on: `state/0/<partition>/<v>_<uuid>.changelog`
  files in the checkpoint and `rocksdbChangeLogWriterCommitLatencyMs` in query progress.

Flink evidence (`smoke/flink-*_evidence.txt`, REST `checkpoints/config`):
`EmbeddedRocksDBStateBackend`, `FileSystemCheckpointStorage`, `exactly_once`, unaligned off,
incremental on; TM log `Loading state backend via factory ...EmbeddedRocksDBStateBackendFactory`.
The `failed: 1` in every Flink checkpoint count is the first trigger attempt at job start:
JM log `Failed to trigger checkpoint ... since Checkpoint triggering task Source: kafka-source
(1/6) ... is not being executed at the moment`. Every real checkpoint completed (the stateless
EKS notes had flagged the same count as unexplained).

## Smoke observations (SMOKE ONLY, not results)

Milliseconds. `window` = `out_ts - window_end` (S1 only). CPU/mem = mean over the engine
containers (Kafka excluded). S1/S3 smokes ran without duplicates, S2 with 10 % (see the
latency-anchor note in `STATEFUL_CONFIG.md`). `-sw1` = Spark with
`spark.shuffle.streaming.networkBufferMaxWaitTimeMs=1`.

| arm (SMOKE ONLY) | out records | pipe p50 | p95 | p99 | max | e2e p50 | e2e p99 | window p50 | window p99 | CPU cores | mem GB |
|---|---|---|---|---|---|---|---|---|---|---|---|
| spark-rtm-s1 (default shuffle flush) | 209910 | 32.0 | 55.5 | 734.2 | 1452.3 | 34.8 | 750.8 | -4964 | -66 | 0.6 | 1.7 |
| spark-rtm-s1-sw1 | 210001 | 6.7 | 8.3 | 330.0 | 1041.2 | 8.8 | 335.9 | -4996 | -95 | 0.8 | 1.7 |
| spark-microbatch-s1 | 190786 | 240.5 | 367.5 | 464.6 | 614.8 | 243.6 | 467.8 | -4784 | 199 | 3.9 | 2.6 |
| flink-ds-s1-update | 210001 | 6.2 | 7.2 | 8.2 | 115.4 | 13.0 | 15.6 | -4995 | -93 | 0.5 | 1.9 |
| flink-ds-bt100-s1-update (Flink default 100 ms buffer timeout) | 209995 | 53.8 | 99.0 | 104.7 | 120.4 | 63.0 | 112.8 | -4945 | -47 | 0.3 | 1.9 |
| flink-ds-s1-window | 103825 | 8538 | 14109 | 14907 | 15179 | 8568 | 14940 | 5110 | 5183 | 0.4 | 1.8 |
| flink-sql-s1 (TUMBLE) | 103778 | 8612 | 14233 | 15007 | 15315 | 8637 | 15028 | 5217 | 5322 | 0.2 | 2.8 |
| spark-rtm-s2 | 210015 | 33.0 | 56.6 | 894.6 | 1609.8 | 36.5 | 917.1 | | | 0.7 | 1.8 |
| spark-rtm-s2-sw1 | 209766 | 7.4 | 9.5 | 584.9 | 1294.0 | 10.2 | 593.9 | | | 1.1 | 1.6 |
| flink-ds-s2 | 210011 | 5.6 | 7.3 | 10.5 | 113.7 | 13.2 | 20.6 | | | 0.6 | 2.5 |
| flink-sql-s2 | 209971 | 5.7 | 6.7 | 8.3 | 115.1 | 12.8 | 16.6 | | | 0.6 | 3.2 |
| flink-ds-eo-s2 (EXACTLY_ONCE, read_committed) | 131460 | 6.5 | 8.5 | 22.2 | 146.7 | 22696 | 43983 | | | 0.6 | 2.9 |
| spark-rtm-s3 | 96379 | 48.8 | 97.4 | 1026.4 | 1464.9 | 51.5 | 1035.8 | | | 0.8 | 2.0 |
| spark-rtm-s3-sw1 | 96386 | 7.3 | 18.9 | 776.6 | 1195.5 | 16.5 | 802.3 | | | 0.9 | 1.7 |
| flink-ds-s3 | 96218 | 6.9 | 8.5 | 11.2 | 197.9 | 13.7 | 19.3 | | | 0.5 | 2.3 |

What the smokes show (to be confirmed at scale on EKS):

1. **Stateful RTM's median is set by the streaming-shuffle flush timer.** With Spark's defaults
   (`spark.shuffle.streaming.networkBufferSize` 32 KB, `...networkBufferMaxWaitTimeMs` 50 ms,
   both read from the 4.3.0 jar) a 3k evt/s stream never fills a buffer, so rows wait for the
   50 ms flush: p50 32-49 ms. At 1 ms the p50 drops to 7 ms, level with Flink (which runs
   `execution.buffer-timeout: 0 ms`). The knob is marked `internal()` in Spark; that it is the
   fair setting is a judgment call, so the EKS matrix runs both (`spark-rtm` = 50 ms default,
   `spark-rtm-sw1` = 1 ms). Flink has the same trade-off: at its default 100 ms
   `execution.buffer-timeout` (`flink-ds-bt100`) the S1 update-mode p50 is 53.8 ms, behind
   Spark's default 32 ms, while tuned vs tuned is 6.2 vs 6.7 ms. The EKS matrix covers the
   full 2x2 (both at defaults, both tuned) for s1-update/s2/s3.
2. **The stateful RTM tail sits at the batch boundary.** `bench/stateful_boundary_split.py`
   on the `spark-rtm-s1-sw1` run: p99 9.9 ms (max 59 ms) away from the 60 s batch transition,
   p99 979 ms (max 1041 ms) within [-2 s, +3 s] of it. Stateful RTM commits synchronously, so
   each transition stalls the pipeline for about a second; a 90 s smoke has one transition,
   which alone is ~7 % of the steady-state records, enough to own the overall p99. Flink
   `s1-update` around its checkpoints: p99 44.6 ms (max 115 ms), elsewhere 8.0 ms.
3. **S1 output semantics differ** (expected, see `STATEFUL_CONFIG.md`): Spark RTM and Flink
   `s1-update` emit a running result per input row (`window` p50 ≈ -5 s: half-way through the
   10 s window), while Flink window/TUMBLE emits once per window at `window_end + 5 s`
   watermark bound + ~0.1-0.3 s. Micro-batch update mode emits per batch, ~240 ms median.
4. **EXACTLY_ONCE costs visibility latency, not processing latency**: `pipe` (stamped before
   the transactional write) stays 6.5 ms, but a read_committed consumer only sees records when
   the 60 s checkpoint commits the transaction, so e2e p50 is 22.7 s.
5. Micro-batch S1 used 3.9 cores mean vs 0.6-1.1 for RTM and ~0.5 for Flink at the same rate
   (back-to-back stateful micro-batches, each planned and committed).
