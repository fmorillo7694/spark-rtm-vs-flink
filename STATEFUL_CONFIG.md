# Stateful extension: workloads, semantics and every engine setting

Spark 4.3.0 Real-time Mode (RTM) vs Flink 2.3.0 on three stateful workloads. Same shared
datagen (`common/producer.py`), same event schema (`common/event_schema.md`), 6 Kafka
partitions, parallelism 6, output keyed by `user_id`. This file is the single record of what
each arm runs; the code it points at is the source of truth.

| file | role |
|---|---|
| `spark/java-rtm/src/main/java/bench/StatefulPipelineJava.java` | Spark: S1/S2/S3, RTM or micro-batch |
| `flink/java-datastream/src/main/java/bench/StatefulDataStream.java` | Flink DataStream: S1 window, S1 update-mode, S2, S3 |
| `flink/java-sql/src/main/java/bench/StatefulSql.java` | Flink SQL: S1 TUMBLE TVF, S2 ROW_NUMBER dedup |
| `spark/submit-stateful.sh`, `flink/submit-flink-stateful.sh` | local submit (all Spark state-store confs live here) |
| `bench/run_stateful.sh` | local runner: correctness replay + latency smoke per arm |
| `bench/stateful_input.sh`, `common/topic_capture.py`, `bench/stateful_check.py` | correctness input, capture, checker |
| `eks/jobs/stateful/*.yaml`, `eks/stateful/*` | EKS manifests + in-cluster matrix runner (not run yet) |

## Workloads

Shared knobs: watermark bound **5 s** (both engines), window **10 s**, session gap **5 s**.

| id | Spark (RTM primary, micro-batch floor) | Flink DataStream | Flink SQL |
|---|---|---|---|
| S1 windowed agg: count, sum(amount), max(ts) per (user_id, 10 s tumbling window) | `withWatermark(event_time, 5 s)` + `groupBy(window(event_time, "10 seconds"), user_id).agg(...)`, output mode update | `s1-window`: `keyBy(user_id).window(TumblingEventTimeWindows.of(10 s)).aggregate(...)`; `s1-update`: `KeyedProcessFunction` emitting the running window result per row | `s1`: `TABLE(TUMBLE(TABLE src, DESCRIPTOR(event_time), INTERVAL '10' SECOND))` GROUP BY user_id, window_start, window_end |
| S2 dedup on event_id | `withWatermark(event_time, 5 s).dropDuplicates("event_id", "event_time")` | `s2`: `keyBy(event_id)` + `ValueState<Boolean>` with TTL 5 s | `s2`: `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY proc_time) = 1`, `table.exec.state.ttl = 5 s` |
| S2 dedup, event-time cleanup (session B) | unchanged (already event-time) | `s2-et`: `keyBy(event_id)` + `ValueState<Boolean>`, event-time timer at the event's ts (fires when the watermark passes it) clears the state; rows with ts ≤ watermark are dropped as late | `s2-rt`: same ROW_NUMBER but `ORDER BY event_time` (planner: `Deduplicate(keep=FirstRow, order=ROWTIME, outputInsertOnly=true)`); `s2-ttl60`: proc-time query with `table.exec.state.ttl = 60 s` |
| S3 per-user session state machine with timers | Java `transformWithState`, `TimeMode.ProcessingTime()`, 4 `ValueState`s (start, last, count, timer) | `s3`: `KeyedProcessFunction`, same 4 states, processing-time timer | not built (no SQL equivalent with timers) |
| S3, light timer (session B) | `s3-lt`: same processor, deadline rounded up to 1 s and re-armed only when that second changes | `s3-lt`: same rule | not built |

Event time is the producer's `ts` (epoch seconds, double): Spark `timestamp_seconds(ts)`,
Flink `(long)(ts*1000)` in the DataStream timestamp assigner and
`TO_TIMESTAMP_LTZ(CAST(ts*1000 AS BIGINT), 3)` in SQL. Watermarks: Spark keeps one global
watermark (min over partitions, `max event time - 5 s`); Flink generates per-Kafka-partition
watermarks at the source (`forBoundedOutOfOrderness(5 s)`), the operator takes the min.

S2 keys on `(event_id, event_time)` in Spark rather than `event_id` alone. The datagen re-sends
exact copies (same bytes, same `ts`), so this is the same dedup, and putting the event-time
column in the key is what lets the watermark evict dedup state (`dropDuplicates` on
`event_id` alone keeps every id forever). `dropDuplicatesWithinWatermark` would be the direct
API but RTM 4.3 does not support it.

S3 rules (identical code paths in both engines):
- state per user: `start` (first ts), `last` (max ts so far), `count`, `timer`;
- an arriving event with `(ts - last) * 1000 > gapMs` emits the open session (`reason=gap`)
  and starts a new one; otherwise `count++`, `last = max(last, ts)` (duplicates arrive late);
- every arrival re-arms a processing-time timer at `now + timeoutMs` (default `timeoutMs =
  gapMs`); when it fires the open session is emitted (`reason=timeout`) and state is cleared.
- Processing time, not event time, on purpose: RTM event-time timers use the watermark fixed
  at the start of the long-running batch, so they would only fire at batch (checkpoint)
  boundaries; processing-time timers are checked per input row.

`s3-lt` (light timer, session B, both engines): the deadline `now + timeoutMs` is rounded up
to the next whole second and the timer is deleted and re-registered only when that rounded
deadline changes, so a busy key moves its timer at most once per second instead of on every
event. Expected semantic difference, nothing else changes:
- **Timeout emissions fire 0-1 s later** (0.5 s on average) than in `s3`: the timer fires at
  the rounded deadline, not at `arrival + timeout`. The `reason=timeout` latency anchor stays
  `last + timeoutMs`, so `s3-lt` timeout latencies include that rounding.
- **Session contents are the same.** Start, end (the last event's ts) and count follow the
  event-time gap rule, which the timer does not touch. An event that lands in the extra
  ≤ 1 s before the rounded timer fires closes the session as `reason=gap` instead of the
  session first closing as `timeout`; the session itself is identical as long as ts tracks
  arrival time (the datagen stamps ts at produce time).
- Local replay: both engines produce the identical 20,578 ground-truth sessions with `s3-lt`
  (`results/local-stateful/correctness/s3.txt`).

## Join workloads (session C)

| id | Spark | Flink DataStream |
|---|---|---|
| S4 stream-static enrichment join on user_id (inner) | **RTM**: `events.join(broadcast(ref), "user_id")`, the stream-static form the 4.3 RTM docs list as supported ("the static side must be broadcast"). `ref` = batch read of the compacted `st-ref` topic, `cache()`d and counted once at start | `s4`: `flatMap(RefJoin)` chained to the source; each TaskManager reads `st-ref` to its end once in `open()` into an in-memory map shared by its slots (`bench.Joins`) |
| S5 stream-stream interval join, orders ⋈ payments on order_id, `pay.ts ∈ [order.ts, order.ts + 10 s]` | **micro-batch only**: `orders.withWatermark(5 s).join(payments.withWatermark(5 s), "p_order_id = o_id AND p_time >= o_time AND p_time <= o_time + interval 10 seconds")`, inner, append mode; arms `spark-mb` (default trigger) and `spark-mb1s` (1 s trigger) | `s5`: `orders.keyBy(order_id).intervalJoin(payments.keyBy(order_id)).between(0, 10 s)`, event time, inclusive bounds, 5 s bounded-out-of-orderness watermarks on both sources |

Data (shared rules in `common/joindata.py`, used by the producer and the checker):

- **S4 reference table:** 1,000,000 user_ids, minus every `user_id % 50 == 0`, so 980,000
  rows; `{user_id, tier, region}`, where tier and region are a pure function of user_id.
  Written once per run to a compacted topic (`eks/jobs/stateful/ref-load.yaml`;
  `bench/join_input.sh` locally). Both engines read the same topic once at start. Events of
  users without a row must be dropped (inner join). This tests the miss path as well as the
  hit path.
- **S5:** the producer writes orders (`--rate` = orders/s). About 80 % get one payment
  U(0, 9.5 s) later. About 5 % get one payment U(11, 14 s) later, outside the bound, and
  must not join. The other 15 % get none. A payment's `ts` is its send wall clock. The 0.5 s
  margin keeps every in-bound pair away from the 10 s edge, where Spark's microsecond
  timestamps and Flink's millisecond timestamps could round differently. EKS base rate: 3,400
  orders/s per pod x 8, which is ~27k orders + ~23k payments = ~50k input records/s, the
  S1–S3 base rate.
- **Latency anchor:** S4 `ts` = the event's ts. S5 `ts` = the payment's ts (the later side;
  a pair cannot be emitted before both exist).

Why the Flink S4 analogue is "load the topic into a map in `open()`", and not the other
options:

- **Broadcast state** (`BroadcastProcessFunction`, what Databricks' `StreamTableJoinJob`
  uses) delivers the table as a second stream. Events can overtake it at start: Databricks'
  version drops events that find no row yet, which would show up as missing records in the
  correctness replay. It also writes the full map into every checkpoint from every subtask,
  a cost Spark's broadcast does not pay.
- **SQL lookup join** is a per-key request to an external system behind a cache, not an
  in-memory table.
- **SQL temporal (versioned) join** buffers events until the watermark passes, adding
  latency that the RTM join does not have.

The map in `open()` matches what Spark does: one copy per executor/TM, built before the
first record, never refreshed, not in checkpointed state, and a map-side join with no
shuffle. Mirrored from the Databricks benchmark: the broadcast hint, the map-side join, the
whole table on every worker. Differences: they join on a random key against 10k rows; we
join on user_id against ~1M rows with a 2 % miss rate.

Which knobs matter:

- **S4:** no shuffle in either engine (Spark: broadcast hash join; Flink: source → flatMap →
  sink chained into one task). So Spark's streaming-shuffle flush and Flink's buffer timeout
  should be no-ops. The session C plan still runs Flink at 100 ms and 0 ms to show it, but
  skips the vendor-tuned Spark arm: with no shuffle and no state store, both of its settings
  do nothing.
- **S5:** Flink `keyBy` → network exchange, so buffer timeout matters (local smoke p50
  6.5 ms at 0 ms vs 56 ms at 100 ms). Spark micro-batch adds the batch cadence.

**S5 is labelled "what you give up by leaving RTM", not RTM vs Flink.** OSS Spark 4.3 RTM
rejects stream-stream joins (`OPERATOR_OR_SINK_NOT_IN_ALLOWLIST`). Databricks Runtime has
added them to its RTM; OSS has not.

Local evidence (`results/local-stateful/`):

- **Correctness replay:** every arm PASSes.
  - S4: 129,535 enriched records, 0 missing, 0 extra, 0 wrong tier/region (Spark RTM,
    Flink 0 ms, Flink 100 ms).
  - S5: 96,014 pairs out of 120,000 orders / 101,995 payments (5,981 outside the bound),
    0 missing, 0 spurious, 0 duplicates (Flink 0 / 100 ms, Spark micro-batch default / 1 s).
- **Smokes:** 3k/s for 60 s, SMOKE ONLY, see the session C plan.

## Semantic differences that matter for the comparison (not bugs)

1. **Spark RTM S1 emits running results; Flink windows emit final results.** RTM requires
   update output mode, so every input row re-emits its window's current count/sum. Flink's
   window operator (DataStream `s1-window`, SQL TUMBLE) emits once per window when the
   watermark passes `window_end`, i.e. at least `window_end + 5 s`. These are different
   products. `s1-update` is the Flink arm with Spark's semantics (per-row updates) and is the
   like-for-like latency comparison; `s1-window`/SQL are reported for the "final result"
   question, where Spark's answer is "the last update you received", with no end-of-window
   marker.
2. **Watermark advancement.** In RTM the watermark is fixed for the whole long-running batch
   and advances only between batches (= every trigger duration, 60 s in the latency runs).
   Window state eviction and late-row dropping therefore move in 60 s steps on Spark and
   continuously on Flink. With the datagen's 3 s max duplicate delay < 5 s bound, no row is
   late on either engine. With real late data the engines would differ: Flink drops a row
   once its window's end ≤ the current per-partition-min watermark; Spark RTM keeps
   accepting it until the next batch boundary moves the watermark.
3. **Dedup retention.** Spark: event-time — an id is forgotten once the watermark passes its
   event_time, so retention is ≥ 5 s and up to trigger + 5 s under RTM. Flink DataStream `s2`:
   processing-time TTL 5 s after the first copy (`OnCreateAndWrite`,
   `NeverReturnExpired`, RocksDB compaction-filter cleanup). Flink SQL `s2`: idle-state TTL 5 s,
   processing time. A duplicate arriving 5-65 s after the original is dropped by Spark RTM and
   passed by Flink. The datagen's 3 s bound sits inside every arm's window, **but a processing
   stall longer than the TTL breaks it**: session A on EKS leaked 6 (50k) to 9,813 (100k)
   duplicates on the TTL arms, and the local stall test (`bench/stall_test.sh`, TaskManager
   frozen 10 s) leaks 281 / 282. Session B fixes it:
   - `s2-et` (DataStream) is Spark's semantics: state is cleared by an event-time timer when
     the watermark passes the event, and copies at or behind the watermark are dropped as
     late, exactly like Spark drops rows older than its watermark. Retention follows the data,
     so a stall changes nothing (stall test: 0 leaked). Cost: one event-time timer per event.
   - `s2-rt` (SQL, rowtime keep-first): leak-free the same way (stall test: 0), **but the
     operator emits each first row only when the watermark passes its rowtime**, so its
     latency is about the 5 s watermark bound. It answers "can Flink SQL dedup exactly under
     stalls", not "how fast"; for latency use `s2-ttl60`.
   - `s2-ttl60` (SQL): same low-latency processing-time dedup as `s2`, TTL 60 s (12x the
     bound, ~6x the worst stall plus duplicate delay seen so far). Still processing time: a
     stall longer than 60 s would leak again. Stall test: 0.
   The session A arms (`s2`) stay in the matrix under their old names for traceability.
4. **Timer firing on idle partitions.** RTM checks processing-time timers after each input row
   reaches the state partition and once at batch completion; there is no background poll.
   Flink fires processing-time timers from the timer service on time, idle or not. At the
   smoke rates every state partition gets rows every few ms, so this did not show; on an idle
   partition an RTM timer can wait up to the trigger duration.
5. **Delivery.** Both default arms are at-least-once into Kafka (RTM's Kafka sink is
   at-least-once; RTM processing is exactly-once for state). Flink's `ds-eo` arm (and
   `flink-sql-eo` on EKS) uses the transactional `EXACTLY_ONCE` Kafka sink; consumers then
   read with `isolation.level=read_committed`, and records only become visible at checkpoint
   commit (60 s cadence), which dominates e2e latency for that arm. RTM ships no exactly-once
   sink, so there is no Spark counterpart.

## Latency definitions (stateful)

Same per-row `out_ts` stamping as the stateless benchmark (per-row
`reflect('java.lang.System','currentTimeMillis')` in Spark, `System.currentTimeMillis()` in
Flink; `current_timestamp()` is frozen under RTM). Every output record carries `ts` and
`out_ts`; `common/latency_consumer.py` reports `pipeline_ms = out_ts - ts` and
`e2e_ms = consume_time - ts`. What `ts` means per workload:

| workload | `ts` in the output (latency anchor) | extra |
|---|---|---|
| S1 | `max(ts)` of the window so far. For update-mode outputs with no late rows this is the ts of the row that triggered the update, so `pipeline_ms` = per-update latency. For final-only window outputs it is the newest row of the window. | `window_ms = out_ts - window_end` (negative for update-mode results emitted before the window closes; ≈ watermark bound + delay for final results) |
| S2 | the event's own ts | |
| S3 | `reason=gap`: ts of the arriving event that closed the session. `reason=timeout`: `last + timeoutMs` (the inactivity deadline), so the value is how late the timer fired | |

Latency smokes run S1/S3 with `DUP_RATE=0`: a late duplicate would be measured against the
window's or session's newer max ts (and re-arms the S3 timer without moving `last`), inflating
the number by up to the 3 s duplicate delay. S2 smokes use 10 % duplicates.

## State backends and checkpointing

### Spark (all stateful arms, local and EKS)

| setting | value | note |
|---|---|---|
| `spark.sql.streaming.stateStore.providerClass` | `org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider` | RTM soft default too |
| `spark.sql.streaming.stateStore.checkpointFormatVersion` | `2` | RTM requires v2 |
| `spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled` | `true` | RTM soft default; progress JSON shows `rocksdbChangeLogWriterCommitLatencyMs` |
| `spark.sql.shuffle.partitions` | `6` | = state partitions |
| `spark.shuffle.streaming.networkBufferMaxWaitTimeMs` | Spark default `50` (arms `spark-rtm`, `spark-mb`); `1` (arm `spark-rtm-sw1`, local `SHUFFLE_MAX_WAIT_MS=1`) | internal conf, new in 4.3: how long a partly-filled streaming-shuffle buffer (`networkBufferSize`, default 32768 B) waits before it is flushed to the state task. The analogue of Flink's `execution.buffer-timeout`, which the Flink arms set to 0 ms. Below buffer-filling rates it sets the stateful RTM median (smoke: p50 32 ms at 50, 7 ms at 1). Both are run; which counts as "production-tuned" is the reader's call |
| `spark.sql.streaming.stateStore.rocksdb.trackTotalNumberOfRows` | Spark default `true` (every arm but one); `false` in arm `spark-rtm-vt` (EKS, session B) | Skips the extra row-count lookup per state update; the state row-count metric is no longer kept. Key checked in the OSS 4.3.0 `spark-sql_2.13` jar (`RocksDBConf`: prefix `spark.sql.streaming.stateStore.rocksdb` + `trackTotalNumberOfRows`). `spark-rtm-vt` = the vendor-tuned arm: this plus a 10 ms shuffle flush (`networkBufferMaxWaitTimeMs=10`), the settings from Databricks' latency-benchmarks repo that exist in OSS 4.3.0 (its async state checkpointing is DBR-only); everything else as `spark-rtm` |
| `spark.sql.adaptive.enabled` | `false` | RTM does not support AQE |
| trigger | `Trigger.RealTime("60 seconds")` (latency), `"10 seconds"` (correctness replays) | batch duration = checkpoint interval |
| `asyncProgressTrackingEnabled` | not set | stateful RTM commits synchronously; setting it true is unsupported |
| output mode | `update` | the only mode RTM accepts |
| task slots | 12 (local: 1 worker `--cores 12`, `spark.executor.cores=12`; EKS: 2 executors x `cores: 6`, `coreRequest/coreLimit: 3`) | see below |
| memory | local: executor 2560m + 512m overhead (= Flink TM 3072m). EKS: driver 1536+512m, executors 5120+1024m (= p6) | unchanged from stateless |

**Why 12 slots.** A stateful RTM query runs the Kafka source stage (6 tasks) and the stateful
stage (6 tasks) as one pipelined group, concurrently; the DAG scheduler refuses to start a
pipelined group without enough concurrent slots ("... concurrent task slots but only ...",
`PIPELINED_GROUP_SLOT_CHECK_ENABLED` in `DAGScheduler`). Flink runs the same two operator
tasks per slot through slot sharing, so 6 Flink slots = 12 Spark slots. Physical CPU is
identical: no local container is CPU-limited, and on EKS each executor pod requests and is
limited to 3 CPUs exactly like a Flink TaskManager.

### Flink (DataStream and SQL, set in the job so local and EKS match)

| setting | value |
|---|---|
| `state.backend.type` | `rocksdb` (EmbeddedRocksDBStateBackend; Flink 2.3's default is still `hashmap`; ForSt ships in the image but is not used) |
| `execution.checkpointing.incremental` | `true` |
| `execution.checkpointing.interval` | `60000 ms` (latency), `10000 ms` (correctness replays) |
| `execution.checkpointing.mode` | `EXACTLY_ONCE` (aligned barriers) |
| `execution.checkpointing.unaligned.enabled` | `false` |
| `execution.checkpointing.max-concurrent-checkpoints` | `1` |
| `execution.checkpointing.dir` | local `file:///tmp/flink-checkpoints/...` (named volume shared by JM+TM); EKS `s3://rtm-flink-bench-ckpt-<acct>/stateful/...` |
| `state.backend.rocksdb.timer-service.factory` | `ROCKSDB` (timers in RocksDB, the default) |
| `state.backend.rocksdb.memory.managed` | `true` (default; managed memory sized by the TM) |
| `execution.buffer-timeout` | `0 ms` (latency-tuned, every Flink arm); `100 ms` = Flink's default (arm `flink-ds-bt100` for s1-update/s2/s3, local `ds-bt100:<wl>`), job arg 13. Pairs with Spark's 50 ms default / 1 ms tuned streaming-shuffle flush, so the matrix has both engines at defaults and both tuned |
| SQL only | `table.local-time-zone = UTC`, `table.exec.state.ttl = 5 s` (S2), mini-batch off (default) |
| sink | `AT_LEAST_ONCE` (default arms); `EXACTLY_ONCE` + `transactional-id-prefix` + `transaction.timeout.ms=900000` (eo arms) |
| resources | local 1 TM 6 slots 3072m; EKS JM 1c/2048m + 2 TM x 3c/6144m, 3 slots each (= p6) |

Verified from the running jobs' REST `checkpoints/config`: `state_backend:
EmbeddedRocksDBStateBackend`, `checkpoint_storage: FileSystemCheckpointStorage`,
`mode: exactly_once`, `unaligned_checkpoints: false` (`results/local-stateful/smoke/*_evidence.txt`).

## RTM 4.3 support matrix, as hit by this work

Sources: [Spark 4.3.0 RTM guide](https://spark.apache.org/docs/4.3.0/streaming/real-time-mode.html)
and the operator allowlist compiled into `spark-sql_2.13-4.3.0.jar`
(`RealTimeModeAllowlist`).

| feature | RTM 4.3 | evidence |
|---|---|---|
| `dropDuplicates` (+ watermark) | yes | S2 ran; allowlist has `StreamingDeduplicateExec` |
| streaming aggregation incl. `window()` | yes, update mode | S1 ran; allowlist has `StatefulStreamlineAggregateExec`, `StateStoreRestoreExec/SaveExec` |
| Java `transformWithState`, processing-time timers | yes | S3 ran; allowlist has `TransformWithStateExec` |
| async progress tracking with state | no (sync commit) | docs; driver jstack shows no `async-log-write` thread in stateful runs |
| **stream-stream join** | **no** | docs list it under "not yet supported" (fails with `OPERATOR_OR_SINK_NOT_IN_ALLOWLIST`); no `StreamingSymmetricHashJoinExec` in the allowlist. Only broadcast stream-static joins (`BroadcastHashJoinExec`) are allowed |
| `flatMapGroupsWithState`, session windows, `dropDuplicatesWithinWatermark`, PySpark `transformWithState`, range repartition | no | docs; none in the allowlist |
| `count(distinct)` | no (general streaming restriction) | docs |
| exactly-once Kafka sink | no (at-least-once) | docs |
