package bench;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Stateful Flink SQL workloads (the SQL counterparts of StatefulDataStream / the Spark job):
 *
 *   s1  10 s TUMBLE windowing TVF, count + sum(amount) + max(ts) per user_id. The window TVF
 *       aggregation is insert-only: ONE final row per (user, window), emitted when the
 *       watermark (event_time - watermarkSec) passes window_end.
 *   s2  dedup on event_id: ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY proc_time) = 1
 *       (keep-first on processing time = insert-only Deduplicate operator), idle state TTL =
 *       watermark bound (table.exec.state.ttl). Session A arm; a stall longer than the TTL
 *       lets duplicates through.
 *   s2-ttl60  same query, processing-time TTL 60 s (far above any observed stall).
 *   s2-rt     keep-first ORDER BY event_time: watermark-driven, leak-free under stalls, but
 *             each row is emitted only when the watermark passes it (~ the 5 s bound later).
 *
 * Same state backend/checkpoint settings as StatefulDataStream (RocksDB, incremental,
 * EXACTLY_ONCE checkpoint mode). Output keyed by user_id (json key, like SqlPipeline).
 *
 * Args (positional):
 *   0 workload  s1 | s2 | s2-ttl60 | s2-rt
 *   1 bootstrap 2 inputTopic 3 outputTopic 4 parallelism
 *   5 checkpointMs (0 = off)  6 checkpointDir
 *   7 delivery  at-least-once | exactly-once   (Kafka SQL sink option values)
 *   8 startingOffsets latest | earliest
 *   9 watermarkSec (5)  10 windowSec (10)
 *  11 bufferTimeoutMs (0 = per-record flush, latency-tuned; 100 = Flink's default)
 */
public class StatefulSql {

    public static void main(String[] args) throws Exception {
        final String workload = args[0];
        final String bootstrap = args[1];
        final String inputTopic = args[2];
        final String outputTopic = args[3];
        final int parallelism = Integer.parseInt(args[4]);
        final long checkpointMs = args.length > 5 ? Long.parseLong(args[5]) : 60000L;
        final String checkpointDir = args.length > 6 ? args[6] : "file:///tmp/flink-checkpoints";
        final String delivery = args.length > 7 ? args[7] : "at-least-once";
        final String startup = (args.length > 8 ? args[8] : "latest") + "-offset";
        final int watermarkSec = args.length > 9 ? Integer.parseInt(args[9]) : 5;
        final int windowSec = args.length > 10 ? Integer.parseInt(args[10]) : 10;
        final long bufferTimeoutMs = args.length > 11 ? Long.parseLong(args[11]) : 0L;

        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        var c = env.getConfig();
        c.set("parallelism.default", String.valueOf(parallelism));
        c.set("execution.buffer-timeout", bufferTimeoutMs + " ms");
        // window_start/window_end are TIMESTAMP in the session zone; UTC makes them epoch-aligned.
        c.set("table.local-time-zone", "UTC");
        c.set("state.backend.type", "rocksdb");
        c.set("execution.checkpointing.incremental", "true");
        c.set("execution.checkpointing.dir", checkpointDir);
        c.set("state.backend.rocksdb.timer-service.factory", "ROCKSDB");
        c.set("state.backend.rocksdb.memory.managed", "true");
        if (checkpointMs > 0) {
            c.set("execution.checkpointing.interval", checkpointMs + " ms");
            c.set("execution.checkpointing.mode", "EXACTLY_ONCE");
            c.set("execution.checkpointing.unaligned.enabled", "false");
            c.set("execution.checkpointing.max-concurrent-checkpoints", "1");
        }
        if (workload.equals("s2")) {
            c.set("table.exec.state.ttl", watermarkSec + " s");   // session A arm
        } else if (workload.equals("s2-ttl60")) {
            // Processing-time idle-state TTL, 12x the watermark bound: far above the worst stall
            // seen in session A (6.3 s at 100k evt/s) plus the 3 s duplicate delay.
            c.set("table.exec.state.ttl", "60 s");
        } else if (workload.equals("s2-rt")) {
            // Rowtime keep-first (2.3 runtime: RowTimeDeduplicateKeepFirstRowFunction) buffers
            // the first row and emits it from an event-time timer when the watermark passes its
            // rowtime; later copies are either merged into the buffer or arrive behind the
            // watermark and are dropped as late. Leak-free under stalls, but every row waits for
            // the watermark (~ the 5 s bound). The TTL only bounds the already-emitted flag.
            c.set("table.exec.state.ttl", "60 s");
        }

        env.createTemporarySystemFunction("now_epoch", SqlPipeline.NowEpoch.class);

        env.executeSql(String.format(
                "CREATE TABLE src ("
                        + "  event_id BIGINT, user_id BIGINT, event_type STRING, country STRING,"
                        + "  amount DOUBLE, ts DOUBLE,"
                        + "  event_time AS TO_TIMESTAMP_LTZ(CAST(ts * 1000 AS BIGINT), 3),"
                        + "  proc_time AS PROCTIME(),"
                        + "  WATERMARK FOR event_time AS event_time - INTERVAL '%d' SECOND"
                        + ") WITH ("
                        + "  'connector' = 'kafka', 'topic' = '%s',"
                        + "  'properties.bootstrap.servers' = '%s',"
                        + "  'properties.group.id' = 'flink-sql-stateful-%s',"
                        + "  'scan.startup.mode' = '%s',"
                        + "  'format' = 'json', 'json.ignore-parse-errors' = 'true')",
                watermarkSec, inputTopic, bootstrap, workload, startup));

        String sinkOpts = String.format(
                "'connector' = 'kafka', 'topic' = '%s', 'properties.bootstrap.servers' = '%s',"
                        + " 'key.format' = 'json', 'key.fields' = 'user_id', 'value.format' = 'json',"
                        + " 'sink.delivery-guarantee' = '%s'", outputTopic, bootstrap, delivery);
        if (delivery.equals("exactly-once")) {
            sinkOpts += ", 'sink.transactional-id-prefix' = 'bench-sql-" + workload + "',"
                    + " 'properties.transaction.timeout.ms' = '900000'";
        }

        String insert;
        switch (workload) {
            case "s1":
                env.executeSql("CREATE TABLE sink_out (user_id BIGINT, window_start DOUBLE,"
                        + " window_end DOUBLE, cnt BIGINT, sum_amount DOUBLE, ts DOUBLE,"
                        + " out_ts DOUBLE) WITH (" + sinkOpts + ")");
                insert = String.format("INSERT INTO sink_out SELECT user_id,"
                        + " CAST(TIMESTAMPDIFF(SECOND, TIMESTAMP '1970-01-01 00:00:00', window_start) AS DOUBLE),"
                        + " CAST(TIMESTAMPDIFF(SECOND, TIMESTAMP '1970-01-01 00:00:00', window_end) AS DOUBLE),"
                        + " cnt, sum_amount, max_ts, now_epoch() FROM ("
                        + "  SELECT user_id, window_start, window_end, COUNT(*) AS cnt,"
                        + "         SUM(amount) AS sum_amount, MAX(ts) AS max_ts"
                        + "  FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(event_time), INTERVAL '%d' SECOND))"
                        + "  WHERE user_id IS NOT NULL"
                        + "  GROUP BY user_id, window_start, window_end)", windowSec);
                break;
            case "s2":
            case "s2-ttl60":
            case "s2-rt":
                env.executeSql("CREATE TABLE sink_out (event_id BIGINT, user_id BIGINT,"
                        + " event_type STRING, country STRING, amount DOUBLE, ts DOUBLE,"
                        + " out_ts DOUBLE) WITH (" + sinkOpts + ")");
                // s2 / s2-ttl60: keep-first on processing time (insert-only Deduplicate).
                // s2-rt: keep-first on the event-time attribute (rowtime deduplication).
                String order = workload.equals("s2-rt") ? "event_time" : "proc_time";
                insert = "INSERT INTO sink_out SELECT event_id, user_id, event_type, country,"
                        + " amount, ts, now_epoch() FROM ("
                        + "  SELECT event_id, user_id, event_type, country, amount, ts,"
                        + "         ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY " + order + " ASC) AS rn"
                        + "  FROM src WHERE user_id IS NOT NULL) WHERE rn = 1";
                break;
            default:
                throw new IllegalArgumentException("unknown workload " + workload + " (s1|s2|s2-ttl60|s2-rt)");
        }
        System.out.println("[StatefulSql] workload=" + workload + " delivery=" + delivery
                + " bufferTimeoutMs=" + bufferTimeoutMs + "\n" + env.explainSql(insert));
        env.executeSql(insert).await();
    }
}
