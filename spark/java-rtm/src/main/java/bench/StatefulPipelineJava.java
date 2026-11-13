package bench;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.ExpiredTimerInfo;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StatefulProcessor;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.TTLConfig;
import org.apache.spark.sql.streaming.TimeMode;
import org.apache.spark.sql.streaming.TimerValues;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.streaming.ValueState;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.jdk.javaapi.CollectionConverters;

import static org.apache.spark.sql.functions.*;

/**
 * Stateful Kafka -> Kafka workloads for the Spark RTM vs Flink benchmark (Spark 4.3 RTM
 * supports dropDuplicates, streaming aggregation incl. window(), and JVM transformWithState
 * via pipelined shuffles). Same event contract as the stateless job (common/event_schema.md),
 * output keyed by user_id. All three run in update output mode (the only mode RTM accepts),
 * and no asyncProgressTrackingEnabled option is set: stateful RTM commits synchronously.
 *
 *   s1  10 s tumbling window per user_id: count + sum(amount) + max(ts), event-time
 *       watermark. Update mode emits the running result of the window on every input row.
 *   s2  dedup on event_id: withWatermark + dropDuplicates(event_id, event_time). The producer
 *       re-sends exact copies (same ts), so adding event_time to the key keeps the semantics
 *       of "dedup on event_id" while letting the watermark evict state.
 *   s3  per-user session state machine with transformWithState + PROCESSING-time timers:
 *       a session closes on an event-time gap > gapMs seen on the next event ("gap"), or when
 *       the inactivity timer (arrival processing time + timeoutMs, default = gapMs) fires
 *       ("timeout"). Correctness replays set timeoutMs >> replay time so timers only close
 *       the sessions still open at the end of the input (deterministic output).
 *
 * out_ts is stamped per row: reflect(System.currentTimeMillis) in the final projection for
 * s1/s2 (current_timestamp() is frozen under RTM), System.currentTimeMillis() in the
 * processor for s3.
 *
 * Args (positional):
 *   0 workload        s1 | s2 | s3 | s3-lt (s3 with a 1 s-bucket light timer)
 *   1 mode            rtm | microbatch
 *   2 bootstrap       e.g. kafka:9092
 *   3 inputTopic
 *   4 outputTopic
 *   5 checkpointDir
 *   6 trigger         RTM batch (= checkpoint) duration, e.g. "60 seconds" (microbatch: ignored)
 *   7 startingOffsets latest | earliest (earliest = correctness replay)
 *   8 watermarkSec    default 5
 *   9 windowSec       default 10
 *  10 sessionGapMs    default 5000
 *  11 sessionTimeoutMs default = sessionGapMs
 *  12 secondTopic     s4: compacted reference topic; s5: payments topic (inputTopic = orders)
 *  13 mbTrigger       micro-batch trigger interval, e.g. "1 second"; "" = Spark default
 *  14 joinBoundSec    s5 time bound, default 10
 *
 *   s4  stream-static enrichment join: events JOIN broadcast(reference) ON user_id (inner;
 *       users without a reference row are dropped). The RTM-supported join form.
 *   s5  stream-stream inner join of orders and payments, payment.ts in
 *       [order.ts, order.ts + joinBoundSec], watermarks on both sides. MICRO-BATCH ONLY:
 *       RTM 4.3 does not support stream-stream joins, so this measures what leaving RTM
 *       costs, not RTM vs Flink.
 */
public class StatefulPipelineJava {

    static final String OUT_TS =
            "CAST(reflect('java.lang.System','currentTimeMillis') AS DOUBLE) / 1000.0";

    static final StructType REF_SCHEMA = new StructType()
            .add("user_id", DataTypes.LongType)
            .add("tier", DataTypes.StringType)
            .add("region", DataTypes.StringType);
    static final StructType ORDER_SCHEMA = new StructType()
            .add("order_id", DataTypes.LongType)
            .add("user_id", DataTypes.LongType)
            .add("amount", DataTypes.DoubleType)
            .add("ts", DataTypes.DoubleType);
    static final StructType PAYMENT_SCHEMA = new StructType()
            .add("payment_id", DataTypes.LongType)
            .add("order_id", DataTypes.LongType)
            .add("user_id", DataTypes.LongType)
            .add("amount", DataTypes.DoubleType)
            .add("ts", DataTypes.DoubleType);

    static Dataset<Row> readJson(SparkSession spark, String bootstrap, String topic,
                                 String startingOffsets, StructType schema) {
        return spark.readStream().format("kafka")
                .option("kafka.bootstrap.servers", bootstrap)
                .option("subscribe", topic)
                .option("startingOffsets", startingOffsets)
                .option("failOnDataLoss", "false")
                .load()
                .selectExpr("CAST(value AS STRING) AS json_str")
                .select(from_json(col("json_str"), schema).alias("e"))
                .select("e.*");
    }

    public static void main(String[] args) throws Exception {
        final String workload = args[0];
        final String mode = args[1];
        final String bootstrap = args[2];
        final String inputTopic = args[3];
        final String outputTopic = args[4];
        final String checkpointDir = args[5];
        final String trigger = args.length > 6 ? args[6] : "60 seconds";
        final String startingOffsets = args.length > 7 ? args[7] : "latest";
        final int watermarkSec = args.length > 8 ? Integer.parseInt(args[8]) : 5;
        final int windowSec = args.length > 9 ? Integer.parseInt(args[9]) : 10;
        final long sessionGapMs = args.length > 10 ? Long.parseLong(args[10]) : 5000L;
        final long sessionTimeoutMs = args.length > 11 ? Long.parseLong(args[11]) : sessionGapMs;
        final String secondTopic = args.length > 12 ? args[12] : "";
        final String mbTrigger = args.length > 13 ? args[13] : "";
        final int joinBoundSec = args.length > 14 && !args[14].isEmpty() ? Integer.parseInt(args[14]) : 10;

        SparkSession spark = SparkSession.builder()
                .appName("stateful-bench-" + workload + "-" + mode)
                .config("spark.sql.adaptive.enabled", "false")
                .getOrCreate();

        Dataset<Row> parsed = readJson(spark, bootstrap, inputTopic, startingOffsets,
                workload.equals("s5") ? ORDER_SCHEMA : RtmPipelineJava.EVENT_SCHEMA)
                .filter(col("user_id").isNotNull().and(col("ts").isNotNull()));

        Dataset<Row> out;
        String outputMode = "update";
        switch (workload) {
            case "s4": {
                // Stream-static join (enrichment), the RTM-supported form: the static side is a
                // batch read of the compacted reference topic, cached once at start (so it is
                // not re-read every batch), and broadcast, so the join is map-side (no shuffle).
                Dataset<Row> ref = spark.read().format("kafka")
                        .option("kafka.bootstrap.servers", bootstrap)
                        .option("subscribe", secondTopic)
                        .option("startingOffsets", "earliest")
                        .option("endingOffsets", "latest")
                        .load()
                        .selectExpr("CAST(value AS STRING) AS json_str")
                        .select(from_json(col("json_str"), REF_SCHEMA).alias("r"))
                        .select("r.*")
                        .filter(col("user_id").isNotNull())
                        .cache();
                long refRows = ref.count();
                System.out.println("[StatefulPipelineJava] s4 reference rows=" + refRows + " topic=" + secondTopic);
                out = parsed.join(broadcast(ref), "user_id").select(
                        col("user_id").cast(DataTypes.StringType).alias("key"),
                        to_json(struct(col("event_id"), col("user_id"), col("event_type"),
                                col("country"), col("amount"), col("ts"), col("tier"), col("region"),
                                expr(OUT_TS).alias("out_ts"))).alias("value"));
                break;
            }
            case "s5": {
                // Stream-stream inner join with watermarks on both sides and a time-range
                // condition: payment within [order.ts, order.ts + bound]. Micro-batch only (RTM
                // 4.3 rejects stream-stream joins); append is the only output mode it allows.
                if (mode.equals("rtm")) {
                    throw new IllegalArgumentException("s5 (stream-stream join) is micro-batch only in Spark 4.3");
                }
                Dataset<Row> orders = parsed
                        .select(col("order_id").alias("o_id"), col("user_id"), col("amount"),
                                col("ts").alias("o_ts"))
                        .withColumn("o_time", timestamp_seconds(col("o_ts")))
                        .withWatermark("o_time", watermarkSec + " seconds");
                Dataset<Row> payments = readJson(spark, bootstrap, secondTopic, startingOffsets, PAYMENT_SCHEMA)
                        .filter(col("order_id").isNotNull().and(col("ts").isNotNull()))
                        .select(col("order_id").alias("p_order_id"), col("ts").alias("p_ts"))
                        .withColumn("p_time", timestamp_seconds(col("p_ts")))
                        .withWatermark("p_time", watermarkSec + " seconds");
                Dataset<Row> joined = orders.join(payments, expr("p_order_id = o_id AND p_time >= o_time"
                        + " AND p_time <= o_time + interval " + joinBoundSec + " seconds"));
                out = joined.select(
                        col("user_id").cast(DataTypes.StringType).alias("key"),
                        to_json(struct(col("o_id").alias("order_id"), col("user_id"), col("amount"),
                                col("o_ts").alias("order_ts"), col("p_ts").alias("pay_ts"),
                                col("p_ts").alias("ts"),     // latency anchor: the later side
                                expr(OUT_TS).alias("out_ts"))).alias("value"));
                outputMode = "append";
                break;
            }
            case "s1": {
                Dataset<Row> agg = parsed
                        .withColumn("event_time", timestamp_seconds(col("ts")))
                        .withWatermark("event_time", watermarkSec + " seconds")
                        .groupBy(window(col("event_time"), windowSec + " seconds"), col("user_id"))
                        .agg(count(lit(1)).alias("cnt"),
                                sum("amount").alias("sum_amount"),
                                max("ts").alias("max_ts"));
                out = agg.select(
                        col("user_id").cast(DataTypes.StringType).alias("key"),
                        to_json(struct(
                                col("user_id"),
                                unix_micros(col("window.start")).divide(1e6).alias("window_start"),
                                unix_micros(col("window.end")).divide(1e6).alias("window_end"),
                                col("cnt"),
                                col("sum_amount"),
                                col("max_ts").alias("ts"),
                                expr(OUT_TS).alias("out_ts"))).alias("value"));
                break;
            }
            case "s2": {
                Dataset<Row> dedup = parsed
                        .withColumn("event_time", timestamp_seconds(col("ts")))
                        .withWatermark("event_time", watermarkSec + " seconds")
                        .dropDuplicates("event_id", "event_time");
                out = dedup.select(
                        col("user_id").cast(DataTypes.StringType).alias("key"),
                        to_json(struct(col("event_id"), col("user_id"), col("event_type"),
                                col("country"), col("amount"), col("ts"),
                                expr(OUT_TS).alias("out_ts"))).alias("value"));
                break;
            }
            case "s3":
            case "s3-lt": {
                long granMs = workload.equals("s3-lt") ? 1000L : 0L;
                Dataset<Tuple2<String, String>> sessions = parsed
                        .select(col("user_id"), col("ts"))
                        .groupByKey((MapFunction<Row, Long>) r -> r.getLong(0), Encoders.LONG())
                        .transformWithState(new SessionProcessor(sessionGapMs, sessionTimeoutMs, granMs),
                                TimeMode.ProcessingTime(), OutputMode.Update(),
                                Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
                out = sessions.toDF("key", "value");
                break;
            }
            default:
                throw new IllegalArgumentException("unknown workload " + workload + " (s1|s2|s3|s3-lt|s4|s5)");
        }

        DataStreamWriter<Row> writer = out.writeStream().format("kafka")
                .option("kafka.bootstrap.servers", bootstrap)
                .option("topic", outputTopic)
                .option("checkpointLocation", checkpointDir)
                .outputMode(outputMode);
        StreamingQuery query;
        if (mode.equals("rtm")) {
            query = writer.trigger(Trigger.RealTime(trigger)).start();
        } else if (mode.equals("microbatch")) {
            // "" = Spark's default trigger (next batch as soon as the previous one ends)
            query = (mbTrigger.isEmpty() ? writer : writer.trigger(Trigger.ProcessingTime(mbTrigger))).start();
        } else {
            throw new IllegalArgumentException("unknown mode '" + mode + "' (use rtm|microbatch)");
        }
        System.out.println("[StatefulPipelineJava] workload=" + workload + " mode=" + mode
                + " id=" + query.id()
                + " trigger=" + (mode.equals("rtm") ? "RealTime(" + trigger + ")"
                        : "micro-batch(" + (mbTrigger.isEmpty() ? "default" : mbTrigger) + ")")
                + " outputMode=" + outputMode + (secondTopic.isEmpty() ? "" : " secondTopic=" + secondTopic)
                + (workload.equals("s5") ? " joinBound=" + joinBoundSec + "s" : "")
                + " startingOffsets=" + startingOffsets + " watermark=" + watermarkSec + "s"
                + " window=" + windowSec + "s gapMs=" + sessionGapMs + " timeoutMs=" + sessionTimeoutMs
                + " stateStore=" + spark.conf().get("spark.sql.streaming.stateStore.providerClass", "<engine default>")
                + " ckpt=" + checkpointDir);
        query.awaitTermination();
    }

    /**
     * Session state machine, one instance per state partition. State per user: first/last
     * event ts of the open session, event count, and the registered processing-time timer.
     * In RTM handleInputRows is called once per row; in micro-batch with all rows of the key in
     * the batch, so the loop handles both.
     */
    public static final class SessionProcessor
            extends StatefulProcessor<Long, Row, Tuple2<String, String>> {
        private final long gapMs;
        private final long timeoutMs;
        private final long granMs;
        private transient ValueState<Double> start;
        private transient ValueState<Double> last;
        private transient ValueState<Long> count;
        private transient ValueState<Long> timer;

        public SessionProcessor(long gapMs, long timeoutMs, long granMs) {
            this.gapMs = gapMs;
            this.timeoutMs = timeoutMs;
            this.granMs = granMs;
        }

        @Override
        public void init(OutputMode outputMode, TimeMode timeMode) {
            start = getHandle().getValueState("start", Encoders.DOUBLE(), TTLConfig.NONE());
            last = getHandle().getValueState("last", Encoders.DOUBLE(), TTLConfig.NONE());
            count = getHandle().getValueState("count", Encoders.LONG(), TTLConfig.NONE());
            timer = getHandle().getValueState("timer", Encoders.LONG(), TTLConfig.NONE());
        }

        @Override
        public scala.collection.Iterator<Tuple2<String, String>> handleInputRows(
                Long key, scala.collection.Iterator<Row> rows, TimerValues timerValues) {
            List<Tuple2<String, String>> out = new ArrayList<>(1);
            while (rows.hasNext()) {
                double ts = rows.next().getDouble(1);
                if (last.exists()) {
                    double l = last.get();
                    if ((ts - l) * 1000.0 > gapMs) {
                        out.add(emit(key, start.get(), l, count.get(), "gap", ts));
                        start.update(ts);
                        last.update(ts);
                        count.update(1L);
                    } else {
                        last.update(Math.max(l, ts));   // duplicates arrive late: keep the max
                        count.update(count.get() + 1);
                    }
                } else {
                    start.update(ts);
                    last.update(ts);
                    count.update(1L);
                }
            }
            // Inactivity timer, processing time. granMs 0 (s3): re-armed on every arrival.
            // granMs > 0 (s3-lt): deadline rounded up to a granMs boundary, re-armed only when
            // that bucket changes. Same rule as StatefulDataStream.Sessions.
            long d = timerValues.getCurrentProcessingTimeInMs() + timeoutMs;
            long t = granMs > 0 ? Math.floorDiv(d + granMs - 1, granMs) * granMs : d;
            if (!timer.exists() || timer.get() != t) {
                if (timer.exists()) {
                    getHandle().deleteTimer(timer.get());
                }
                getHandle().registerTimer(t);
                timer.update(t);
            }
            return CollectionConverters.asScala(out.iterator());
        }

        @Override
        public scala.collection.Iterator<Tuple2<String, String>> handleExpiredTimer(
                Long key, TimerValues timerValues, ExpiredTimerInfo expiredTimerInfo) {
            List<Tuple2<String, String>> out = new ArrayList<>(1);
            if (start.exists()) {
                double l = last.get();
                out.add(emit(key, start.get(), l, count.get(), "timeout", l + timeoutMs / 1000.0));
            }
            start.clear();
            last.clear();
            count.clear();
            timer.clear();
            return CollectionConverters.asScala(out.iterator());
        }

        /** anchorTs = the event-time instant the emission is "due" at (latency anchor). */
        private static Tuple2<String, String> emit(long user, double s, double e, long n,
                                                   String reason, double anchorTs) {
            String json = "{\"user_id\":" + user
                    + ",\"session_start\":" + s
                    + ",\"session_end\":" + e
                    + ",\"events\":" + n
                    + ",\"reason\":\"" + reason + "\""
                    + ",\"ts\":" + anchorTs
                    + ",\"out_ts\":" + (System.currentTimeMillis() / 1000.0) + "}";
            return new Tuple2<>(String.valueOf(user), json);
        }
    }
}
