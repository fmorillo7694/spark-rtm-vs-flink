package bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Stateful Kafka -> Kafka workloads, the Flink DataStream counterpart of
 * spark/java-rtm/.../StatefulPipelineJava.java. Same input, same output fields, output keyed
 * by user_id (the "user_id\tjson" convention of StatelessPipeline + its KeyedJsonSerializer).
 *
 *   s1-window  10 s keyed TumblingEventTimeWindows, AggregateFunction (count, sum, max ts):
 *              ONE final result per (user, window), emitted when the watermark passes
 *              window_end. Flink's native windowing semantics.
 *   s1-update  same aggregate as a KeyedProcessFunction that emits the running window result
 *              on EVERY input row (the semantics of Spark's update output mode, which RTM
 *              requires); window state is dropped by an event-time timer at window_end.
 *   s2         dedup on event_id: keyed ValueState "seen" with a processing-time TTL equal to
 *              the watermark bound; first copy is emitted, later copies dropped.
 *   s2-et      dedup on event_id with event-time cleanup (watermark), Spark's dropDuplicates
 *              semantics; see DedupEventTime.
 *   s3         per-user session state machine, processing-time inactivity timer; identical
 *              rules to the Spark transformWithState processor.
 *   s3-lt      s3 with a light timer: deadline rounded up to 1 s, re-armed only when that
 *              bucket changes (same in the Spark processor).
 *
 * State backend RocksDB, incremental checkpoints, EXACTLY_ONCE checkpointing mode (aligned),
 * set here so every deployment (local session cluster, EKS application mode) runs the same
 * settings; see STATEFUL_CONFIG.md.
 *
 * Args (positional):
 *   0 workload       s1-window | s1-update | s2 | s2-et | s3 | s3-lt
 *   1 bootstrap
 *   2 inputTopic
 *   3 outputTopic
 *   4 parallelism
 *   5 checkpointMs   (0 = off)
 *   6 checkpointDir  e.g. file:///tmp/flink-checkpoints or s3://bucket/path
 *   7 delivery       AT_LEAST_ONCE | EXACTLY_ONCE (transactional Kafka sink)
 *   8 startingOffsets latest | earliest
 *   9 watermarkSec   default 5
 *  10 windowSec      default 10
 *  11 sessionGapMs   default 5000
 *  12 sessionTimeoutMs default = sessionGapMs (processing-time inactivity timer)
 *  13 bufferTimeoutMs  default 0 (per-record flush, latency-tuned); 100 = Flink's default
 *                      execution.buffer-timeout, for the "both engines at defaults" arm
 *  14 secondTopic      s4: compacted reference topic; s5: payments topic (inputTopic = orders)
 *  15 joinBoundSec     s5 time bound, default 10
 *
 * Join workloads (session C, see Joins):
 *   s4  stream-static enrichment: each TaskManager loads the whole reference topic into
 *       memory once, before processing (Joins.RefJoin), the analogue of Spark's broadcast.
 *   s5  orders/payments interval join: keyBy(order_id), payment.ts in
 *       [order.ts, order.ts + joinBoundSec], event time.
 */
public class StatefulDataStream {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Input event; public fields + no-arg ctor so Flink treats it as a POJO. */
    public static final class Event {
        public long eventId;
        public long userId;
        public String eventType;
        public String country;
        public double amount;
        public double ts;

        public Event() {}

        long tsMs() {
            return (long) (ts * 1000.0);
        }
    }

    /** Running window aggregate (POJO). */
    public static final class Acc {
        public long cnt;
        public double sum;
        public double maxTs;

        public Acc() {}
    }

    /** JSON -> Event; malformed or key-less records are skipped (null). */
    public static final class EventDeserializer extends AbstractDeserializationSchema<Event> {
        @Override
        public Event deserialize(byte[] message) {
            try {
                JsonNode n = MAPPER.readTree(message);
                if (!n.hasNonNull("user_id") || !n.hasNonNull("ts")) {
                    return null;
                }
                Event e = new Event();
                e.eventId = n.path("event_id").asLong();
                e.userId = n.path("user_id").asLong();
                e.eventType = n.path("event_type").asText("");
                e.country = n.path("country").asText("");
                e.amount = n.path("amount").asDouble(0.0);
                e.ts = n.path("ts").asDouble();
                return e;
            } catch (Exception ex) {
                return null;
            }
        }

        @Override
        public TypeInformation<Event> getProducedType() {
            return TypeInformation.of(Event.class);
        }
    }

    public static void main(String[] args) throws Exception {
        final String workload = args[0];
        final String bootstrap = args[1];
        final String inputTopic = args[2];
        final String outputTopic = args[3];
        final int parallelism = Integer.parseInt(args[4]);
        final long checkpointMs = args.length > 5 ? Long.parseLong(args[5]) : 60000L;
        final String checkpointDir = args.length > 6 ? args[6] : "file:///tmp/flink-checkpoints";
        final DeliveryGuarantee delivery =
                DeliveryGuarantee.valueOf(args.length > 7 ? args[7] : "AT_LEAST_ONCE");
        final String startingOffsets = args.length > 8 ? args[8] : "latest";
        final int watermarkSec = args.length > 9 ? Integer.parseInt(args[9]) : 5;
        final int windowSec = args.length > 10 ? Integer.parseInt(args[10]) : 10;
        final long gapMs = args.length > 11 ? Long.parseLong(args[11]) : 5000L;
        final long timeoutMs = args.length > 12 ? Long.parseLong(args[12]) : gapMs;
        final long bufferTimeoutMs = args.length > 13 ? Long.parseLong(args[13]) : 0L;
        final String secondTopic = args.length > 14 ? args[14] : "";
        final int joinBoundSec = args.length > 15 && !args[15].isEmpty() ? Integer.parseInt(args[15]) : 10;

        Map<String, String> conf = new HashMap<>();
        conf.put("state.backend.type", "rocksdb");
        conf.put("execution.checkpointing.incremental", "true");
        conf.put("execution.checkpointing.dir", checkpointDir);
        conf.put("state.backend.rocksdb.timer-service.factory", "ROCKSDB");
        conf.put("state.backend.rocksdb.memory.managed", "true");
        if (checkpointMs > 0) {
            conf.put("execution.checkpointing.interval", checkpointMs + " ms");
            conf.put("execution.checkpointing.mode", "EXACTLY_ONCE");
            conf.put("execution.checkpointing.unaligned.enabled", "false");
            conf.put("execution.checkpointing.min-pause", "0 ms");
            conf.put("execution.checkpointing.max-concurrent-checkpoints", "1");
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(Configuration.fromMap(conf));
        env.setParallelism(parallelism);
        // 0 = per-record flush (same as the stateless job); 100 = Flink's default.
        env.setBufferTimeout(bufferTimeoutMs);
        System.out.println("[StatefulDataStream] workload=" + workload + " delivery=" + delivery
                + " bufferTimeoutMs=" + bufferTimeoutMs + " checkpointMs=" + checkpointMs);

        DataStream<String> out;
        if (workload.equals("s5")) {
            out = Joins.intervalJoin(env, bootstrap, inputTopic, secondTopic, startingOffsets,
                    watermarkSec, joinBoundSec);
        } else {
        KafkaSource<Event> source = KafkaSource.<Event>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(inputTopic)
                .setGroupId("flink-stateful-" + workload)
                .setStartingOffsets(startingOffsets.equals("earliest")
                        ? OffsetsInitializer.earliest() : OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new EventDeserializer())
                .build();

        // Per-partition (per-split) bounded-out-of-orderness watermarks, ts in epoch seconds.
        WatermarkStrategy<Event> wms = WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(watermarkSec))
                .withTimestampAssigner((e, t) -> e.tsMs());
        DataStream<Event> in = env.fromSource(source, wms, "kafka-source");
        KeyedStream<Event, Long> byUser = in.keyBy(e -> e.userId, Types.LONG);

        switch (workload) {
            case "s4":
                // chained to the source (no keyBy): a map-side join, like Spark's broadcast join
                out = in.flatMap(new Joins.RefJoin(bootstrap, secondTopic)).name("s4-static-ref-join");
                break;
            case "s1-window":
                out = byUser
                        .window(TumblingEventTimeWindows.of(Duration.ofSeconds(windowSec)))
                        .aggregate(new WindowAgg(), new WindowEmit())
                        .name("s1-tumbling-window");
                break;
            case "s1-update":
                out = byUser.process(new UpdateModeWindow(windowSec * 1000L))
                        .name("s1-update-mode-window");
                break;
            case "s2":
                out = in.keyBy(e -> e.eventId, Types.LONG)
                        .process(new Dedup(Duration.ofSeconds(watermarkSec)))
                        .name("s2-dedup");
                break;
            case "s2-et":
                out = in.keyBy(e -> e.eventId, Types.LONG)
                        .process(new DedupEventTime())
                        .name("s2-dedup-event-time");
                break;
            case "s3":
                out = byUser.process(new Sessions(gapMs, timeoutMs, 0)).name("s3-sessions");
                break;
            case "s3-lt":
                out = byUser.process(new Sessions(gapMs, timeoutMs, 1000)).name("s3-sessions-light-timer");
                break;
            default:
                throw new IllegalArgumentException("unknown workload " + workload);
        }
        }

        KafkaSinkBuilder<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(new StatelessPipeline.KeyedJsonSerializer(outputTopic))
                .setDeliveryGuarantee(delivery);
        if (delivery == DeliveryGuarantee.EXACTLY_ONCE) {
            // Must not exceed the broker's transaction.max.timeout.ms (15 min default).
            sink.setTransactionalIdPrefix("bench-" + workload)
                    .setProperty("transaction.timeout.ms", "900000");
        }
        out.sinkTo(sink.build()).name("kafka-sink");

        env.execute("flink-stateful-" + workload + "-" + delivery);
    }

    static double now() {
        return System.currentTimeMillis() / 1000.0;
    }

    static String windowJson(long user, long startMs, long endMs, Acc a) {
        return user + "\t{\"user_id\":" + user
                + ",\"window_start\":" + (startMs / 1000.0)
                + ",\"window_end\":" + (endMs / 1000.0)
                + ",\"cnt\":" + a.cnt
                + ",\"sum_amount\":" + a.sum
                + ",\"ts\":" + a.maxTs
                + ",\"out_ts\":" + now() + "}";
    }

    public static final class WindowAgg implements AggregateFunction<Event, Acc, Acc> {
        @Override
        public Acc createAccumulator() {
            return new Acc();
        }

        @Override
        public Acc add(Event e, Acc a) {
            a.cnt++;
            a.sum += e.amount;
            a.maxTs = Math.max(a.maxTs, e.ts);
            return a;
        }

        @Override
        public Acc getResult(Acc a) {
            return a;
        }

        @Override
        public Acc merge(Acc x, Acc y) {
            x.cnt += y.cnt;
            x.sum += y.sum;
            x.maxTs = Math.max(x.maxTs, y.maxTs);
            return x;
        }
    }

    public static final class WindowEmit extends ProcessWindowFunction<Acc, String, Long, TimeWindow> {
        @Override
        public void process(Long user, Context ctx, Iterable<Acc> accs, Collector<String> out) {
            Acc a = accs.iterator().next();
            out.collect(windowJson(user, ctx.window().getStart(), ctx.window().getEnd(), a));
        }
    }

    /** Spark-update-mode semantics: emit the running (user, window) aggregate per input row. */
    public static final class UpdateModeWindow extends KeyedProcessFunction<Long, Event, String> {
        private final long windowMs;
        private transient MapState<Long, Acc> windows;

        UpdateModeWindow(long windowMs) {
            this.windowMs = windowMs;
        }

        @Override
        public void open(OpenContext ctx) {
            windows = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("windows", Types.LONG, TypeInformation.of(Acc.class)));
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
            long tsMs = e.tsMs();
            long start = tsMs - Math.floorMod(tsMs, windowMs);
            long end = start + windowMs;
            if (end - 1 <= ctx.timerService().currentWatermark()) {
                return;   // late: the window is already closed (same rule as the window operator)
            }
            Acc a = windows.get(start);
            if (a == null) {
                a = new Acc();
                ctx.timerService().registerEventTimeTimer(end - 1);   // state cleanup
            }
            a.cnt++;
            a.sum += e.amount;
            a.maxTs = Math.max(a.maxTs, e.ts);
            windows.put(start, a);
            out.collect(windowJson(ctx.getCurrentKey(), start, end, a));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            windows.remove(timestamp + 1 - windowMs);
        }
    }

    /** Inactivity deadline: now + timeout, rounded up to a granMs boundary when granMs > 0. */
    static long deadline(long nowMs, long timeoutMs, long granMs) {
        long d = nowMs + timeoutMs;
        return granMs > 0 ? Math.floorDiv(d + granMs - 1, granMs) * granMs : d;
    }

    static String dedupJson(Event e) throws Exception {
        return e.userId + "\t{\"event_id\":" + e.eventId
                + ",\"user_id\":" + e.userId
                + ",\"event_type\":" + MAPPER.writeValueAsString(e.eventType)
                + ",\"country\":" + MAPPER.writeValueAsString(e.country)
                + ",\"amount\":" + e.amount
                + ",\"ts\":" + e.ts
                + ",\"out_ts\":" + now() + "}";
    }

    /**
     * s2-et: watermark-based dedup, the semantics of Spark's withWatermark + dropDuplicates.
     * Keyed by event_id; the first copy is emitted and remembered until the watermark passes
     * its event time (event-time timer at ts = event_time + bound in data time, since the
     * watermark lags the max ts by the bound); copies that arrive with ts <= watermark are
     * late and dropped, like Spark drops rows older than the watermark. Retention follows the
     * data, not the wall clock, so a stall cannot expire state early.
     */
    public static final class DedupEventTime extends KeyedProcessFunction<Long, Event, String> {
        private transient ValueState<Boolean> seen;

        @Override
        public void open(OpenContext ctx) {
            seen = getRuntimeContext().getState(new ValueStateDescriptor<>("seen", Types.BOOLEAN));
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
            long tsMs = e.tsMs();
            if (tsMs <= ctx.timerService().currentWatermark() || seen.value() != null) {
                return;
            }
            seen.update(Boolean.TRUE);
            ctx.timerService().registerEventTimeTimer(tsMs);
            out.collect(dedupJson(e));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {
            seen.clear();
        }
    }

    /** s2: processing-time TTL dedup (session A arm, kept for traceability). */
    public static final class Dedup extends KeyedProcessFunction<Long, Event, String> {
        private final Duration ttl;
        private transient ValueState<Boolean> seen;

        Dedup(Duration ttl) {
            this.ttl = ttl;
        }

        @Override
        public void open(OpenContext ctx) {
            ValueStateDescriptor<Boolean> d = new ValueStateDescriptor<>("seen", Types.BOOLEAN);
            d.enableTimeToLive(StateTtlConfig.newBuilder(ttl)
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .cleanupInRocksdbCompactFilter(1000)
                    .build());
            seen = getRuntimeContext().getState(d);
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
            if (seen.value() != null) {
                return;
            }
            seen.update(Boolean.TRUE);
            out.collect(dedupJson(e));
        }
    }

    /** Same rules as StatefulPipelineJava.SessionProcessor (Spark transformWithState). */
    public static final class Sessions extends KeyedProcessFunction<Long, Event, String> {
        private final long gapMs;
        private final long timeoutMs;
        private final long granMs;
        private transient ValueState<Double> start;
        private transient ValueState<Double> last;
        private transient ValueState<Long> count;
        private transient ValueState<Long> timer;

        Sessions(long gapMs, long timeoutMs, long granMs) {
            this.gapMs = gapMs;
            this.timeoutMs = timeoutMs;
            this.granMs = granMs;
        }

        @Override
        public void open(OpenContext ctx) {
            start = getRuntimeContext().getState(new ValueStateDescriptor<>("start", Types.DOUBLE));
            last = getRuntimeContext().getState(new ValueStateDescriptor<>("last", Types.DOUBLE));
            count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Types.LONG));
            timer = getRuntimeContext().getState(new ValueStateDescriptor<>("timer", Types.LONG));
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) throws Exception {
            double ts = e.ts;
            Double l = last.value();
            if (l != null) {
                if ((ts - l) * 1000.0 > gapMs) {
                    out.collect(emit(e.userId, start.value(), l, count.value(), "gap", ts));
                    start.update(ts);
                    last.update(ts);
                    count.update(1L);
                } else {
                    last.update(Math.max(l, ts));
                    count.update(count.value() + 1);
                }
            } else {
                start.update(ts);
                last.update(ts);
                count.update(1L);
            }
            // granMs 0: re-arm on every event (deadline = now + timeout). granMs > 0 (s3-lt):
            // round the deadline up to the next granMs boundary and re-arm only when that
            // bucket changes, so a busy key moves its timer at most once per granMs.
            long nt = deadline(ctx.timerService().currentProcessingTime(), timeoutMs, granMs);
            Long t = timer.value();
            if (t == null || t != nt) {
                if (t != null) {
                    ctx.timerService().deleteProcessingTimeTimer(t);
                }
                ctx.timerService().registerProcessingTimeTimer(nt);
                timer.update(nt);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Double s = start.value();
            if (s != null) {
                double l = last.value();
                out.collect(emit(ctx.getCurrentKey(), s, l, count.value(), "timeout", l + timeoutMs / 1000.0));
            }
            start.clear();
            last.clear();
            count.clear();
            timer.clear();
        }

        private static String emit(long user, double s, double e, long n, String reason, double anchor) {
            return user + "\t{\"user_id\":" + user
                    + ",\"session_start\":" + s
                    + ",\"session_end\":" + e
                    + ",\"events\":" + n
                    + ",\"reason\":\"" + reason + "\""
                    + ",\"ts\":" + anchor
                    + ",\"out_ts\":" + now() + "}";
        }
    }
}
