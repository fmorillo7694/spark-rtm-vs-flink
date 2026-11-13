package bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Join workloads for the Flink DataStream job (session C). Output strings use the
 * "user_id\tjson" convention of StatefulDataStream (KeyedJsonSerializer keys by user_id).
 *
 * s4 RefJoin: stream-static enrichment. The reference topic is read to its end ONCE per
 *   TaskManager JVM, in open(), before the subtask processes its first event, and held as a
 *   plain in-memory map shared by the TM's slots: the analogue of a Spark broadcast variable
 *   (one copy per executor, built before the tasks run, never refreshed, not in state).
 *   Chosen over the alternatives because they are not "a static table in memory":
 *     - broadcast state (BroadcastProcessFunction, Databricks' StreamTableJoinJob): the table
 *       arrives as a second stream, so events can overtake it (unmatched or buffered rows at
 *       start, a race the correctness replay would expose), and the full map is written into
 *       every checkpoint by every subtask, a cost Spark's broadcast does not pay;
 *     - SQL lookup join: a per-key request to an external system behind a cache;
 *     - SQL temporal (versioned) join: event-time versioned state, buffers events until the
 *       watermark, which adds latency the RTM join does not have.
 *
 * s5 intervalJoin: orders keyBy(order_id) intervalJoin payments keyBy(order_id),
 *   between(0, bound) on event time: payment.ts in [order.ts, order.ts + bound], inclusive
 *   bounds (Flink's default), bounded-out-of-orderness watermarks on both sources. A pair is
 *   emitted as soon as its second side arrives; the latency anchor ts is the payment ts.
 */
public final class Joins {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Joins() {}

    // ------------------------------------------------------------------ s4

    /** Reference table, loaded once per JVM per (bootstrap, topic). */
    static final class RefTable {
        private static final Map<String, Map<Long, String>> TABLES = new HashMap<>();

        static synchronized Map<Long, String> get(String bootstrap, String topic) {
            return TABLES.computeIfAbsent(bootstrap + "/" + topic, k -> load(bootstrap, topic));
        }

        /** user_id -> the JSON fragment ,"tier":"..","region":".." (interned: ~32 distinct). */
        private static Map<Long, String> load(String bootstrap, String topic) {
            long t0 = System.currentTimeMillis();
            Properties p = new Properties();
            p.put("bootstrap.servers", bootstrap);
            p.put("key.deserializer", ByteArrayDeserializer.class.getName());
            p.put("value.deserializer", ByteArrayDeserializer.class.getName());
            p.put("enable.auto.commit", "false");
            p.put("max.poll.records", "10000");
            Map<Long, String> m = new HashMap<>(1 << 21);
            Map<String, String> intern = new HashMap<>();
            try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p)) {
                List<TopicPartition> parts = new ArrayList<>();
                for (PartitionInfo pi : c.partitionsFor(topic, Duration.ofSeconds(30))) {
                    parts.add(new TopicPartition(topic, pi.partition()));
                }
                c.assign(parts);
                c.seekToBeginning(parts);
                Map<TopicPartition, Long> end = c.endOffsets(parts);
                while (parts.stream().anyMatch(tp -> c.position(tp) < end.get(tp))) {
                    for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(500))) {
                        if (r.value() == null) {
                            continue;
                        }
                        JsonNode n = MAPPER.readTree(r.value());
                        if (!n.hasNonNull("user_id")) {
                            continue;
                        }
                        String frag = ",\"tier\":" + MAPPER.writeValueAsString(n.path("tier").asText())
                                + ",\"region\":" + MAPPER.writeValueAsString(n.path("region").asText());
                        m.put(n.path("user_id").asLong(), intern.computeIfAbsent(frag, f -> f));
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("loading reference topic " + topic, e);
            }
            System.out.println("[Joins] s4 reference rows=" + m.size() + " topic=" + topic
                    + " loadMs=" + (System.currentTimeMillis() - t0));
            return m;
        }
    }

    /** Inner join on user_id against the in-memory reference table; no state, no shuffle. */
    public static final class RefJoin extends RichFlatMapFunction<StatefulDataStream.Event, String> {
        private final String bootstrap;
        private final String topic;
        private transient Map<Long, String> ref;

        RefJoin(String bootstrap, String topic) {
            this.bootstrap = bootstrap;
            this.topic = topic;
        }

        @Override
        public void open(OpenContext ctx) {
            ref = RefTable.get(bootstrap, topic);
        }

        @Override
        public void flatMap(StatefulDataStream.Event e, Collector<String> out) throws Exception {
            String frag = ref.get(e.userId);
            if (frag == null) {
                return;
            }
            out.collect(e.userId + "\t{\"event_id\":" + e.eventId
                    + ",\"user_id\":" + e.userId
                    + ",\"event_type\":" + MAPPER.writeValueAsString(e.eventType)
                    + ",\"country\":" + MAPPER.writeValueAsString(e.country)
                    + ",\"amount\":" + e.amount
                    + ",\"ts\":" + e.ts
                    + frag
                    + ",\"out_ts\":" + StatefulDataStream.now() + "}");
        }
    }

    // ------------------------------------------------------------------ s5

    /** Order or payment (POJO); kind-specific fields left 0 on the other kind. */
    public static final class Rec {
        public long orderId;
        public long userId;
        public double amount;
        public double ts;

        public Rec() {}

        long tsMs() {
            return (long) (ts * 1000.0);
        }
    }

    public static final class RecDeserializer extends AbstractDeserializationSchema<Rec> {
        @Override
        public Rec deserialize(byte[] message) {
            try {
                JsonNode n = MAPPER.readTree(message);
                if (!n.hasNonNull("order_id") || !n.hasNonNull("ts")) {
                    return null;
                }
                Rec r = new Rec();
                r.orderId = n.path("order_id").asLong();
                r.userId = n.path("user_id").asLong();
                r.amount = n.path("amount").asDouble(0.0);
                r.ts = n.path("ts").asDouble();
                return r;
            } catch (Exception ex) {
                return null;
            }
        }

        @Override
        public TypeInformation<Rec> getProducedType() {
            return TypeInformation.of(Rec.class);
        }
    }

    static DataStream<Rec> source(StreamExecutionEnvironment env, String bootstrap, String topic,
                                  String startingOffsets, int watermarkSec, String name) {
        KafkaSource<Rec> src = KafkaSource.<Rec>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId("flink-stateful-s5-" + name)
                .setStartingOffsets(startingOffsets.equals("earliest")
                        ? OffsetsInitializer.earliest() : OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new RecDeserializer())
                .build();
        WatermarkStrategy<Rec> wms = WatermarkStrategy
                .<Rec>forBoundedOutOfOrderness(Duration.ofSeconds(watermarkSec))
                .withTimestampAssigner((r, t) -> r.tsMs());
        return env.fromSource(src, wms, "kafka-" + name);
    }

    static DataStream<String> intervalJoin(StreamExecutionEnvironment env, String bootstrap,
                                           String ordersTopic, String paymentsTopic,
                                           String startingOffsets, int watermarkSec, int boundSec) {
        DataStream<Rec> orders = source(env, bootstrap, ordersTopic, startingOffsets, watermarkSec, "orders");
        DataStream<Rec> payments = source(env, bootstrap, paymentsTopic, startingOffsets, watermarkSec, "payments");
        return orders.keyBy(r -> r.orderId, Types.LONG)
                .intervalJoin(payments.keyBy(r -> r.orderId, Types.LONG))
                .between(Duration.ZERO, Duration.ofSeconds(boundSec))
                .process(new ProcessJoinFunction<Rec, Rec, String>() {
                    @Override
                    public void processElement(Rec o, Rec p, Context ctx, Collector<String> out) {
                        out.collect(o.userId + "\t{\"order_id\":" + o.orderId
                                + ",\"user_id\":" + o.userId
                                + ",\"amount\":" + o.amount
                                + ",\"order_ts\":" + o.ts
                                + ",\"pay_ts\":" + p.ts
                                + ",\"ts\":" + p.ts
                                + ",\"out_ts\":" + StatefulDataStream.now() + "}");
                    }
                }, Types.STRING)
                .name("s5-interval-join");
    }
}
