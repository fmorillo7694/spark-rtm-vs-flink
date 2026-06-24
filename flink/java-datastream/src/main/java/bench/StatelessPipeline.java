package bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Stateless Kafka -> transform -> Kafka pipeline, the Flink counterpart to the Spark RTM
 * job. Identical event contract and transform (see common/event_schema.md), so latency,
 * CPU, memory and cost are measured on the same workload.
 *
 * Latency-tuned: bufferTimeout(0) flushes the network buffer per record (no batching
 * delay), parallelism matches the Kafka partition count, object reuse on.
 *
 * Args (positional):
 *   0 bootstrap    e.g. kafka:9092
 *   1 inputTopic   e.g. input-events
 *   2 outputTopic  e.g. output-events
 *   3 parallelism  e.g. 3   (match input partitions)
 */
public class StatelessPipeline {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        final String bootstrap = args[0];
        final String inputTopic = args[1];
        final String outputTopic = args[2];
        final int parallelism = args.length > 3 ? Integer.parseInt(args[3]) : 3;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        // Flush per record — the key latency knob (default 100ms batches the network buffer).
        env.setBufferTimeout(0);
        env.getConfig().enableObjectReuse();
        // Optional checkpointing for the fault-tolerance test (arg 4 = checkpoint ms).
        // Left OFF for the latency benchmark; ON makes the job recoverable on TM loss.
        if (args.length > 4 && Integer.parseInt(args[4]) > 0) {
            env.enableCheckpointing(Integer.parseInt(args[4]));
        }

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(inputTopic)
                .setGroupId("flink-stateless")
                // Only measure records produced during this run.
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        DataStream<String> in = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "kafka-source");

        DataStream<String> out = in
                .flatMap(new TransformFn())
                .name("stateless-transform");

        out.sinkTo(sink).name("kafka-sink");

        env.execute("flink-stateless-pipeline");
    }

    /**
     * The identical stateless transform: filter purchase/add_to_cart with amount>0,
     * add amount_with_tax = amount*1.21 (2dp), uppercase country, stamp out_ts =
     * wall-clock epoch seconds at processing time, carry ts through. Emits dropped
     * records as nothing (flatMap with 0 or 1 output).
     */
    public static final class TransformFn
            implements org.apache.flink.api.common.functions.FlatMapFunction<String, String> {

        @Override
        public void flatMap(String value, org.apache.flink.util.Collector<String> outc)
                throws Exception {
            JsonNode e = MAPPER.readTree(value);
            String eventType = e.path("event_type").asText("");
            double amount = e.path("amount").asDouble(0.0);
            if (!(eventType.equals("purchase") || eventType.equals("add_to_cart")) || amount <= 0) {
                return;
            }
            ObjectNode o = MAPPER.createObjectNode();
            o.put("event_id", e.path("event_id").asLong());
            o.put("user_id", e.path("user_id").asLong());
            o.put("event_type", eventType);
            o.put("country", e.path("country").asText("").toUpperCase());
            o.put("amount", amount);
            // round(amount*1.21, 2) — match Spark's round() half-up behavior closely enough.
            o.put("amount_with_tax", Math.round(amount * 1.21 * 100.0) / 100.0);
            o.put("ts", e.path("ts").asDouble());
            o.put("out_ts", System.currentTimeMillis() / 1000.0);  // true per-record write time
            outc.collect(MAPPER.writeValueAsString(o));
        }
    }
}
