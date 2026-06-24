package bench;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

/**
 * Java DataFrame counterpart of RtmPipeline.scala — the SAME stateless Kafka -> transform
 * -> Kafka pipeline and the SAME Trigger.RealTime, written in Java instead of Scala so the
 * Spark-vs-Flink comparison can be Java-on-both (removes language as a variable).
 *
 * out_ts is stamped per-row via SQL reflect(System.currentTimeMillis) — NOT
 * current_timestamp(), which is frozen at task launch under RTM (documented gotcha).
 *
 * Args (positional): mode bootstrap inputTopic outputTopic checkpointDir [triggerDuration]
 */
public class RtmPipelineJava {

    static final StructType EVENT_SCHEMA = new StructType()
            .add("event_id", DataTypes.LongType)
            .add("user_id", DataTypes.LongType)
            .add("event_type", DataTypes.StringType)
            .add("country", DataTypes.StringType)
            .add("amount", DataTypes.DoubleType)
            .add("ts", DataTypes.DoubleType);

    public static void main(String[] args) throws Exception {
        String mode = args[0];
        String bootstrap = args[1];
        String inputTopic = args[2];
        String outputTopic = args[3];
        String checkpointDir = args[4];
        String triggerDuration = args.length > 5 ? args[5] : "5 seconds";

        SparkSession spark = SparkSession.builder()
                .appName("rtm-bench-java-" + mode)
                .config("spark.sql.adaptive.enabled", "false")
                .config("spark.sql.shuffle.partitions", "12")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> reader = spark.readStream().format("kafka")
                .option("kafka.bootstrap.servers", bootstrap)
                .option("subscribe", inputTopic)
                .option("startingOffsets", "latest")
                .load();
        // maxOffsetsPerTrigger is incompatible with RTM; only bound batch size in micro-batch.
        Dataset<Row> raw = mode.equals("microbatch")
                ? spark.readStream().format("kafka")
                    .option("kafka.bootstrap.servers", bootstrap)
                    .option("subscribe", inputTopic)
                    .option("startingOffsets", "latest")
                    .option("maxOffsetsPerTrigger", "2000000")
                    .load()
                : reader;

        Dataset<Row> parsed = raw
                .selectExpr("CAST(value AS STRING) AS json_str")
                .select(from_json(col("json_str"), EVENT_SCHEMA).alias("e"))
                .select("e.*");

        Dataset<Row> transformed = parsed
                .filter(col("event_type").isin("purchase", "add_to_cart").and(col("amount").gt(0)))
                .withColumn("amount_with_tax", round(col("amount").multiply(lit(1.21)), 2))
                .withColumn("country", upper(col("country")))
                .withColumn("out_ts", expr(
                        "CAST(reflect('java.lang.System','currentTimeMillis') AS DOUBLE) / 1000.0"));

        Dataset<Row> out = transformed.select(
                col("user_id").cast(DataTypes.StringType).alias("key"),
                to_json(struct("event_id", "user_id", "event_type", "country",
                        "amount", "amount_with_tax", "ts", "out_ts")).alias("value"));

        var baseWriter = out.writeStream().format("kafka")
                .option("kafka.bootstrap.servers", bootstrap)
                .option("topic", outputTopic)
                .option("checkpointLocation", checkpointDir);

        StreamingQuery query;
        if (mode.equals("rtm")) {
            query = baseWriter.outputMode("update").trigger(Trigger.RealTime(triggerDuration)).start();
        } else if (mode.equals("microbatch")) {
            query = baseWriter.outputMode("append").start();
        } else {
            throw new IllegalArgumentException("unknown mode '" + mode + "' (use rtm|microbatch)");
        }

        System.out.println("[RtmPipelineJava] mode=" + mode + " id=" + query.id()
                + " trigger=" + (mode.equals("rtm") ? "RealTime(" + triggerDuration + ")" : "micro-batch"));
        query.awaitTermination();
    }
}
