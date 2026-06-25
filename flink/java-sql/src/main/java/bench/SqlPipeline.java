package bench;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Flink SQL / Table API counterpart to the Spark RTM job — the variant that makes the JSON
 * path apples-to-apples. Where the DataStream job parses with Jackson by hand, this job uses
 * the schema-based 'json' format and declarative SQL, exactly the way Spark uses Catalyst's
 * from_json/to_json. Output is keyed by user_id, matching Spark. Everything runs on the JVM
 * (no Python worker, unlike PyFlink), so this isolates the engine from both the parser
 * implementation and the language runtime.
 *
 * Args (positional):
 *   0 bootstrap    e.g. kafka:9092
 *   1 inputTopic   e.g. input-events
 *   2 outputTopic  e.g. output-events
 *   3 parallelism  e.g. 12  (match input partitions)
 */
public class SqlPipeline {

    /**
     * Per-row wall-clock epoch seconds (sub-ms resolution). The JVM analogue of Spark's
     * reflect('java.lang.System','currentTimeMillis'); non-deterministic so the planner
     * evaluates it per row at actual write time rather than folding it to a constant.
     */
    public static final class NowEpoch extends ScalarFunction {
        public double eval() {
            return System.currentTimeMillis() / 1000.0;
        }

        @Override
        public boolean isDeterministic() {
            return false;
        }
    }

    public static void main(String[] args) throws Exception {
        final String bootstrap = args[0];
        final String inputTopic = args[1];
        final String outputTopic = args[2];
        final int parallelism = args.length > 3 ? Integer.parseInt(args[3]) : 12;
        // Arg 4 = checkpoint interval in ms (0/absent = off). Matches the DataStream job and
        // the Spark RTM trigger cadence so the durable-checkpointing comparison is like-for-like.
        final int checkpointMs = args.length > 4 ? Integer.parseInt(args[4]) : 0;

        TableEnvironment env = TableEnvironment.create(
                EnvironmentSettings.inStreamingMode());
        env.getConfig().set("parallelism.default", String.valueOf(parallelism));
        // Per-record flush — match the DataStream job's setBufferTimeout(0).
        env.getConfig().set("execution.buffer-timeout", "0 ms");
        if (checkpointMs > 0) {
            env.getConfig().set("execution.checkpointing.interval", checkpointMs + " ms");
            env.getConfig().set("execution.checkpointing.mode", "AT_LEAST_ONCE");
        }

        // Source: schema-based JSON parse (the Catalyst from_json analogue). value-only.
        env.executeSql(String.format(
                "CREATE TABLE source_events ("
                        + "  event_id BIGINT,"
                        + "  user_id BIGINT,"
                        + "  event_type STRING,"
                        + "  country STRING,"
                        + "  amount DOUBLE,"
                        + "  ts DOUBLE"
                        + ") WITH ("
                        + "  'connector' = 'kafka',"
                        + "  'topic' = '%s',"
                        + "  'properties.bootstrap.servers' = '%s',"
                        + "  'properties.group.id' = 'flink-sql-stateless',"
                        + "  'scan.startup.mode' = 'latest-offset',"
                        + "  'format' = 'json',"
                        + "  'json.ignore-parse-errors' = 'true'"
                        + ")", inputTopic, bootstrap));

        // Sink: schema-based JSON serialize (the to_json analogue), keyed by user_id.
        env.executeSql(String.format(
                "CREATE TABLE sink_events ("
                        + "  event_id BIGINT,"
                        + "  user_id BIGINT,"
                        + "  event_type STRING,"
                        + "  country STRING,"
                        + "  amount DOUBLE,"
                        + "  amount_with_tax DOUBLE,"
                        + "  ts DOUBLE,"
                        + "  out_ts DOUBLE"
                        + ") WITH ("
                        + "  'connector' = 'kafka',"
                        + "  'topic' = '%s',"
                        + "  'properties.bootstrap.servers' = '%s',"
                        + "  'key.format' = 'json',"
                        + "  'key.fields' = 'user_id',"
                        + "  'value.format' = 'json'"
                        + ")", outputTopic, bootstrap));

        env.createTemporarySystemFunction("now_epoch", NowEpoch.class);

        // Identical stateless transform, expressed declaratively in SQL.
        env.executeSql(
                "INSERT INTO sink_events "
                        + "SELECT "
                        + "  event_id, "
                        + "  user_id, "
                        + "  event_type, "
                        + "  UPPER(country) AS country, "
                        + "  amount, "
                        + "  ROUND(amount * 1.21, 2) AS amount_with_tax, "
                        + "  ts, "
                        + "  now_epoch() AS out_ts "
                        + "FROM source_events "
                        + "WHERE event_type IN ('purchase', 'add_to_cart') AND amount > 0")
                .await();
    }
}
