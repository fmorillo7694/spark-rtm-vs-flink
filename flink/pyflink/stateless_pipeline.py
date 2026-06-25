#!/usr/bin/env python3
"""
PyFlink counterpart of the Java DataStream job — same stateless Kafka -> transform ->
Kafka pipeline, expressed in idiomatic PyFlink (Table API + SQL Kafka connector).

This is the Python-Flink corner of the 2x2 (Scala-Spark / Java-Flink / PySpark / PyFlink).
Like PySpark DataFrame, the Table API AUTHORS in Python but EXECUTES on the JVM — the
transform runs as native Flink operators, not per-record Python. This is the fair,
idiomatic Python path (a python-UDF path would add a Python process per record).

Args (positional):
  0 bootstrap    e.g. kafka:9092
  1 inputTopic   e.g. input-events
  2 outputTopic  e.g. output-events
  3 parallelism  e.g. 3
"""
import sys
import time

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.udf import udf


# out_ts stamp. Flink SQL has no epoch-MILLIS builtin (UNIX_TIMESTAMP is seconds-only),
# which would wreck ms-latency measurement. A Python scalar UDF gives true per-row epoch
# seconds with sub-ms resolution. NOTE: this is the one piece that executes in Python per
# row; the filter/projection run as native JVM operators. Documented in REPORT.md.
@udf(result_type="DOUBLE", deterministic=False)
def now_epoch() -> float:
    return time.time()


def main():
    bootstrap = sys.argv[1]
    input_topic = sys.argv[2]
    output_topic = sys.argv[3]
    parallelism = int(sys.argv[4]) if len(sys.argv) > 4 else 3
    # Arg 5 = checkpoint interval ms (0/absent = off), matched to the other engines' cadence.
    checkpoint_ms = int(sys.argv[5]) if len(sys.argv) > 5 else 0

    env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    cfg = env.get_config().get_configuration()
    cfg.set_string("parallelism.default", str(parallelism))
    # Per-record flush — match the Java job's setBufferTimeout(0).
    cfg.set_string("execution.buffer-timeout", "0 ms")
    if checkpoint_ms > 0:
        cfg.set_string("execution.checkpointing.interval", f"{checkpoint_ms} ms")
        cfg.set_string("execution.checkpointing.mode", "AT_LEAST_ONCE")
    # Python UDF bundling: defaults (bundle.time=1000ms, size=100000) make records WAIT up
    # to ~1s before being shipped to the Python worker — a latency floor unrelated to real
    # Python cost. Shrink the window so measured latency reflects the per-record Python
    # round-trip, not buffering. (This knob is the PyFlink latency gotcha; see REPORT.md.)
    cfg.set_string("python.fn-execution.bundle.time", "1")
    cfg.set_string("python.fn-execution.bundle.size", "1000")

    # Source: JSON events; value-only. `ts` is epoch seconds (double).
    env.execute_sql(f"""
        CREATE TABLE source_events (
            event_id   BIGINT,
            user_id    BIGINT,
            event_type STRING,
            country    STRING,
            amount     DOUBLE,
            ts         DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{input_topic}',
            'properties.bootstrap.servers' = '{bootstrap}',
            'properties.group.id' = 'pyflink-stateless',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # Sink: same output contract, keyed by user_id.
    env.execute_sql(f"""
        CREATE TABLE sink_events (
            event_id        BIGINT,
            user_id         BIGINT,
            event_type      STRING,
            country         STRING,
            amount          DOUBLE,
            amount_with_tax DOUBLE,
            ts              DOUBLE,
            out_ts          DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{output_topic}',
            'properties.bootstrap.servers' = '{bootstrap}',
            'key.format' = 'json',
            'key.fields' = 'user_id',
            'value.format' = 'json'
        )
    """)

    # Register the per-row epoch UDF for use in SQL.
    env.create_temporary_function("now_epoch", now_epoch)

    # Identical stateless transform; out_ts stamped per row via the UDF.
    env.execute_sql("""
        INSERT INTO sink_events
        SELECT
            event_id,
            user_id,
            event_type,
            UPPER(country) AS country,
            amount,
            ROUND(amount * 1.21, 2) AS amount_with_tax,
            ts,
            now_epoch() AS out_ts
        FROM source_events
        WHERE event_type IN ('purchase', 'add_to_cart') AND amount > 0
    """).wait()


if __name__ == "__main__":
    main()
