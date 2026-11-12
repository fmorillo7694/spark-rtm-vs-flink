#!/usr/bin/env python3
"""
MEASURED PySpark RTM job (the Python-Spark corner of the 2x2), native API.

Same stateless pipeline as the Scala/Java jobs, driven from Python, using the native
`DataStreamWriter.trigger(realTime=...)` keyword added in Spark 4.2 (SPARK-54660). This
replaces the py4j bridge in spark/pyspark-bridge/rtm_pyspark_measured.py (kept for history,
no longer used by the local runner) -- the measured logic is otherwise identical.

out_ts correctness: Spark's current_timestamp() is FROZEN at task/epoch launch under RTM
(long-running tasks) -> wrong latency. The Spark image has no Python on executors, so a
Python UDF isn't an option. We stamp per-row via SQL reflect() calling
java.lang.System.currentTimeMillis() -- a non-deterministic JVM call evaluated per row.

Args via env: KAFKA_BOOTSTRAP_INTERNAL, INPUT_TOPIC, OUTPUT_TOPIC, RTM_TRIGGER, SPARK_MASTER_URL
"""
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_json, lit, round as sround, struct, to_json, upper
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_INTERNAL", "kafka:9092")
INPUT = os.getenv("INPUT_TOPIC", "input-events")
OUTPUT = os.getenv("OUTPUT_TOPIC", "output-events")
MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
TRIGGER = os.getenv("RTM_TRIGGER", "5 seconds")

EVENT_SCHEMA = StructType([
    StructField("event_id", LongType()),
    StructField("user_id", LongType()),
    StructField("event_type", StringType()),
    StructField("country", StringType()),
    StructField("amount", DoubleType()),
    StructField("ts", DoubleType()),
])


def main():
    # On K8s (spark-operator) the master is set by spark-submit; only set it for standalone.
    builder = SparkSession.builder.appName("rtm-pyspark-native") \
        .config("spark.sql.adaptive.enabled", "false")
    if os.getenv("SET_MASTER", "0") == "1":
        # 6 cores / 3072m total (heap 2560m + 512m overhead), matching the other local engines.
        builder = builder.master(MASTER) \
            .config("spark.cores.max", "6").config("spark.executor.cores", "6") \
            .config("spark.executor.memory", "2560m") \
            .config("spark.executor.memoryOverhead", "512m")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", INPUT)
           .option("startingOffsets", "latest")
           .load())

    parsed = (raw.selectExpr("CAST(value AS STRING) AS json_str")
              .select(from_json(col("json_str"), EVENT_SCHEMA).alias("e"))
              .select("e.*"))

    transformed = (parsed
                   .filter(col("event_type").isin("purchase", "add_to_cart") & (col("amount") > 0))
                   .withColumn("amount_with_tax", sround(col("amount") * lit(1.21), 2))
                   .withColumn("country", upper(col("country")))
                   # per-row JVM wall-clock (no executor Python, not frozen):
                   .withColumn("out_ts",
                               expr("CAST(reflect('java.lang.System','currentTimeMillis') AS DOUBLE) / 1000.0")))

    out = transformed.select(
        col("user_id").cast(StringType()).alias("key"),
        to_json(struct("event_id", "user_id", "event_type", "country",
                       "amount", "amount_with_tax", "ts", "out_ts")).alias("value"),
    )

    ckpt = os.getenv("CHECKPOINT_LOCATION", f"/tmp/spark-checkpoints/pyspark-{int(time.time())}")
    query = (out.writeStream.format("kafka")
             .option("kafka.bootstrap.servers", BOOTSTRAP)
             .option("topic", OUTPUT)
             .option("checkpointLocation", ckpt)
             .outputMode("update")
             .trigger(realTime=TRIGGER)   # native RTM trigger (Spark >= 4.2)
             .start())

    print(f"[pyspark-native] STARTED RTM query id={query.id} trigger=RealTime({TRIGGER}) ckpt={ckpt}",
          flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
