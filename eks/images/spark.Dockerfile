# Spark 4.3.0 image for EKS: RTM Scala jar + Kafka connector jars baked in so the
# SparkApplication needs no --packages download at submit time. Build context = repo root.
FROM apache/spark:4.3.0-scala2.13-java17-ubuntu

USER root
ARG SCALA_BIN=2.13
ARG SPARK_VER=4.3.0

# Python for the measured PySpark RTM job (executors run no Python — out_ts via reflect()).
RUN apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq python3 \
 && ln -sf /usr/bin/python3 /usr/local/bin/python3 && rm -rf /var/lib/apt/lists/*

# The compiled RTM pipeline jars (Scala + Java) and the measured PySpark job.
COPY spark/scala-rtm/target/scala-${SCALA_BIN}/scala-rtm_${SCALA_BIN}-0.1.0.jar /opt/spark/jars/scala-rtm.jar
COPY spark/java-rtm/target/java-rtm.jar /opt/spark/jars/java-rtm.jar
COPY spark/pyspark-bridge/rtm_pyspark_measured.py /opt/spark/work-dir/rtm_pyspark_measured.py
# Native trigger(realTime=...) variant (Spark >= 4.2); EKS manifests still point at the
# bridge job above until the EKS matrix is re-run.
COPY spark/pyspark/rtm_pyspark_native.py /opt/spark/work-dir/rtm_pyspark_native.py

# Kafka structured-streaming connector + its transitive deps (pinned to 4.3.0 / kafka 3.x),
# plus hadoop-aws + AWS SDK bundle + S3 Analytics Accelerator (Hadoop 3.5.0 S3A needs it)
# for durable checkpoints.
RUN cd /opt/spark/jars && \
    for url in \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_BIN}/${SPARK_VER}/spark-sql-kafka-0-10_${SCALA_BIN}-${SPARK_VER}.jar" \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BIN}/${SPARK_VER}/spark-token-provider-kafka-0-10_${SCALA_BIN}-${SPARK_VER}.jar" \
      "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.2/kafka-clients-3.9.2.jar" \
      "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.13.1/commons-pool2-2.13.1.jar" \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.jar" \
      "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.35.4/bundle-2.35.4.jar" \
      "https://repo1.maven.org/maven2/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/1.3.1/analyticsaccelerator-s3-1.3.1.jar" \
    ; do curl -fsSL -O "$url"; done

USER spark
