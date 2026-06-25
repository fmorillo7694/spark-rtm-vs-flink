# Spark 4.1.2 image for EKS: RTM Scala jar + Kafka connector jars baked in so the
# SparkApplication needs no --packages download at submit time. Build context = repo root.
FROM apache/spark:4.1.2-scala2.13-java17-ubuntu

USER root
ARG SCALA_BIN=2.13
ARG SPARK_VER=4.1.2

# Python for the measured PySpark RTM job (executors run no Python — out_ts via reflect()).
RUN apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq python3 \
 && ln -sf /usr/bin/python3 /usr/local/bin/python3 && rm -rf /var/lib/apt/lists/*

# The compiled RTM pipeline jars (Scala + Java) and the measured PySpark job.
COPY spark/scala-rtm/target/scala-${SCALA_BIN}/scala-rtm_${SCALA_BIN}-0.1.0.jar /opt/spark/jars/scala-rtm.jar
COPY spark/java-rtm/target/java-rtm.jar /opt/spark/jars/java-rtm.jar
COPY spark/pyspark-bridge/rtm_pyspark_measured.py /opt/spark/work-dir/rtm_pyspark_measured.py

# Kafka structured-streaming connector + its transitive deps (pinned to 4.1.2 / kafka 3.x),
# plus hadoop-aws + AWS SDK bundle (matching Hadoop 3.4.2) for S3A durable checkpoints.
RUN cd /opt/spark/jars && \
    for url in \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_BIN}/${SPARK_VER}/spark-sql-kafka-0-10_${SCALA_BIN}-${SPARK_VER}.jar" \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BIN}/${SPARK_VER}/spark-token-provider-kafka-0-10_${SCALA_BIN}-${SPARK_VER}.jar" \
      "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.0/kafka-clients-3.9.0.jar" \
      "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar" \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar" \
      "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar" \
    ; do curl -fsSL -O "$url"; done

USER spark
