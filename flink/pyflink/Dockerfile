# PyFlink driver image: Flink 2.2 runtime + Python 3 + apache-flink wheel.
# Used to run the PyFlink Table API job against the same session cluster.
FROM flink:2.2.0-scala_2.12-java17

USER root
# python3-dev for Python headers; a full JDK provides the JNI headers that apache-beam
# (an apache-flink build dep) needs. The base image ships only a JRE at /opt/java/openjdk
# (no include/), so we point that expected path at the installed JDK's headers.
RUN apt-get update -qq \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
      python3 python3-pip python3-dev openjdk-17-jdk-headless build-essential \
 && ln -sf /usr/bin/python3 /usr/local/bin/python \
 && ln -sf /usr/lib/jvm/java-17-openjdk-*/include /opt/java/openjdk/include \
 && rm -rf /var/lib/apt/lists/*

# Match the cluster runtime exactly.
RUN pip3 install --no-cache-dir --break-system-packages apache-flink==2.2.0

# SQL Kafka connector jar (fat) for Flink 2.2, made available to the TableEnvironment.
ARG KAFKA_SQL_CONNECTOR=flink-sql-connector-kafka-5.0.0-2.2.jar
RUN curl -fsSL -o /opt/flink/lib/${KAFKA_SQL_CONNECTOR} \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/${KAFKA_SQL_CONNECTOR}

# Bake the PyFlink job into the image (application mode reads it from a fixed path).
COPY stateless_pipeline.py /opt/pyflink/stateless_pipeline.py

USER flink
