# Flink 2.2.0 image for EKS: the shaded Java DataStream job baked into /opt/flink/usrlib
# so the FlinkDeployment runs it as an application-mode job. Build context = repo root.
FROM flink:2.2.0-scala_2.12-java17

# Application-mode jobs are loaded from /opt/flink/usrlib.
COPY flink/java-datastream/target/flink-stateless.jar /opt/flink/usrlib/flink-stateless.jar
