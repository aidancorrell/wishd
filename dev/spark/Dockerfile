# Spark with the OpenLineage listener jar. Pinned versions: the listener is
# version-coupled to Spark, and a silent mismatch produces no events at all.
FROM apache/spark:3.5.7-python3

USER root
ARG OL_VERSION=1.52.0
RUN curl -fsSL -o /opt/spark/jars/openlineage-spark.jar \
    "https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.12/${OL_VERSION}/openlineage-spark_2.12-${OL_VERSION}.jar" \
 && ls -la /opt/spark/jars/openlineage-spark.jar
USER spark
