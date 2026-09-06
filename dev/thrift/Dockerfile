# Spark Thrift Server with the OpenLineage listener.
#
# This exists to settle D6, which is the last unproven claim reachable without a
# cloud account. Every dbt validation so far used dbt-postgres, and ADR-003 says
# the reference deployment is dbt-spark over thrift. Job naming and the parent
# handoff are adapter-independent and carry over; **dataset namespaces are not**,
# and Phase 04's lineage joins on exactly those.
#
# Same pinning discipline as dev/spark: the listener is version-coupled to Spark,
# and a silent mismatch emits no events at all rather than failing loudly.

FROM apache/spark:3.5.7-python3

USER root

ARG OL_VERSION=1.52.0
RUN curl -fsSL -o /opt/spark/jars/openlineage-spark.jar \
    "https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.12/${OL_VERSION}/openlineage-spark_2.12-${OL_VERSION}.jar" \
 && ls -la /opt/spark/jars/openlineage-spark.jar

# Iceberg, so dbt can materialise models in the format the reference deployment
# actually targets -- and so the REST catalog has something real to serve.
#
# Pinned for the same reason the listener is: the Spark runtime jar is compiled
# against a specific Spark minor, and a mismatch fails at query time with a
# NoSuchMethodError rather than at startup.
ARG ICEBERG_VERSION=1.9.1
RUN curl -fsSL -o /opt/spark/jars/iceberg-spark-runtime.jar \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
 && ls -la /opt/spark/jars/iceberg-spark-runtime.jar

# The thrift server writes its Derby metastore and warehouse here. A named
# volume would survive `down`, which is the wrong default for a validation
# stack -- a stale metastore silently changes the namespaces we are trying to
# observe, which is the exact thing under test.
RUN mkdir -p /warehouse /eventlog && chmod -R 777 /warehouse /eventlog

USER spark
WORKDIR /opt/spark

# `start-thriftserver.sh` daemonises and returns, which would make the container
# exit immediately. Run the class in the foreground instead so Docker can
# supervise it and `depends_on` health checks mean something.
ENTRYPOINT ["/opt/spark/bin/spark-submit", \
  "--class", "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2", \
  "--name", "dataspine-thrift", \
  "--master", "local[2]"]
