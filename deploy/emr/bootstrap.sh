#!/usr/bin/env bash
# wishd EMR bootstrap action.
#
# Add this as a bootstrap action on cluster creation:
#   aws emr create-cluster ... \
#     --bootstrap-actions Name=wishd,Path=s3://YOUR-BUCKET/bootstrap.sh,\
#         Args=["--gateway","http://wishd.internal:8080","--token","$TOKEN"]
#
# What it does, and deliberately does not do:
#
#   * registers the OpenLineage Spark listener (lineage + run identity)
#   * turns on Spark event logging to S3 (metrics, read later by
#     `wishd ingest-eventlog`)
#
# It installs NO wishd code into the Spark driver. Per ADR-004 we read the
# event log Spark already writes rather than shipping our own JVM listener,
# because a collector that can destabilise a driver ends the pilot -- and a
# listener cannot help you at all when the cluster dies, which is exactly when
# you need the data.

set -euo pipefail

GATEWAY=""
TOKEN=""
OL_VERSION="1.52.0"
EVENTLOG_DIR=""
NAMESPACE="spark"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --gateway)      GATEWAY="$2"; shift 2 ;;
    --token)        TOKEN="$2"; shift 2 ;;
    --token-file)   TOKEN="$(cat "$2")"; shift 2 ;;
    --ol-version)   OL_VERSION="$2"; shift 2 ;;
    --eventlog-dir) EVENTLOG_DIR="$2"; shift 2 ;;
    --namespace)    NAMESPACE="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "$GATEWAY" ]]; then
  echo "wishd bootstrap: --gateway is required" >&2
  exit 2
fi

JAR_DIR=/usr/lib/spark/jars
JAR="openlineage-spark_2.12-${OL_VERSION}.jar"
URL="https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.12/${OL_VERSION}/${JAR}"

echo "wishd: fetching ${JAR}"
sudo curl -fsSL --retry 3 -o "${JAR_DIR}/${JAR}" "${URL}"

CONF=/etc/spark/conf/spark-defaults.conf

# Appended, never rewritten: an EMR cluster's spark-defaults.conf carries
# AWS-managed settings that must survive.
{
  echo ""
  echo "# --- wishd (added by bootstrap action) ---"
  # Spark's default redaction pattern does not match OpenLineage's apiKey.
  echo 'spark.redaction.regex (?i)secret|password|token|access[.]?key|api[._-]?key'
  echo "spark.extraListeners io.openlineage.spark.agent.OpenLineageSparkListener"
  echo "spark.openlineage.transport.type http"
  echo "spark.openlineage.transport.url ${GATEWAY}"
  echo "spark.openlineage.transport.endpoint /api/v1/lineage"
  echo "spark.openlineage.namespace ${NAMESPACE}"
  if [[ -n "$TOKEN" ]]; then
    echo "spark.openlineage.transport.auth.type api_key"
    echo "spark.openlineage.transport.auth.apiKey ${TOKEN}"
  fi
  # Failing open is non-negotiable: a gateway outage must never fail a job.
  echo "spark.openlineage.circuitBreaker.type timeout"
  echo "spark.openlineage.circuitBreaker.timeoutInSeconds 10"
  if [[ -n "$EVENTLOG_DIR" ]]; then
    echo "spark.eventLog.enabled true"
    echo "spark.eventLog.dir ${EVENTLOG_DIR}"
    echo "spark.eventLog.compress true"
  fi
} | sudo tee -a "$CONF" > /dev/null

echo "wishd: bootstrap complete (gateway=${GATEWAY}, eventlog=${EVENTLOG_DIR:-off})"
