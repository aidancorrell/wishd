"""What a run was *doing*, and what it was doing it *on*.

The run list shows two different facts that are easy to conflate:

  **Tool** — who emitted the event. Airflow, dbt, the Spark listener. This is
  `integration`, and we already store it.

  **Infrastructure** — where the work physically happened. A dbt model run says
  `dbt://analytics` no matter whether the SQL executed on EMR, Snowflake or a
  Postgres container. That distinction is the whole point of this project's
  thesis: the same dbt project on different compute has completely different
  cost and failure modes, and a run list that only says "DBT" hides it.

Infrastructure is derived from the OpenLineage *namespace*, which by convention
carries the connection identity — `spark://emr-j-2ABC`, `postgres://host:5432`,
`snowflake://account`. That convention is not guaranteed, so every classifier
here degrades to a plain, honest label rather than guessing: an unrecognised
namespace shows its scheme, not a made-up platform.

Deliberately dependency-free and pure. It is called once per row on a list page,
and it must never be the reason a page 500s.
"""

from __future__ import annotations

import re
from typing import Any, NamedTuple

# EMR cluster ids are `j-` followed by alphanumerics. Finding one in a Spark
# namespace is what separates "Spark on EMR" from "Spark somewhere else", and it
# is the only reliable signal without an AWS call.
#
# Case-insensitive deliberately: namespaces arrive as producers wrote them, and
# `spark://emr-j-2ABC` is as common as an uppercased one. The captured id is
# normalised to upper on the way out so the badge is stable regardless.
EMR_CLUSTER = re.compile(r"\b(j-[a-z0-9]{4,})\b", re.IGNORECASE)


class Platform(NamedTuple):
    key: str  # icon lookup, and a CSS class
    label: str  # what a human reads
    detail: str  # the specific instance, when the namespace names one


UNKNOWN = Platform("unknown", "Unknown", "")

# scheme -> (key, label). Only schemes we can name honestly.
SCHEMES: dict[str, tuple[str, str]] = {
    "postgres": ("postgres", "Postgres"),
    "postgresql": ("postgres", "Postgres"),
    "snowflake": ("snowflake", "Snowflake"),
    "bigquery": ("bigquery", "BigQuery"),
    "redshift": ("redshift", "Redshift"),
    "databricks": ("databricks", "Databricks"),
    "mysql": ("mysql", "MySQL"),
    "hive": ("hive", "Hive"),
    "s3": ("s3", "S3"),
    "s3a": ("s3", "S3"),
    "gs": ("gcs", "GCS"),
    "abfss": ("adls", "ADLS"),
    "file": ("file", "Local disk"),
    "dbt": ("dbt", "dbt"),
    # Iceberg catalogs. `iceberg://` is not something a producer emits -- Spark
    # names Iceberg tables by path -- but it is the natural namespace for a
    # catalog-polled source, so it is recognised for symmetry with what the
    # `sources:` block declares.
    "iceberg": ("iceberg", "Iceberg"),
    "glue": ("glue", "Glue"),
}


def _split(namespace: str) -> tuple[str, str]:
    """`spark://emr-j-2ABC` -> `("spark", "emr-j-2ABC")`. Tolerates no scheme.

    Splits on `:` and then strips slashes rather than requiring `://`, because
    real Spark emits **one** slash for local paths — `file:/warehouse/fct_orders`
    is verbatim from the 1.52.0 capture. Requiring two classified every local
    dataset as scheme-less and therefore unknown.
    """
    if ":" not in namespace:
        return "", namespace
    scheme, _, rest = namespace.partition(":")
    if not scheme or "/" in scheme:
        # A colon that is not a scheme separator -- `host:5432` with no scheme.
        return "", namespace
    return scheme.lower(), rest.lstrip("/")


def infrastructure(namespace: str | None, integration: str | None = None) -> Platform:
    """Where the work ran, from the job namespace.

    `integration` is only a tie-breaker: Airflow namespaces are free-form
    (`prod-airflow`, `analytics`) and carry no scheme, so nothing but the
    integration identifies them.
    """
    namespace = (namespace or "").strip()
    integration = (integration or "").strip().upper()

    if not namespace:
        return Platform("airflow", "Airflow", "") if integration == "AIRFLOW" else UNKNOWN

    scheme, rest = _split(namespace)

    if scheme == "spark":
        # Spark is a tool that runs *on* something. The namespace names the
        # cluster or the endpoint, which is the part worth showing.
        if match := EMR_CLUSTER.search(rest):
            return Platform("emr", "EMR", match.group(1).upper())
        host = rest.split("/")[0]
        if "databricks" in host.lower():
            return Platform("databricks", "Databricks", host)
        return Platform("spark", "Spark", host)

    if scheme in SCHEMES:
        key, label = SCHEMES[scheme]
        host = rest.split("/")[0]
        # dbt's namespace is `dbt://<project>`; the project is not infrastructure,
        # so it is carried as detail rather than pretended to be a host.
        return Platform(key, label, host)

    if not scheme:
        # No scheme at all. Airflow is the only producer that does this in
        # practice, and the namespace is the deployment name.
        if integration == "AIRFLOW":
            return Platform("airflow", "Airflow", namespace)
        return Platform("unknown", namespace, "")

    # A scheme we do not recognise. Show it verbatim -- an honest unknown beats
    # a confident wrong guess, and it tells whoever sees it what to add here.
    return Platform("unknown", scheme, rest.split("/")[0])


def tool(integration: str | None) -> Platform:
    """Who emitted the event, as opposed to where it ran."""
    key = (integration or "").strip().upper()
    return {
        "AIRFLOW": Platform("airflow", "Airflow", ""),
        "DBT": Platform("dbt", "dbt", ""),
        "SPARK": Platform("spark", "Spark", ""),
    }.get(key, UNKNOWN)


# What a run is actually doing, from the OpenLineage jobType facet. These are the
# values real producers send, verified in the captures -- not invented.
ACTIVITY = {
    "DAG": "orchestration",
    "TASK": "task",
    "JOB": "dbt invocation",
    "MODEL": "builds a table",
    "SQL": "SQL statement",
    "APPLICATION": "Spark application",
    "SQL_JOB": "Spark SQL",
    "QUERY": "query",
}


def activity(job_type: str | None, job_name: str | None = None) -> str:
    """A short phrase for what this run does. Empty when we cannot say."""
    key = (job_type or "").strip().upper()
    if key in ACTIVITY:
        return ACTIVITY[key]
    # dbt's per-statement runs are named `<model>.sql.N` and carry no job type in
    # some versions; the name is unambiguous, so use it rather than showing blank.
    if job_name and re.search(r"\.sql\.\d+$", job_name):
        return ACTIVITY["SQL"]
    return ""


def lane(state: str | None, started_at: Any) -> str:
    """Which of the three columns a run belongs in.

    **`queued` is narrower than it sounds, and deliberately so.** OpenLineage has
    no "about to run" event -- a producer speaks when work *starts*. So the only
    runs we can honestly call queued are ones another producer has already named
    as a parent, or which reported a nominal time without a start. Work that
    Airflow's scheduler knows about but has not launched is invisible to us; that
    needs the scheduler's own telemetry, which is D2.
    """
    state = (state or "UNKNOWN").upper()
    if state in {"COMPLETED", "FAILED", "ABORTED"}:
        return "finished"
    if state == "RUNNING" and started_at:
        return "running"
    return "queued"
