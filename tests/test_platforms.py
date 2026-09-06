"""Tool vs infrastructure, and the run list's three lanes.

The run list draws two facts that are easy to conflate: *who emitted* an event
(Airflow, dbt, the Spark listener) and *where the work ran* (EMR, Snowflake, a
Postgres container). Only the first is stored directly; the second is derived
from the OpenLineage namespace, which is convention rather than contract. So the
rule these tests hold to is: name it when the namespace says so, degrade to
something honest when it does not, and never invent a platform.

Every namespace here is one a real producer emitted into this project — from the
captures and from the seeded pipelines — rather than a shape invented to make
the classifier look good.
"""

from __future__ import annotations

import pytest

from dataspine import platforms

# --------------------------------------------------------------- what ran where


@pytest.mark.parametrize(
    ("namespace", "key", "label", "detail"),
    [
        # The distinction the whole module exists for: Spark is a tool that runs
        # *on* something, and the namespace is the only thing that says what.
        ("spark://emr-j-1A2B3C4D5E6F", "emr", "EMR", "J-1A2B3C4D5E6F"),
        ("spark://thrift-dev", "spark", "Spark", "thrift-dev"),
        ("spark://local-dev", "spark", "Spark", "local-dev"),
        # Warehouses, by scheme.
        ("postgres://postgres:5432", "postgres", "Postgres", "postgres:5432"),
        ("snowflake://acme-account", "snowflake", "Snowflake", "acme-account"),
        ("bigquery://analytics-prod", "bigquery", "BigQuery", "analytics-prod"),
        ("databricks://dbc-123.cloud", "databricks", "Databricks", "dbc-123.cloud"),
        # Object stores, which is where dbt-spark output actually lands.
        ("s3://acme-lakehouse", "s3", "S3", "acme-lakehouse"),
        # One slash, verbatim from the real Spark 1.52.0 capture. Requiring two
        # classified every local dataset as unknown.
        ("file:/warehouse", "file", "Local disk", "warehouse"),
        ("dbt://analytics", "dbt", "dbt", "analytics"),
    ],
)
def test_infrastructure_from_namespace(namespace, key, label, detail):
    infra = platforms.infrastructure(namespace)
    assert (infra.key, infra.label) == (key, label)
    assert infra.detail == detail


def test_emr_is_detected_whatever_the_case():
    """The bug this test exists for.

    The first implementation uppercased the namespace and then matched a
    lowercase `j-`, so it never matched at all and every EMR cluster in the UI
    was labelled a generic "Spark". It was invisible in review and obvious on
    screen — the badge read `Spark emr-j-1A2B3C4D5E6F`, carrying the cluster id
    it had just failed to recognise.
    """
    for namespace in (
        "spark://emr-j-1a2b3c4d5e6f",
        "spark://emr-j-1A2B3C4D5E6F",
        "spark://EMR-J-1A2B3C4D5E6F",
    ):
        infra = platforms.infrastructure(namespace)
        assert infra.key == "emr", namespace
        # Normalised, so the badge does not flicker between cases across rows.
        assert infra.detail == "J-1A2B3C4D5E6F"


def test_airflow_namespaces_have_no_scheme():
    """Airflow's namespace is a free-form deployment name, so the integration is
    the only thing that identifies it."""
    infra = platforms.infrastructure("prod-airflow", "AIRFLOW")
    assert (infra.key, infra.detail) == ("airflow", "prod-airflow")


def test_an_unknown_scheme_is_shown_rather_than_guessed():
    """An honest unknown beats a confident wrong guess — and it tells whoever
    sees it exactly which scheme to add to SCHEMES."""
    infra = platforms.infrastructure("clickhouse://analytics:9000")
    assert infra.key == "unknown"
    assert infra.label == "clickhouse"


def test_nothing_at_all_never_raises():
    """This runs once per row on a list page. It must never be why a page 500s."""
    for value in (None, "", "   ", "://", "notascheme"):
        assert platforms.infrastructure(value).label


# --------------------------------------------------------------- what it is doing


def test_activity_reads_the_job_type():
    assert platforms.activity("MODEL") == "builds a table"
    assert platforms.activity("APPLICATION") == "Spark application"
    assert platforms.activity("DAG") == "orchestration"


def test_dbt_per_statement_runs_are_recognised_by_name():
    """dbt's `.sql.N` runs do not always carry a job type, and a blank cell in
    the busiest rows of the tree is where the hierarchy stops being readable."""
    assert platforms.activity(None, "model.analytics.fct_orders.sql.3") == "SQL statement"
    assert platforms.activity(None, "model.analytics.fct_orders") == ""


# ----------------------------------------------------------------------- lanes


def test_terminal_states_are_finished():
    for state in ("COMPLETED", "FAILED", "ABORTED"):
        assert platforms.lane(state, "2026-08-16T00:00:00Z") == "finished"


def test_running_needs_an_actual_start():
    assert platforms.lane("RUNNING", "2026-08-16T00:00:00Z") == "running"
    # RUNNING with no start is a placeholder a child named as its parent — real
    # in the data, not yet real on any machine.
    assert platforms.lane("RUNNING", None) == "queued"


def test_unknown_is_queued_not_finished():
    """A run we have heard of but which reports nothing is pending, not done.
    Filing it under finished would quietly mark unstarted work complete."""
    assert platforms.lane("UNKNOWN", None) == "queued"
    assert platforms.lane(None, None) == "queued"
