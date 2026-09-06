"""Tests against events captured from a real Airflow.

`tests/fixtures/airflow_3.0.2_provider_2.19.0.json` is a verbatim capture of what
`apache-airflow-providers-openlineage` 2.19.0 emitted on Airflow 3.0.2 for a
two-task DAG run, recorded 2026-08-07. Nothing in it was written by hand.

This is the antidote to the circularity in the rest of the suite: every other
test feeds the correlator events produced by `simulate.py`, which is our own
reconstruction of the wire format. If that reconstruction is wrong, those tests
pass and production breaks. These do not have that problem.

When bumping the supported provider version, capture a new fixture rather than
editing this one, and keep both.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dataspine import queries
from dataspine.events import RunEvent
from dataspine.ingest import ingest_run_event

FIXTURE = Path(__file__).parent / "fixtures" / "airflow_3.0.2_provider_2.19.0.json"


@pytest.fixture()
def real_events() -> list[dict]:
    return json.loads(FIXTURE.read_text())


def test_capture_is_what_we_think_it_is(real_events):
    """Guards the fixture itself. If someone regenerates it against a different
    setup, the rest of this file would start asserting the wrong thing."""
    assert len(real_events) == 6
    producers = {e["producer"] for e in real_events}
    assert producers == {
        "https://github.com/apache/airflow/tree/providers-openlineage/2.19.0"
    }


def test_real_events_are_accepted_unmodified(conn, real_events):
    """The gateway must swallow the real thing as-is — including the four
    Airflow-specific facets we never anticipated."""
    for payload in real_events:
        ingest_run_event(conn, RunEvent.model_validate(payload))

    health = queries.ingest_health(conn)
    assert health["unstitched_runs"] == 0
    assert health["placeholder_runs"] == 0


def test_real_run_tree_assembles(conn, real_events):
    for payload in real_events:
        ingest_run_event(conn, RunEvent.model_validate(payload))

    roots = queries.list_runs(conn, roots_only=True, limit=10)
    assert len(roots) == 1
    assert roots[0]["job_name"] == "analytics_daily"

    tree = queries.run_tree(conn, roots[0]["run_id"])
    assert [(n["job_name"], n["level"]) for n in tree] == [
        ("analytics_daily", 0),
        ("analytics_daily.dbt_run_marts", 1),
        ("analytics_daily.publish_marts", 1),
    ]
    assert all(n["state"] == "COMPLETED" for n in tree)


def test_real_job_naming_matches_what_the_simulator_assumes(conn, real_events):
    """The assumption `simulate.py` was built on, pinned to real output:
    a DAG job is `<dag_id>`, a task job is `<dag_id>.<task_id>`.

    If a future provider changes this, the simulator silently drifts from
    reality and this test is the thing that notices."""
    for payload in real_events:
        ingest_run_event(conn, RunEvent.model_validate(payload))

    jobs = {
        (j["name"], j["integration"], j["job_type"])
        for j in conn.execute("select name, integration, job_type from jobs").fetchall()
    }
    assert ("analytics_daily", "AIRFLOW", "DAG") in jobs
    assert ("analytics_daily.dbt_run_marts", "AIRFLOW", "TASK") in jobs


def test_real_parent_facet_carries_root(real_events):
    """Documents what the Airflow provider puts in `root`.

    For a task, `root` == `parent` == the DAG run, which happens to be correct.
    It is not correct in general — openlineage-dbt reports its own parent as
    root (see test_every_real_run_roots_at_the_dag) — which is why the correlator
    treats this facet as a hint for pre-creating ancestors rather than as the
    answer."""
    task_start = next(
        e
        for e in real_events
        if e["job"]["name"].endswith("dbt_run_marts") and e["eventType"] == "START"
    )
    parent = task_start["run"]["facets"]["parent"]
    assert parent["job"]["name"] == "analytics_daily"
    assert parent["root"]["run"]["runId"] == parent["run"]["runId"]


def test_unanticipated_facets_survive_ingest(conn, real_events):
    """The real provider sends `airflow`, `airflowDagRun`, `airflowState` and
    `unknownSourceAttribute` — none of which we model. Storing facets as opaque
    jsonb means they arrive intact and are queryable later, which is the whole
    argument for not flattening them into columns."""
    for payload in real_events:
        ingest_run_event(conn, RunEvent.model_validate(payload))

    stored = conn.execute(
        """
        select r.facets from runs r join jobs j on j.id = r.job_id
        where j.name = 'analytics_daily.dbt_run_marts'
        """
    ).fetchone()["facets"]
    assert {"airflow", "parent", "processing_engine", "unknownSourceAttribute"} <= set(stored)


def test_real_runs_use_uuid7(real_events):
    """The provider emits UUIDv7, as the spec recommends. Worth knowing because
    v7 sorts by creation time — if we ever index on run_id, that is free
    time-ordering we would otherwise pay for."""
    from uuid import UUID

    run_id = UUID(real_events[0]["run"]["runId"])
    assert run_id.version == 7


# ---------------------------------------------------------- real Airflow + dbt

END_TO_END = Path(__file__).parent / "fixtures" / "airflow_dbt_end_to_end.json"


@pytest.fixture()
def real_pipeline() -> list[dict]:
    """Airflow 3.0.2 + provider 2.19.0 + openlineage-dbt 1.52.0, captured
    2026-08-07 from a DAG whose BashOperator ran `dbt-ol run
    --consume-structured-logs`. Two producers, one pipeline."""
    return json.loads(END_TO_END.read_text())


def test_capture_spans_both_producers(real_pipeline):
    producers = {e["producer"] for e in real_pipeline}
    assert any("airflow" in p for p in producers)
    assert any("dbt" in p for p in producers)


def test_the_airflow_to_dbt_handoff_holds(conn, real_pipeline):
    """The link the roadmap called the most fragile in the chain.

    dbt claims the Airflow task as its parent via OPENLINEAGE_PARENT_ID. If that
    breaks, the pipeline silently becomes two unrelated trees and every
    downstream feature — root cause, blast radius, cost attribution — is wrong
    without appearing wrong.
    """
    for payload in real_pipeline:
        ingest_run_event(conn, RunEvent.model_validate(payload))

    dbt_run = conn.execute(
        """
        select r.run_id, r.parent_run_id from runs r join jobs j on j.id = r.job_id
        where j.name = 'dbt-run-analytics' order by r.started_at desc limit 1
        """
    ).fetchone()
    assert dbt_run["parent_run_id"] is not None, "dbt did not claim an Airflow parent"

    parent_job = conn.execute(
        """
        select j.name from runs r join jobs j on j.id = r.job_id where r.run_id = %s
        """,
        (dbt_run["parent_run_id"],),
    ).fetchone()
    assert parent_job["name"] == "analytics_daily.dbt_run_marts"


def test_every_real_run_roots_at_the_dag(conn, real_pipeline):
    """Regression for the root-facet bug found on 2026-08-07.

    openlineage-dbt reports `root` as its own parent (the Airflow task). When we
    trusted that, 19 dbt runs rooted at the task and the DAG plus its sibling
    tasks vanished from `tree`. Every run must resolve to a DAG-level root.
    """
    for payload in real_pipeline:
        ingest_run_event(conn, RunEvent.model_validate(payload))

    mis_rooted = conn.execute(
        """
        select s.job_name, rj.job_type as root_type
        from run_summary s
        join runs rr on rr.run_id = s.root_run_id
        join jobs rj on rj.id = rr.job_id
        where rj.job_type is distinct from 'DAG'
        """
    ).fetchall()
    assert mis_rooted == [], f"runs not rooted at a DAG: {mis_rooted}"


def test_deepest_dbt_run_resolves_to_the_whole_pipeline(conn, real_pipeline):
    """The product promise, on real data: hand it the id of the thing that
    failed, get back the entire execution it belonged to."""
    for payload in real_pipeline:
        ingest_run_event(conn, RunEvent.model_validate(payload))

    deepest = conn.execute(
        """
        select r.run_id from runs r join jobs j on j.id = r.job_id
        where j.name like 'model.analytics.%%.sql.%%'
        order by r.depth desc limit 1
        """
    ).fetchone()
    root = queries.root_of(conn, deepest["run_id"])
    tree = queries.run_tree(conn, root)

    assert tree[0]["job_name"] == "analytics_daily"
    names = {n["job_name"] for n in tree}
    assert "analytics_daily.dbt_run_marts" in names
    assert "dbt-run-analytics" in names
    assert "model.analytics.fct_orders" in names
    # The sibling task that the root bug used to hide.
    assert "analytics_daily.publish_marts" in names


def test_real_dbt_job_naming(conn, real_pipeline):
    """Pins the conventions openlineage-dbt actually uses, which are NOT what
    simulate.py originally guessed:

        invocation   dbt-run-<project>            (jobType JOB)
        model        model.<project>.<model>      (jobType MODEL)
        statement    model.<project>.<model>.sql.N (jobType SQL)

    `.sql.N` only appears in --consume-structured-logs mode; it is one event per
    statement executed within a node.
    """
    for payload in real_pipeline:
        ingest_run_event(conn, RunEvent.model_validate(payload))

    jobs = {
        j["name"]: j["job_type"]
        for j in conn.execute(
            "select name, job_type from jobs where integration = 'DBT'"
        ).fetchall()
    }
    assert jobs.get("dbt-run-analytics") == "JOB"
    assert jobs.get("model.analytics.fct_orders") == "MODEL"
    assert jobs.get("model.analytics.fct_orders.sql.1") == "SQL"


# ------------------------------------------------------------------ real Spark

SPARK_FIXTURE = Path(__file__).parent / "fixtures" / "spark_openlineage_1.52.0.json"


@pytest.fixture()
def real_spark() -> list[dict]:
    """openlineage-spark 1.52.0 on Spark 3.5.7, captured 2026-08-08 from a job
    that reads two tables, joins them and writes a third — the shape a dbt-spark
    model materialisation produces. Its parent facet names a real Airflow task
    run id, so this exercises the full three-producer chain."""
    return json.loads(SPARK_FIXTURE.read_text())


def test_spark_capture_is_real(real_spark):
    assert len(real_spark) == 21
    assert all("integration/spark" in e["producer"] for e in real_spark)


def test_real_spark_job_naming(conn, real_spark):
    """Pins openlineage-spark's conventions, which simulate.py originally got
    wrong:

        application   <appName>                          (jobType APPLICATION)
        sql execution <appName>.<command>.<db>_<table>    (jobType SQL_JOB)

    Note the command sits in the middle, not at the end — the simulator had it
    the other way around.
    """
    for payload in real_spark:
        ingest_run_event(conn, RunEvent.model_validate(payload))

    jobs = {
        j["name"]: j["job_type"]
        for j in conn.execute(
            "select name, job_type from jobs where integration = 'SPARK'"
        ).fetchall()
    }
    assert jobs.get("dbt_spark_analytics") == "APPLICATION"
    sql_jobs = [n for n, t in jobs.items() if t == "SQL_JOB"]
    assert any(
        n.startswith("dbt_spark_analytics.execute_insert_into_hadoop_fs_relation_command.")
        for n in sql_jobs
    ), sql_jobs


def test_spark_also_reports_root_as_its_own_parent(real_spark):
    """The dbt lesson, confirmed a second time.

    openlineage-spark sets `root` to its parent (the Airflow task), not the top
    of the tree. Two independent integrations get this wrong, which is the
    justification for the correlator treating `root` as a hint rather than the
    answer -- and for re-checking every new producer against this.
    """
    start = next(e for e in real_spark if e.get("eventType") == "START")
    parent = start["run"]["facets"]["parent"]
    assert parent["root"]["run"]["runId"] == parent["run"]["runId"], (
        "openlineage-spark started reporting a true root; the hint may now be "
        "trustworthy for this integration"
    )
    assert parent["job"]["name"] == "analytics_daily.dbt_run_marts"


def test_spark_and_dbt_share_the_same_airflow_parent(real_pipeline, real_spark):
    """Guards the fixtures' relationship, not the code.

    The Spark capture was deliberately re-run pointing at the same Airflow task
    run that dbt claimed, so a single tree spans all three producers. Captured
    against different DAG runs, every assertion below would still pass while
    proving something much weaker.
    """
    dbt_start = next(
        e for e in real_pipeline
        if "dbt" in e["producer"] and e["job"]["name"] == "dbt-run-analytics"
    )
    spark_start = next(e for e in real_spark if e.get("eventType") == "START")
    assert (
        dbt_start["run"]["facets"]["parent"]["run"]["runId"]
        == spark_start["run"]["facets"]["parent"]["run"]["runId"]
    )


def test_three_producer_chain_roots_at_the_dag(conn, real_pipeline, real_spark):
    """The whole thesis, on entirely real data: Airflow, dbt and Spark — three
    independent producers that know nothing about each other — assembled into
    one tree rooted at the DAG run."""
    for payload in real_pipeline + real_spark:
        ingest_run_event(conn, RunEvent.model_validate(payload))

    mis_rooted = conn.execute(
        """
        select s.job_name from run_summary s
        join runs rr on rr.run_id = s.root_run_id
        join jobs rj on rj.id = rr.job_id
        where rj.job_type is distinct from 'DAG'
        """
    ).fetchall()
    assert mis_rooted == [], f"runs not rooted at a DAG: {mis_rooted}"

    integrations = {
        r["integration"]
        for r in conn.execute(
            "select distinct integration from run_summary where integration is not null"
        ).fetchall()
    }
    assert {"AIRFLOW", "DBT", "SPARK"} <= integrations

    # And from the Spark application you can reach the whole pipeline.
    app = conn.execute(
        """
        select r.run_id from runs r join jobs j on j.id = r.job_id
        where j.name = 'dbt_spark_analytics' and j.integration = 'SPARK'
        """
    ).fetchone()
    tree = queries.run_tree(conn, queries.root_of(conn, app["run_id"]))
    assert tree[0]["job_name"] == "analytics_daily"
    assert {"AIRFLOW", "DBT", "SPARK"} <= {n["integration"] for n in tree if n["integration"]}


def test_spark_specific_facets_survive(conn, real_spark):
    """`spark_properties`, `spark_applicationDetails`, `spark_jobDetails` and
    `environment-properties` are all unmodelled. Phase 02 will mine them for
    stage metrics, so they must arrive intact."""
    for payload in real_spark:
        ingest_run_event(conn, RunEvent.model_validate(payload))

    stored = conn.execute(
        """
        select r.facets from runs r join jobs j on j.id = r.job_id
        where j.name = 'dbt_spark_analytics' and j.integration = 'SPARK'
        """
    ).fetchone()["facets"]
    assert {"spark_properties", "spark_applicationDetails"} <= set(stored)
