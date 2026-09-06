"""Storing Spark metrics and joining them to the run tree.

The join is the point. Spark event logs and OpenLineage events are produced by
different mechanisms that know nothing about each other, but both carry the
Spark **application id** -- OpenLineage in `spark_applicationDetails.applicationId`,
the event log in `SparkListenerApplicationStart.App ID`. That shared key is what
turns "here are some stage metrics" into "this dbt model's Spark job spilled
4GB", which is the sentence Phase 02 exists to produce.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dataspine import spark_metrics, sparklog
from dataspine.events import RunEvent
from dataspine.ingest import ingest_run_event

FIXTURES = Path(__file__).parent / "fixtures"
EVENTLOG = FIXTURES / "spark_eventlog_3.5.7.jsonl"
OL_SPARK = FIXTURES / "spark_openlineage_1.52.0.json"


@pytest.fixture()
def spark_run(conn):
    """Ingest the real OpenLineage Spark events so there is a run to join to."""
    for payload in json.loads(OL_SPARK.read_text()):
        ingest_run_event(conn, RunEvent.model_validate(payload))
    return conn.execute(
        """
        select r.run_id from runs r join jobs j on j.id = r.job_id
        where j.integration = 'SPARK' and j.job_type = 'APPLICATION'
        """
    ).fetchone()["run_id"]


def test_store_and_read_back(conn):
    summary = sparklog.parse_event_log(EVENTLOG)
    spark_metrics.store(conn, summary, source_uri=str(EVENTLOG))

    row = spark_metrics.get_by_app_id(conn, summary.app_id)
    assert row is not None
    assert row["app_name"] == summary.app_name
    assert row["metrics"]["task_count"] == summary.task_count
    assert row["source_uri"].endswith("spark_eventlog_3.5.7.jsonl")


def test_reingest_is_idempotent(conn):
    summary = sparklog.parse_event_log(EVENTLOG)
    spark_metrics.store(conn, summary, source_uri="a")
    spark_metrics.store(conn, summary, source_uri="b")
    count = conn.execute("select count(*) c from spark_apps").fetchone()["c"]
    assert count == 1


def test_metrics_link_to_the_run_via_application_id(conn, spark_run):
    """The whole point: two independent producers, joined on the app id."""
    summary = sparklog.parse_event_log(EVENTLOG)
    linked = spark_metrics.store(conn, summary, source_uri=str(EVENTLOG))

    assert linked == spark_run, "event-log metrics did not attach to the OpenLineage run"
    row = spark_metrics.get_for_run(conn, spark_run)
    assert row is not None
    assert row["app_id"] == summary.app_id


def test_metrics_are_kept_even_when_no_run_matches(conn):
    """A backfill may cover applications whose OpenLineage events were never
    sent, or have not arrived yet. Keeping them unlinked is right: the run may
    show up later, and throwing away metrics we already parsed would be worse
    than an orphan row."""
    summary = sparklog.parse_event_log(EVENTLOG)
    conn.execute("delete from runs")
    linked = spark_metrics.store(conn, summary, source_uri="x")

    assert linked is None
    assert spark_metrics.get_by_app_id(conn, summary.app_id) is not None


def test_late_arriving_run_gets_linked_on_relink(conn):
    """Backfill first, OpenLineage second -- the usual order when importing
    history. A relink pass must attach what arrived late."""
    summary = sparklog.parse_event_log(EVENTLOG)
    spark_metrics.store(conn, summary, source_uri="x")
    assert spark_metrics.get_by_app_id(conn, summary.app_id)["run_id"] is None

    for payload in json.loads(OL_SPARK.read_text()):
        ingest_run_event(conn, RunEvent.model_validate(payload))
    linked = spark_metrics.relink_orphans(conn)

    assert linked == 1
    assert spark_metrics.get_by_app_id(conn, summary.app_id)["run_id"] is not None


def test_lookup_uses_an_index_not_a_seq_scan(conn, spark_run):
    """The app-id lookup runs once per ingested event log; a backfill of a
    year's history would do it thousands of times."""
    plan = "\n".join(
        r["QUERY PLAN"]
        for r in conn.execute(
            """
            explain select run_id from runs
            where facets #>> '{spark_applicationDetails,applicationId}' = 'x'
            """
        ).fetchall()
    )
    assert "Seq Scan" not in plan, plan


def test_metrics_appear_on_the_run_page(api_client, conn):
    """End to end: OpenLineage events over HTTP, event log ingested, and the
    numbers rendered on the run they belong to."""
    api_client.post("/api/v1/lineage/batch", json=json.loads(OL_SPARK.read_text()))
    summary = sparklog.parse_event_log(EVENTLOG)

    import psycopg
    from psycopg.rows import dict_row

    from dataspine.db import database_url

    with psycopg.connect(database_url(), row_factory=dict_row) as own:
        spark_metrics.store(own, summary, source_uri=str(EVENTLOG))
        own.commit()

    runs = api_client.get("/api/v1/runs", params={"integration": "SPARK"}).json()["runs"]
    app = next(r for r in runs if r["job_type"] == "APPLICATION")
    body = api_client.get(f"/runs/{app['run_id']}").text

    assert "Spark metrics" in body
    assert "Tasks" in body


# ------------------------------------------------------------------ backfill


def test_backfill_a_directory_of_event_logs(conn, tmp_path):
    """How history actually gets imported: point it at the directory EMR writes
    event logs to and let it walk."""
    from dataspine.backfill import backfill_directory

    for i in range(3):
        text = EVENTLOG.read_text().replace(
            sparklog.parse_event_log(EVENTLOG).app_id, f"application_{i}"
        )
        (tmp_path / f"application_{i}").write_text(text)
    (tmp_path / "not-an-event-log.txt").write_text("hello")

    result = backfill_directory(conn, tmp_path)

    assert result["ingested"] == 3
    assert result["failed"] == 0
    stored = conn.execute("select count(*) c from spark_apps").fetchone()["c"]
    assert stored == 3


def test_backfill_keeps_going_after_a_bad_file(conn, tmp_path):
    """One corrupt log in a year of history must not abort the import."""
    from dataspine.backfill import backfill_directory

    (tmp_path / "good").write_text(EVENTLOG.read_text())
    (tmp_path / "empty").write_text("")

    result = backfill_directory(conn, tmp_path)
    assert result["ingested"] == 1
    assert result["skipped"] == 1
