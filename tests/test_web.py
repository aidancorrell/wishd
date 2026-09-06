"""UI tests.

Not pixel tests — these assert that the page contains the facts a person opened
it to find. The run detail page exists to answer "why did last night fail", so
the test asserts the error text and the pipeline context are actually on it.
"""

from __future__ import annotations

from datetime import UTC, datetime

from dataspine.simulate import build_pipeline, shuffle_events

T0 = datetime(2026, 8, 6, 2, 0, 0, tzinfo=UTC)


def _load(client, fail_model="fct_order_items"):
    events = shuffle_events(build_pipeline(fail_model=fail_model, start=T0), seed=11)
    client.post("/api/v1/lineage/batch", json=events)
    return client.get("/api/v1/runs", params={"limit": 200}).json()["runs"]


def test_run_list_renders(api_client):
    _load(api_client)
    page = api_client.get("/")
    assert page.status_code == 200
    body = page.text
    assert "analytics_daily" in body
    assert "FAILED" in body
    # Ingest health is on the list page, not buried behind navigation. Matched
    # case-insensitively: the contract is that the number is *there*, and the
    # label's casing is the header's business.
    assert "unstitched" in body.lower()


def test_run_list_filters(api_client):
    """Filtering selects whole pipelines, not individual rows.

    This assertion was inverted deliberately when the run list became
    hierarchical. The old flat table filtered *rows*, so `integration=SPARK` hid
    everything that was not Spark. A tree cannot work that way: a Spark run is
    never a root — it is always something an Airflow DAG caused — so filtering
    rows returns either nothing at all, or orphans with their ancestry cut off,
    which is precisely the context the page exists to show.

    So the filter now asks "which pipelines contain Spark work", and draws each
    match whole. The Airflow task below is context, not noise.
    """
    _load(api_client)
    body = api_client.get("/", params={"integration": "SPARK"}).text
    assert "dbt_spark_analytics" in body
    assert "analytics_daily.dbt_run_marts" in body

    # A filter matching nothing must return nothing, not everything — the usual
    # failure mode when a WHERE clause is built conditionally.
    empty = api_client.get("/", params={"job": "no-such-pipeline-anywhere"}).text
    assert "No pipelines match" in empty
    assert "dbt_spark_analytics" not in empty


def test_run_detail_leads_with_the_failure(api_client):
    """Opening a failed run should surface the Spark error without any
    expanding, clicking, or log fetching."""
    runs = _load(api_client)
    failed_spark = next(
        r for r in runs if r["job_type"] == "SQL_JOB" and r["state"] == "FAILED"
    )
    body = api_client.get(f"/runs/{failed_spark['run_id']}").text

    assert "Container killed by YARN" in body
    assert "Stack trace" in body
    # And the pipeline context: the DAG that owns this Spark execution.
    assert "analytics_daily" in body
    # And the SQL that produced it.
    assert "insert overwrite table" in body


def test_run_detail_shows_job_history(api_client):
    """Two runs of the same pipeline: the detail page must show the prior run so
    'slower than usual' is answerable in place."""
    for start in (T0, T0.replace(day=5)):
        events = build_pipeline(fail_model=None, start=start)
        api_client.post("/api/v1/lineage/batch", json=events)

    runs = api_client.get("/api/v1/runs", params={"job": "fct_orders", "limit": 50}).json()["runs"]
    body = api_client.get(f"/runs/{runs[0]['run_id']}").text
    assert "History" in body
    assert "Rows out" in body


def test_jobs_page(api_client):
    _load(api_client)
    body = api_client.get("/jobs").text
    assert "dbt-run-analytics" in body
    assert "AIRFLOW" in body


def test_unknown_run_renders_not_found(api_client):
    resp = api_client.get("/runs/00000000-0000-0000-0000-000000000000")
    assert resp.status_code == 200
    assert "No run with id" in resp.text


def test_static_css_is_served(api_client):
    resp = api_client.get("/static/style.css")
    assert resp.status_code == 200
    assert "--accent" in resp.text


# ------------------------------------------------------- time range + links


def test_run_list_time_range_filter(api_client):
    """Two pipelines a day apart; a 1-day window must exclude the older one."""
    from datetime import timedelta

    for start in (T0, T0 - timedelta(days=3)):
        api_client.post("/api/v1/lineage/batch", json=build_pipeline(fail_model=None, start=start))

    all_runs = api_client.get("/api/v1/runs", params={"roots_only": True}).json()
    assert all_runs["count"] == 2

    since = (T0 - timedelta(hours=2)).isoformat()
    narrowed = api_client.get(
        "/api/v1/runs", params={"roots_only": True, "since": since}
    ).json()
    assert narrowed["count"] == 1, "since= did not narrow the window"

    body = api_client.get("/", params={"since": since}).text
    assert "analytics_daily" in body


def test_invalid_time_range_is_ignored_not_fatal(api_client):
    """A hand-edited URL must not 500 the landing page."""
    _load(api_client)
    resp = api_client.get("/", params={"since": "not-a-date"})
    assert resp.status_code == 200


def test_run_detail_shows_deep_links(api_client, monkeypatch):
    """A Spark run must offer its UI link on the page, using the URL the real
    listener provides."""
    import json as _json
    from pathlib import Path

    spark_events = _json.loads(
        (Path(__file__).parent / "fixtures" / "spark_openlineage_1.52.0.json").read_text()
    )
    api_client.post("/api/v1/lineage/batch", json=spark_events)

    runs = api_client.get("/api/v1/runs", params={"integration": "SPARK"}).json()["runs"]
    app = next(r for r in runs if r["job_type"] == "APPLICATION")
    body = api_client.get(f"/runs/{app['run_id']}").text

    assert "Spark UI" in body
    assert "Application ID" in body
