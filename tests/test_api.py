"""Gateway tests.

The behavioural contract here is unusual and deliberate: this endpoint **fails
open**. It runs in the hot path of production Spark drivers, so a malformed
event gets archived and reported, never 4xx'd into a client retry loop that
hammers a driver's heartbeat thread.
"""

from __future__ import annotations

from datetime import UTC, datetime

from dataspine.simulate import build_pipeline, shuffle_events

T0 = datetime(2026, 8, 6, 2, 0, 0, tzinfo=UTC)


def test_health(api_client):
    health = api_client.get("/health").json()
    assert health["status"] == "ok"
    # Operators need to be able to confirm auth is on without holding a token,
    # so /health reports it and stays unauthenticated.
    assert health["auth_enabled"] is False
    assert api_client.get("/ready").json() == {"status": "ready"}


def test_post_lineage_accepts_openlineage_events(api_client):
    events = build_pipeline(fail_model=None, start=T0)
    for event in events:
        resp = api_client.post("/api/v1/lineage", json=event)
        assert resp.status_code == 201, resp.text
        assert resp.json()["accepted"] is True

    runs = api_client.get("/api/v1/runs", params={"roots_only": True}).json()
    assert runs["count"] == 1


def test_tree_endpoint_resolves_from_any_run_in_the_pipeline(api_client):
    """Paste in a Spark SQL run id — the id you would actually have, because it
    is the thing that failed — and get the whole pipeline back."""
    for event in shuffle_events(build_pipeline(fail_model=None, start=T0), seed=3):
        api_client.post("/api/v1/lineage", json=event)

    spark_runs = api_client.get("/api/v1/runs", params={"integration": "SPARK"}).json()["runs"]
    leaf = next(r for r in spark_runs if r["job_type"] == "SQL_JOB")

    tree = api_client.get(f"/api/v1/runs/{leaf['run_id']}/tree").json()
    names = [n["job_name"] for n in tree["nodes"]]
    assert names[0] == "analytics_daily"
    assert len(names) > 5
    assert any(d["direction"] == "OUTPUT" for d in tree["datasets"])


def test_malformed_payloads_fail_open(api_client):
    # Not JSON at all.
    resp = api_client.post(
        "/api/v1/lineage", content=b"{not json", headers={"content-type": "application/json"}
    )
    assert resp.status_code == 200
    assert resp.json()["accepted"] is False

    # Valid JSON, missing the run object (a DatasetEvent or JobEvent).
    resp = api_client.post("/api/v1/lineage", json={"eventTime": T0.isoformat(), "job": {}})
    assert resp.status_code == 200
    assert resp.json()["accepted"] is False

    # A RunEvent with a non-UUID runId.
    resp = api_client.post(
        "/api/v1/lineage",
        json={
            "eventTime": T0.isoformat(),
            "producer": "t",
            "schemaURL": "t",
            "eventType": "START",
            "run": {"runId": "not-a-uuid"},
            "job": {"namespace": "n", "name": "j"},
        },
    )
    assert resp.status_code == 200
    assert resp.json()["accepted"] is False


def test_ingest_health_endpoint(api_client):
    for event in build_pipeline(fail_model="fct_order_items", start=T0):
        api_client.post("/api/v1/lineage", json=event)

    health = api_client.get("/api/v1/health/ingest").json()
    assert health["unstitched_runs"] == 0
    assert health["placeholder_runs"] == 0
    assert set(health["runs_by_integration"]) == {"AIRFLOW", "DBT", "SPARK"}


def test_unknown_run_404s(api_client):
    resp = api_client.get("/api/v1/runs/00000000-0000-0000-0000-000000000000")
    assert resp.status_code == 404
