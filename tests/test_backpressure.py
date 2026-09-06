"""Ingest backpressure.

The rule this encodes: **a slow or dead Postgres must never become latency in a
Spark driver.** The gateway sits in the hot path of production compute, and the
OpenLineage HTTP transport emits synchronously from the driver thread. If our
database has a bad minute and we make the driver wait for it, we have made
someone's ETL slower by installing an observability tool -- which ends the pilot.

So the ingest path is a bounded queue with a background drain, and when the
queue is full we **shed load**: accept the request, drop the event, and count the
drop loudly. Losing observability data is bad; adding seconds to a Spark stage is
worse. That trade is deliberate and `dropped` in ingest health is how you find
out it happened.

Load shedding always answers 2xx. A 5xx would make well-behaved OpenLineage
clients retry, which is precisely the wrong response to saturation.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime

import pytest

from dataspine.simulate import build_pipeline

T0 = datetime(2026, 8, 6, 2, 0, 0, tzinfo=UTC)


def _wait_for(predicate, timeout: float = 10.0, interval: float = 0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


# ------------------------------------------------------------------- behaviour


def test_events_are_persisted_asynchronously(api_client_async):
    """Accepted now, durable shortly after. The producer does not wait for the
    write."""
    events = build_pipeline(fail_model=None, start=T0)
    for event in events:
        resp = api_client_async.post("/api/v1/lineage", json=event)
        assert resp.status_code in (201, 202), resp.text

    # Wait for the queue to actually drain, not merely for the first row to
    # land. With concurrent workers a child is routinely persisted before its
    # parent, so `unstitched_runs` is transiently non-zero mid-drain -- that is
    # correct eventual-consistency behaviour, not a defect, and asserting a
    # steady-state property before steady state is reached is a flaky test.
    assert _wait_for(
        lambda: api_client_async.get("/api/v1/health/ingest").json()["processed_events"]
        >= len(events)
    ), "queued events never reached Postgres"

    health = api_client_async.get("/api/v1/health/ingest").json()
    assert health["queue_depth"] == 0
    assert health["unstitched_runs"] == 0, (
        "tree left unstitched after the queue drained"
    )


def test_ingest_returns_before_the_write_completes(api_client_async):
    """The point of the queue: response latency is decoupled from write latency.

    Not asserting a hard number -- CI machines are noisy -- but enqueueing a
    whole pipeline must be markedly faster than the synchronous path, and must
    not scale with database work.
    """
    events = build_pipeline(fail_model=None, start=T0)
    start = time.monotonic()
    for event in events:
        api_client_async.post("/api/v1/lineage", json=event)
    elapsed = time.monotonic() - start

    per_event_ms = (elapsed / len(events)) * 1000
    assert per_event_ms < 25, f"enqueue took {per_event_ms:.1f}ms/event; queue is not decoupling"


def test_saturation_sheds_load_with_2xx_not_5xx(api_client_tiny_queue):
    """A full queue must never produce a 5xx.

    OpenLineage's HTTP transport retries on server errors. Answering 5xx under
    saturation turns a slow database into a retry storm against the very system
    that is already struggling, while the Spark driver waits on each retry.
    """
    events = build_pipeline(fail_model=None, start=T0) * 6
    codes = set()
    for event in events:
        codes.add(api_client_tiny_queue.post("/api/v1/lineage", json=event).status_code)

    assert codes, "no responses recorded"
    assert all(200 <= code < 300 for code in codes), f"saturation produced non-2xx: {codes}"
    assert 202 in codes, "queue never saturated; test cannot prove shedding"


def test_shed_events_are_counted_loudly(api_client_tiny_queue):
    """Silent data loss is unacceptable. If we drop, the number must be visible
    next to the other ingest health figures."""
    events = build_pipeline(fail_model=None, start=T0) * 6
    for event in events:
        api_client_tiny_queue.post("/api/v1/lineage", json=event)

    health = api_client_tiny_queue.get("/api/v1/health/ingest").json()
    assert "dropped_events" in health
    assert health["dropped_events"] > 0
    assert "queue_depth" in health


def test_batch_endpoint_also_shed_safely(api_client_tiny_queue):
    events = build_pipeline(fail_model=None, start=T0) * 4
    resp = api_client_tiny_queue.post("/api/v1/lineage/batch", json=events)
    assert 200 <= resp.status_code < 300
    body = resp.json()
    # Whatever it could not take must be reported, not silently discarded.
    assert body["accepted"] + body.get("shed", 0) == len(events)


def test_queue_drains_on_shutdown(database_url, monkeypatch):
    """A deploy must not vaporise whatever is still in flight."""
    import psycopg
    from fastapi.testclient import TestClient

    from dataspine import db
    from dataspine import queue as ingest_queue
    from dataspine.api import app

    monkeypatch.setenv("DATASPINE_INGEST_ASYNC", "true")
    monkeypatch.delenv("DATASPINE_API_TOKENS", raising=False)
    ingest_queue.reset_queue()
    db.reset_pool()

    with psycopg.connect(database_url) as conn:
        conn.execute("truncate events, run_datasets, runs, datasets, jobs, "
            "spark_apps, artifacts, artifact_blobs restart identity cascade")
        conn.commit()

    events = build_pipeline(fail_model=None, start=T0)
    with TestClient(app) as client:
        for event in events:
            client.post("/api/v1/lineage", json=event)
        # Exiting the context triggers FastAPI shutdown.

    with psycopg.connect(database_url) as conn:
        persisted = conn.execute("select count(*) from events").fetchone()[0]
    ingest_queue.reset_queue()
    db.reset_pool()
    assert persisted == len(events), (
        f"shutdown lost events: {persisted}/{len(events)} persisted"
    )


def test_a_dead_database_never_becomes_producer_latency(api_client_async, monkeypatch):
    """The worst case, stated plainly: Postgres is gone. The gateway must still
    answer fast and must not raise. We lose data; the pipeline keeps running."""
    from dataspine import ingest
    from dataspine import queue as ingest_queue

    def explode(*args, **kwargs):
        raise RuntimeError("postgres is down")

    monkeypatch.setattr(ingest, "ingest_run_event", explode)

    events = build_pipeline(fail_model=None, start=T0)
    start = time.monotonic()
    for event in events:
        resp = api_client_async.post("/api/v1/lineage", json=event)
        assert 200 <= resp.status_code < 300
    elapsed = time.monotonic() - start
    assert elapsed < 5, f"a dead database blocked ingest for {elapsed:.1f}s"

    # Every write failed, and the failures are counted rather than swallowed.
    q = ingest_queue.get_queue()
    assert _wait_for(lambda: q.stats()["failed_events"] > 0)

    # Stop the workers here, while the patch is still installed. Left running,
    # they race fixture teardown and lazily rebuild a connection pool that
    # nothing then owns.
    ingest_queue.reset_queue()


@pytest.mark.parametrize("endpoint", ["/api/v1/lineage", "/api/v1/lineage/batch"])
def test_malformed_payloads_still_fail_open_in_async_mode(api_client_async, endpoint):
    """Async mode must not regress the fail-open contract."""
    resp = api_client_async.post(
        endpoint, content=b"{not json", headers={"content-type": "application/json"}
    )
    assert 200 <= resp.status_code < 300
