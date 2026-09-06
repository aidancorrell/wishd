"""Test fixtures.

These are integration tests against a real Postgres, not mocks. The correlator's
whole job is expressed in SQL — upserts, monotonic state transitions, recursive
subtree repair — so testing it against anything other than Postgres would be
testing a different program.

The database is embedded (`pgserver`), so `pytest` works with no Docker and no
service container.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def database_url(tmp_path_factory: pytest.TempPathFactory) -> str:
    pgserver = pytest.importorskip("pgserver")
    data_dir = tmp_path_factory.mktemp("pgdata")
    server = pgserver.get_server(data_dir, cleanup_mode="stop")
    uri = server.get_uri()
    os.environ["DATASPINE_DATABASE_URL"] = uri
    # The background upkeep thread provisions partitions on app startup, which
    # would race the retention tests' own assertions about which partitions
    # exist. Tests that want it exercise `upkeep.run_once` directly.
    os.environ.setdefault("DATASPINE_UPKEEP", "off")

    from dataspine.db import migrate

    migrate(uri)
    return uri


@pytest.fixture()
def conn(database_url: str):
    """A connection wrapped in a transaction that is always rolled back.

    Every test therefore starts from an identical empty schema without paying
    for a migration run.
    """
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        yield connection
        connection.rollback()


@pytest.fixture()
def api_client(database_url: str, monkeypatch):
    """Synchronous ingest: these tests assert an event is durable the moment the
    request returns, which is only true with the queue disabled."""
    from fastapi.testclient import TestClient

    from dataspine import db
    from dataspine.api import app

    monkeypatch.setenv("DATASPINE_INGEST_ASYNC", "false")
    db.reset_pool()
    with TestClient(app) as client:
        yield client
    db.reset_pool()


@pytest.fixture(autouse=True)
def _clean_tables(database_url: str, request: pytest.FixtureRequest):
    """Truncate between tests that talk to the API (which uses its own pool)."""
    yield
    if "api_client" not in request.fixturenames:
        return
    import psycopg

    with psycopg.connect(database_url) as connection:
        connection.execute(
            "truncate events, run_datasets, runs, datasets, jobs, "
            "spark_apps, artifacts, artifact_blobs, monitors, "
            "dataset_snapshots, column_profiles, external_checks, "
            "dataset_entities, incidents, clusters, cost_line_items "
            "restart identity cascade"
        )
        connection.commit()


@pytest.fixture()
def fixtures_dir() -> Path:
    return Path(__file__).parent / "fixtures"


@pytest.fixture()
def api_client_async(database_url: str, monkeypatch):
    """Gateway in async (queued) ingest mode."""
    from fastapi.testclient import TestClient

    from dataspine import db
    from dataspine import queue as ingest_queue
    from dataspine.api import app

    monkeypatch.setenv("DATASPINE_INGEST_ASYNC", "true")
    monkeypatch.delenv("DATASPINE_API_TOKENS", raising=False)
    ingest_queue.reset_queue()
    db.reset_pool()
    _truncate(database_url)
    with TestClient(app) as client:
        yield client
    ingest_queue.reset_queue()   # stop workers before the pool they borrow from
    db.reset_pool()


@pytest.fixture()
def api_client_tiny_queue(database_url: str, monkeypatch):
    """Async mode with a queue small enough that saturation is reachable in a
    test. Proving load shedding needs a queue you can actually fill."""
    from fastapi.testclient import TestClient

    from dataspine import db
    from dataspine import queue as ingest_queue
    from dataspine.api import app

    monkeypatch.setenv("DATASPINE_INGEST_ASYNC", "true")
    monkeypatch.setenv("DATASPINE_INGEST_QUEUE_SIZE", "5")
    monkeypatch.setenv("DATASPINE_INGEST_WORKERS", "1")
    monkeypatch.delenv("DATASPINE_API_TOKENS", raising=False)
    ingest_queue.reset_queue()
    db.reset_pool()
    _truncate(database_url)
    with TestClient(app) as client:
        yield client
    ingest_queue.reset_queue()   # stop workers before the pool they borrow from
    db.reset_pool()


def _truncate(database_url: str) -> None:
    import psycopg

    with psycopg.connect(database_url) as conn:
        # monitors cascades to metric_points and monitor_results.
        conn.execute("truncate events, run_datasets, runs, datasets, jobs, "
            "spark_apps, artifacts, artifact_blobs, monitors, "
            "dataset_snapshots, column_profiles, external_checks, "
            "dataset_entities, incidents, clusters, cost_line_items "
            "restart identity cascade")
        conn.commit()


@pytest.fixture(autouse=True)
def isolated_slack_env(monkeypatch):
    """Slack configuration is read from the environment at delivery time.

    Without this, a developer with a real `DATASPINE_SLACK_WEBHOOK` exported
    would have the suite decide what to do based on their shell — and, in the
    worst case, post test alerts into a channel their team is watching. The
    tests that need a transport set one explicitly.
    """
    # New product-prefixed settings must never pick up a developer's credentials.
    for name in list(os.environ):
        if name.startswith("WISHD_"):
            monkeypatch.delenv(name, raising=False)
    for name in (
        "DATASPINE_API_TOKENS",
        "DATASPINE_PAGERDUTY_ROUTING_KEY",
        "DATASPINE_ALERT_WEBHOOK",
        "DATASPINE_SLACK_BOT_TOKEN",
        "DATASPINE_SLACK_WEBHOOK",
        "DATASPINE_SLACK_CHANNEL",
        "DATASPINE_SLACK_ROUTES",
        "DATASPINE_BASE_URL",
        # Same reasoning, one step further: with these set, every alert in the
        # suite grows a row of buttons pointing at whatever repository the
        # developer happens to have configured.
        "DATASPINE_AGENT_TARGETS",
        "DATASPINE_AGENT_SECRET",
        "DATASPINE_AGENT_REPO",
        "DATASPINE_AGENT_CWD",
        "DATASPINE_SLACK_SIGNING_SECRET",
    ):
        monkeypatch.delenv(name, raising=False)
