"""Scheduler latency and lateness.

The roadmap asked for "Airflow OTel traces alongside OpenLineage, for
scheduler-level latency". The underlying question is the real one — *was this
run late, and was it late because the scheduler sat on it or because it ran
slowly?* — but standing up an OTLP receiver is the wrong way to answer it here,
because the data is already in facets we receive:

  `airflowDagRun.dagRun.run_after`   when the run became eligible
  `airflowDagRun.dagRun.start_date`  when it actually started
  `nominalTime.nominalStartTime`     the scheduled window it belongs to

run_after → start_date is queue delay. nominalStartTime → started_at is
lateness. Both verified against the captured real Airflow fixture.

OTel is still worth having for scheduler *internals* (heartbeat, pool
saturation, executor slots) and is left in the roadmap for that. It is not
needed for this question, and adding a collector to the install to compute a
subtraction we can already do would have been the expensive way to be wrong.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dataspine import timing
from dataspine.events import RunEvent
from dataspine.ingest import ingest_run_event

FIXTURE = Path(__file__).parent / "fixtures" / "airflow_dbt_end_to_end.json"


@pytest.fixture()
def real_events() -> list[dict]:
    return json.loads(FIXTURE.read_text())


# ------------------------------------------------------------- queue delay


def test_queue_delay_from_real_airflow_facets(real_events):
    """The real capture has run_after 23:28:41.949 and start_date 23:28:42.894,
    so roughly a one-second delay. Small here, but this is the number that grows
    when a pool is saturated or the scheduler is behind."""
    dag_start = next(
        e for e in real_events
        if e["job"]["name"] == "analytics_daily" and e.get("eventType") == "START"
    )
    delay = timing.queue_delay_seconds(dag_start["run"]["facets"])
    assert delay is not None
    assert 0 <= delay < 5


def test_queue_delay_is_none_without_the_facet():
    assert timing.queue_delay_seconds({}) is None
    assert timing.queue_delay_seconds({"airflowDagRun": {"dagRun": {}}}) is None


def test_queue_delay_tolerates_junk():
    """Producers send surprising things; a run page must still render."""
    for facets in (
        {"airflowDagRun": "nope"},
        {"airflowDagRun": {"dagRun": {"run_after": "not-a-date", "start_date": "x"}}},
        {"airflowDagRun": {"dagRun": {"run_after": None, "start_date": None}}},
    ):
        assert timing.queue_delay_seconds(facets) is None


def test_negative_queue_delay_is_discarded():
    """A start before the run became eligible means clock skew, not a negative
    wait. Reporting '-4s queued' would just look broken."""
    facets = {
        "airflowDagRun": {
            "dagRun": {
                "run_after": "2026-08-07T23:00:10+00:00",
                "start_date": "2026-08-07T23:00:00+00:00",
            }
        }
    }
    assert timing.queue_delay_seconds(facets) is None


# ---------------------------------------------------------------- lateness


def test_lateness_against_the_nominal_window(real_events):
    """The scheduled run in the capture has a 02:00 nominal time and started at
    23:45, so it is hours late. That is exactly the signal worth surfacing: the
    02:00 load did not happen at 02:00."""
    dag_start = next(
        e for e in real_events
        if e["job"]["name"] == "analytics_daily"
        and e.get("eventType") == "START"
        and "nominalTime" in e["run"]["facets"]
    )
    started = datetime.fromisoformat(dag_start["eventTime"].replace("Z", "+00:00"))
    late = timing.lateness_seconds(dag_start["run"]["facets"], started)
    assert late is not None and late > 3600


def test_manually_triggered_runs_have_no_lateness(real_events):
    """Verified against the real capture: Airflow emits `nominalTime` only for
    runs that belong to a schedule. A manual trigger has no window it was
    supposed to land in, so 'late' is undefined rather than zero — and
    reporting 0 would wrongly imply it was on time.
    """
    manual = [
        e for e in real_events
        if e["job"]["name"] == "analytics_daily"
        and e.get("eventType") == "START"
        and "nominalTime" not in e["run"]["facets"]
    ]
    assert manual, "fixture should contain manually triggered runs"
    for event in manual:
        started = datetime.fromisoformat(event["eventTime"].replace("Z", "+00:00"))
        assert timing.lateness_seconds(event["run"]["facets"], started) is None
        # Queue delay is still available, because it does not depend on a schedule.
        assert timing.queue_delay_seconds(event["run"]["facets"]) is not None


def test_early_runs_are_not_reported_as_late():
    facets = {"nominalTime": {"nominalStartTime": "2026-08-07T02:00:00+00:00"}}
    started = datetime(2026, 8, 7, 1, 55, tzinfo=UTC)
    assert timing.lateness_seconds(facets, started) is None


def test_lateness_needs_both_halves():
    assert timing.lateness_seconds({}, datetime.now(UTC)) is None
    facets = {"nominalTime": {"nominalStartTime": "2026-08-07T02:00:00+00:00"}}
    assert timing.lateness_seconds(facets, None) is None


# ------------------------------------------------------------- integration


def test_run_timing_is_exposed_on_the_run(conn, real_events):
    """Surfaced from stored facets, so it works for runs ingested before this
    code existed — no reprocessing required."""
    for payload in real_events:
        ingest_run_event(conn, RunEvent.model_validate(payload))

    row = conn.execute(
        """
        select r.run_id, r.facets, r.started_at
        from runs r join jobs j on j.id = r.job_id
        where j.name = 'analytics_daily' and r.producer ilike '%%airflow%%'
          and r.facets ? 'nominalTime'
        limit 1
        """
    ).fetchone()

    info = timing.run_timing(row["facets"], row["started_at"])
    assert info["queue_delay_seconds"] is not None
    assert info["lateness_seconds"] is not None


def test_run_timing_is_all_none_for_a_non_airflow_run(conn):
    from dataspine.simulate import build_pipeline

    for payload in build_pipeline(fail_model=None, start=datetime(2026, 8, 1, tzinfo=UTC)):
        ingest_run_event(conn, RunEvent.model_validate(payload))
    row = conn.execute(
        """
        select r.facets, r.started_at from runs r join jobs j on j.id = r.job_id
        where j.integration = 'SPARK' limit 1
        """
    ).fetchone()
    info = timing.run_timing(row["facets"], row["started_at"])
    assert info["queue_delay_seconds"] is None


def test_delay_is_shown_on_the_run_page(api_client):
    """A late run should say so on the page, not require arithmetic."""
    events = json.loads(FIXTURE.read_text())
    api_client.post("/api/v1/lineage/batch", json=events)

    runs = api_client.get(
        "/api/v1/runs", params={"integration": "AIRFLOW", "limit": 100}
    ).json()["runs"]
    dag = next(r for r in runs if r["job_type"] == "DAG")
    body = api_client.get(f"/runs/{dag['run_id']}").text
    assert "Queued" in body or "Late" in body


@pytest.mark.parametrize(
    "seconds,expected",
    [(0, "0s"), (45, "45s"), (90, "1m30s"), (3900, "1h05m"), (90000, "1d1h")],
)
def test_human_readable_durations(seconds, expected):
    assert timing.humanize(seconds) == expected
