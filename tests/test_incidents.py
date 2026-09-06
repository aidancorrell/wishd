"""Incidents: grouping, root cause, blast radius, and alert suppression.

This is what the lineage graph is *for*. Everything before it produced signals;
this decides which of them are the same event and which of them are consequences
of it.

The roadmap names the stake plainly: **alert fatigue is what kills these tools in
month three.** One upstream table arriving late breaches the freshness monitor on
every one of the fifty tables built from it, and a tool that sends fifty pages
gets muted — after which the fifty-first, which was a genuinely different
problem, is missed too.

So the shape is: breaches that are connected by lineage and close in time become
one incident with one cause and a blast radius. The downstream breaches are still
recorded — they are the blast radius, and someone will want them — but only the
cause is delivered.

The hard tests here are the ones about *not* grouping. Two unrelated failures
merged into one incident is worse than two alerts, because the second problem
becomes invisible behind the first one's story.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from dataspine import alerts, checks, identity, incidents, lineage, monitors

NOW = datetime(2026, 8, 9, 12, 0, tzinfo=UTC)


# --------------------------------------------------------------------- setup


def _dataset(conn, name) -> int:
    return conn.execute(
        "insert into datasets (namespace, name) values ('file', %s) "
        "on conflict (namespace, name) do update set updated_at = now() returning id",
        (f"/warehouse/{name}",),
    ).fetchone()["id"]


def _model(conn, name, *, inputs=()):
    job = conn.execute(
        "insert into jobs (namespace, name, integration) values ('t', %s, 'SPARK') "
        "on conflict (namespace, name) do update set name = excluded.name returning id",
        (f"build_{name}",),
    ).fetchone()["id"]
    run = uuid4()
    conn.execute(
        "insert into runs (run_id, job_id, root_run_id, state, started_at, ended_at) "
        "values (%s, %s, %s, 'COMPLETED', %s, %s)",
        (run, job, run, NOW, NOW),
    )
    conn.execute(
        "insert into run_datasets (run_id, dataset_id, direction) values (%s, %s, 'OUTPUT')",
        (run, _dataset(conn, name)),
    )
    for upstream in inputs:
        conn.execute(
            "insert into run_datasets (run_id, dataset_id, direction) "
            "values (%s, %s, 'INPUT') on conflict do nothing",
            (run, _dataset(conn, upstream)),
        )


def _monitor(conn, name, target, *, kind="freshness"):
    """Apply one monitor from its own source file.

    The per-monitor `source` is load-bearing in the test, not incidental:
    `apply_specs` is scoped to the files it is given, so reconciling several
    single-monitor files that all claim `t.yml` would disable each previous one —
    which is exactly the bad-merge protection working, and would silently leave
    the suite with one enabled monitor.
    """
    source = f"{name}.yml"
    monitors.apply_specs(
        conn,
        [monitors.MonitorSpec(name, kind, "dataset", target, {"max_age_minutes": 60},
                              source=source)],
        sources=[source],
    )
    return monitors.get_monitor(conn, name)


def _breach(conn, monitor, *, at=NOW, status="breach", message="stale"):
    """Record an evaluation directly, as a check sweep would."""
    conn.execute(
        """
        insert into monitor_results (monitor_id, evaluated_at, status, message,
                                     transitioned, context)
        values (%s, %s, %s, %s, true, '{}'::jsonb)
        """,
        (monitor["id"], at, status, message),
    )
    conn.execute(
        "update monitors set last_status = %s, last_evaluated_at = %s where id = %s",
        (status, at, monitor["id"]),
    )


@pytest.fixture()
def pipeline(conn):
    """raw_orders -> stg_orders -> fct_orders -> report_daily, and an unrelated table."""
    _model(conn, "raw_orders")
    _model(conn, "stg_orders", inputs=["raw_orders"])
    _model(conn, "fct_orders", inputs=["stg_orders"])
    _model(conn, "report_daily", inputs=["fct_orders"])
    _model(conn, "unrelated_table")
    identity.resolve(conn)
    lineage.resolve(conn)
    return conn


# ------------------------------------------------------------------ grouping


def test_a_chain_of_breaches_becomes_one_incident(pipeline):
    """The headline case. One late source, four breached monitors, one incident."""
    conn = pipeline
    for name in ("raw_orders", "stg_orders", "fct_orders", "report_daily"):
        _breach(conn, _monitor(conn, f"{name}_fresh", name))

    found = incidents.detect(conn, now=NOW)

    assert len(found) == 1
    assert found[0]["cause_monitor"] == "raw_orders_fresh"
    assert len(found[0]["consequences"]) == 3


def test_the_root_cause_is_the_furthest_upstream_breach(pipeline):
    """Not the first one detected, not the loudest — the one with nothing broken
    above it."""
    conn = pipeline
    for name in ("stg_orders", "fct_orders", "report_daily"):
        _breach(conn, _monitor(conn, f"{name}_fresh", name))

    found = incidents.detect(conn, now=NOW)
    assert found[0]["cause_monitor"] == "stg_orders_fresh"


def test_blast_radius_lists_what_the_cause_reached(pipeline):
    conn = pipeline
    for name in ("stg_orders", "fct_orders", "report_daily"):
        _breach(conn, _monitor(conn, f"{name}_fresh", name))

    found = incidents.detect(conn, now=NOW)
    radius = {c["entity_name"] for c in found[0]["consequences"]}
    assert radius == {"fct_orders", "report_daily"}


def test_unrelated_breaches_stay_separate(pipeline):
    """Two incidents merged into one is worse than two alerts: the second problem
    becomes invisible behind the first one's story."""
    conn = pipeline
    _breach(conn, _monitor(conn, "stg_fresh", "stg_orders"))
    _breach(conn, _monitor(conn, "unrelated_fresh", "unrelated_table"))

    found = incidents.detect(conn, now=NOW)
    assert len(found) == 2
    assert {i["cause_monitor"] for i in found} == {"stg_fresh", "unrelated_fresh"}


def test_breaches_far_apart_in_time_are_not_one_incident(pipeline):
    """Yesterday's outage on the same table is not today's.

    Lineage proximity alone would keep merging every breach a table ever had into
    a single immortal incident.
    """
    conn = pipeline
    _breach(conn, _monitor(conn, "stg_fresh", "stg_orders"), at=NOW - timedelta(days=3))
    _breach(conn, _monitor(conn, "fct_fresh", "fct_orders"), at=NOW)

    found = incidents.detect(conn, now=NOW, window_hours=6)
    assert len(found) == 2


def test_a_recovered_monitor_is_not_part_of_an_incident(pipeline):
    conn = pipeline
    _breach(conn, _monitor(conn, "stg_fresh", "stg_orders"))
    _breach(conn, _monitor(conn, "fct_fresh", "fct_orders"), status="ok", message="fine")

    found = incidents.detect(conn, now=NOW)
    assert len(found) == 1
    assert found[0]["consequences"] == []


def test_a_monitor_on_a_job_rather_than_a_dataset_still_forms_an_incident(pipeline):
    """Job SLOs breach alongside data monitors, and a slow job is frequently the
    cause of the staleness beneath it."""
    conn = pipeline
    monitors.apply_specs(
        conn,
        [monitors.MonitorSpec("slow_job", "job_duration", "job", "build_stg_orders",
                              {"max_seconds": 1}, source="slow_job.yml")],
        sources=["slow_job.yml"],
    )
    _breach(conn, monitors.get_monitor(conn, "slow_job"))

    found = incidents.detect(conn, now=NOW)
    assert len(found) == 1
    assert found[0]["cause_monitor"] == "slow_job"


# ---------------------------------------------------------------- persistence


def test_incidents_are_stored_and_reused(pipeline):
    """A breach that persists across sweeps stays the same incident.

    A new incident per sweep would page someone hourly for one problem, which is
    the fatigue this exists to prevent — just moved one level up.
    """
    conn = pipeline
    _breach(conn, _monitor(conn, "stg_fresh", "stg_orders"))

    first = incidents.detect(conn, now=NOW, persist=True)
    second = incidents.detect(conn, now=NOW + timedelta(hours=1), persist=True)

    assert first[0]["id"] == second[0]["id"]
    assert conn.execute("select count(*) as n from incidents").fetchone()["n"] == 1


def test_an_incident_closes_when_its_cause_recovers(pipeline):
    conn = pipeline
    monitor = _monitor(conn, "stg_fresh", "stg_orders")
    _breach(conn, monitor)
    incidents.detect(conn, now=NOW, persist=True)

    _breach(conn, monitor, status="ok", message="fine now", at=NOW + timedelta(hours=1))
    incidents.detect(conn, now=NOW + timedelta(hours=1), persist=True)

    row = conn.execute("select * from incidents").fetchone()
    assert row["resolved_at"] is not None
    assert incidents.open_incidents(conn) == []


def test_a_reopened_problem_is_a_new_incident(pipeline):
    """Broken, fixed, broken again is two incidents. Reusing the first would make
    the timeline lie about how long it was down."""
    conn = pipeline
    monitor = _monitor(conn, "stg_fresh", "stg_orders")

    _breach(conn, monitor)
    first = incidents.detect(conn, now=NOW, persist=True)[0]["id"]
    _breach(conn, monitor, status="ok", at=NOW + timedelta(hours=1))
    incidents.detect(conn, now=NOW + timedelta(hours=1), persist=True)
    _breach(conn, monitor, at=NOW + timedelta(hours=2))
    second = incidents.detect(conn, now=NOW + timedelta(hours=2), persist=True)[0]["id"]

    assert first != second


# ------------------------------------------------------------- suppression


def test_only_the_cause_is_delivered(pipeline, monkeypatch):
    """Fifty downstream freshness breaches are one alert about the upstream cause.

    This is the whole reason the phase exists, and the difference between a tool
    a team keeps and one they mute in month three.
    """
    conn = pipeline
    results = []
    for name in ("stg_orders", "fct_orders", "report_daily"):
        _breach(conn, _monitor(conn, f"{name}_fresh", name))
        results.append({
            "monitor": f"{name}_fresh", "status": "breach", "message": "stale",
            "value": 1.0, "collected": 1, "transitioned": True,
        })

    monkeypatch.setenv(alerts.WEBHOOK_ENV, "https://example.invalid/hook")
    client = _Recorder()
    alerts.deliver(incidents.suppress(conn, results, now=NOW), client=client, conn=conn)

    payload = json.dumps(client.calls[0][1])
    assert "stg_orders_fresh" in payload
    assert "fct_orders_fresh" not in payload
    assert "report_daily_fresh" not in payload


def test_the_alert_says_how_far_the_problem_reached(pipeline, monkeypatch):
    """A cause with a blast radius is a more useful page than a cause alone —
    it is the difference between "fix this" and "fix this, and these are waiting
    on you"."""
    conn = pipeline
    results = []
    for name in ("stg_orders", "fct_orders", "report_daily"):
        _breach(conn, _monitor(conn, f"{name}_fresh", name))
        results.append({
            "monitor": f"{name}_fresh", "status": "breach", "message": "stale",
            "value": 1.0, "collected": 1, "transitioned": True,
        })

    suppressed = incidents.suppress(conn, results, now=NOW)
    assert len(suppressed) == 1
    assert "2 downstream" in suppressed[0]["message"]


def test_suppression_leaves_unrelated_alerts_alone(pipeline):
    conn = pipeline
    for name, monitor_name in (("stg_orders", "stg_fresh"),
                               ("unrelated_table", "unrelated_fresh")):
        _breach(conn, _monitor(conn, monitor_name, name))

    results = [
        {"monitor": "stg_fresh", "status": "breach", "message": "stale",
         "value": 1.0, "collected": 1, "transitioned": True},
        {"monitor": "unrelated_fresh", "status": "breach", "message": "stale",
         "value": 1.0, "collected": 1, "transitioned": True},
    ]
    suppressed = incidents.suppress(conn, results, now=NOW)
    assert {r["monitor"] for r in suppressed} == {"stg_fresh", "unrelated_fresh"}


def test_recoveries_are_never_suppressed(pipeline):
    """"It is fixed" must always get through, even for a downstream table whose
    breach was suppressed as a consequence."""
    conn = pipeline
    _breach(conn, _monitor(conn, "stg_fresh", "stg_orders"))
    _breach(conn, _monitor(conn, "fct_fresh", "fct_orders"), status="ok")

    results = [
        {"monitor": "stg_fresh", "status": "breach", "message": "stale",
         "value": 1.0, "collected": 1, "transitioned": True},
        {"monitor": "fct_fresh", "status": "ok", "message": "fine",
         "value": 1.0, "collected": 1, "transitioned": True},
    ]
    suppressed = incidents.suppress(conn, results, now=NOW)
    assert {r["monitor"] for r in suppressed} == {"stg_fresh", "fct_fresh"}


def test_suppression_is_a_no_op_without_lineage(conn):
    """A fresh install has no graph. Suppression must pass everything through
    rather than silently swallowing alerts it cannot reason about."""
    _model(conn, "a")
    _model(conn, "b")
    identity.resolve(conn)

    for name in ("a", "b"):
        _breach(conn, _monitor(conn, f"{name}_fresh", name))
    results = [
        {"monitor": f"{name}_fresh", "status": "breach", "message": "stale",
         "value": 1.0, "collected": 1, "transitioned": True}
        for name in ("a", "b")
    ]
    assert len(incidents.suppress(conn, results, now=NOW)) == 2


def test_check_all_suppresses_consequences_end_to_end(pipeline, monkeypatch):
    """Wired into the sweep, not a step someone remembers."""
    conn = pipeline
    for name in ("stg_orders", "fct_orders"):
        monitors.apply_specs(
            conn,
            [monitors.MonitorSpec(f"{name}_fresh", "freshness", "dataset", name,
                                  {"max_age_minutes": 1}, source=f"{name}.yml")],
            sources=[f"{name}.yml"],
        )

    monkeypatch.setenv(alerts.WEBHOOK_ENV, "https://example.invalid/hook")
    client = _Recorder()
    results = checks.check_all(conn, now=NOW + timedelta(hours=2), client=client)

    assert {r["status"] for r in results} == {"breach"}
    payload = json.dumps(client.calls[0][1])
    assert "stg_orders_fresh" in payload
    assert "fct_orders_fresh" not in payload


class _Recorder:
    def __init__(self):
        self.calls = []

    def post(self, url, json=None, headers=None, **kwargs):
        import httpx

        self.calls.append((url, json))
        return httpx.Response(200, request=httpx.Request("POST", url))
