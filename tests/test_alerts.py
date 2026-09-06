"""Alert delivery.

Everything here is about restraint. Detection was the easy half; a tool that
delivers badly is worse than one that does not deliver at all, because the team
mutes the channel and then misses the alert that mattered.

Four rules the tests exist to hold:

  **Only transitions.** A table broken since 02:00 is one alert, not one an hour.
  **Recoveries too.** "It is fixed" is as load-bearing as "it broke", and a tool
  that only ever sends bad news trains people to dread the channel.
  **One message per sweep.** Twelve breaches from one bad upstream is one
  notification with twelve lines, not twelve notifications.
  **Delivery never breaks monitoring.** A 500 from Slack must not fail a check
  run, and must be recorded rather than swallowed.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from dataspine import alerts, checks, monitors

NOW = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)


class Recorder:
    """An httpx client stand-in that records posts instead of sending them."""

    def __init__(self, status=200, boom=False):
        self.calls: list[tuple[str, dict]] = []
        self.status = status
        self.boom = boom

    def post(self, url, json=None, headers=None, **kwargs):
        if self.boom:
            raise httpx.ConnectError("connection refused")
        self.calls.append((url, json))
        return httpx.Response(self.status, request=httpx.Request("POST", url))


def _result(monitor, status, message, *, transitioned=True):
    return {
        "monitor": monitor,
        "status": status,
        "message": message,
        "value": 1.0,
        "collected": 1,
        "transitioned": transitioned,
    }


# ------------------------------------------------------------------- selection


def test_only_transitions_are_delivered(monkeypatch):
    """The dedup rule, at the delivery boundary rather than in each channel."""
    monkeypatch.setenv(alerts.WEBHOOK_ENV, "https://example.invalid/hook")
    client = Recorder()

    sent = alerts.deliver(
        [
            _result("stale_table", "breach", "6h old", transitioned=True),
            _result("still_stale", "breach", "7h old", transitioned=False),
        ],
        client=client,
    )

    assert len(client.calls) == 1, "one grouped notification"
    body = client.calls[0][1]
    assert "stale_table" in json.dumps(body)
    assert "still_stale" not in json.dumps(body)
    assert sent == 1


def test_recoveries_are_delivered_too(monkeypatch):
    monkeypatch.setenv(alerts.WEBHOOK_ENV, "https://example.invalid/hook")
    client = Recorder()
    alerts.deliver([_result("fct_orders", "ok", "last written 12 minutes ago")], client=client)

    payload = json.dumps(client.calls[0][1])
    assert "fct_orders" in payload
    assert "recovered" in payload.lower()


def test_insufficient_data_is_never_an_alert(monkeypatch):
    """A monitor on a table that has not run yet is not news.

    It transitions on its first evaluation, so without this rule every new
    install pages itself the moment someone runs `dataspine apply`.
    """
    monkeypatch.setenv(alerts.WEBHOOK_ENV, "https://example.invalid/hook")
    client = Recorder()
    sent = alerts.deliver(
        [_result("new_monitor", "insufficient_data", "no observations yet")], client=client
    )
    assert sent == 0
    assert client.calls == []


def test_monitor_errors_go_out_but_are_marked_as_our_problem(monkeypatch):
    """"Broken monitor" and "broken data" need different readers, so the message
    has to distinguish them rather than reporting both as a breach."""
    monkeypatch.setenv(alerts.WEBHOOK_ENV, "https://example.invalid/hook")
    client = Recorder()
    alerts.deliver([_result("bad_config", "error", "KeyError: max_seconds")], client=client)

    payload = json.dumps(client.calls[0][1]).lower()
    assert "bad_config" in payload
    assert "monitor error" in payload


# -------------------------------------------------------------------- grouping


def test_one_sweep_is_one_notification(monkeypatch):
    """Twelve breaches from one bad upstream is one message with twelve lines.

    Phase 04 groups by lineage and can say *why* they are related; until then the
    honest grouping is "these were found together".
    """
    monkeypatch.setenv(alerts.WEBHOOK_ENV, "https://example.invalid/hook")
    client = Recorder()
    alerts.deliver(
        [_result(f"table_{i}", "breach", f"{i}h old") for i in range(12)], client=client
    )

    assert len(client.calls) == 1
    payload = json.dumps(client.calls[0][1])
    for i in range(12):
        assert f"table_{i}" in payload


def test_breaches_are_listed_before_recoveries(monkeypatch):
    monkeypatch.setenv(alerts.WEBHOOK_ENV, "https://example.invalid/hook")
    client = Recorder()
    alerts.deliver(
        [
            _result("recovered_one", "ok", "fine now"),
            _result("broken_one", "breach", "6h old"),
        ],
        client=client,
    )

    payload = json.dumps(client.calls[0][1])
    assert payload.index("broken_one") < payload.index("recovered_one")


# -------------------------------------------------------------------- channels


def test_no_channel_configured_is_silent_not_an_error():
    """The default install has no webhook. That is a valid state, not a warning
    to print on every check run."""
    client = Recorder()
    assert alerts.deliver([_result("x", "breach", "y")], client=client) == 0
    assert client.calls == []


def test_slack_payload_uses_blocks_and_names_the_monitor(monkeypatch):
    monkeypatch.setenv(alerts.SLACK_ENV, "https://hooks.slack.com/services/T/B/X")
    client = Recorder()
    alerts.deliver([_result("fct_orders_freshness", "breach", "6h old, limit 90m")], client=client)

    url, body = client.calls[0]
    assert url.startswith("https://hooks.slack.com/")
    payload = json.dumps(body)
    assert "fct_orders_freshness" in payload
    assert "6h old, limit 90m" in payload
    assert "blocks" in body


def test_pagerduty_sends_one_event_per_monitor_with_a_stable_dedup_key(monkeypatch):
    """PagerDuty is an incident tracker, not a chat room.

    It needs one event per alert with a stable `dedup_key` so that a recovery
    resolves the incident the breach opened, rather than leaving a human to close
    it by hand.
    """
    monkeypatch.setenv(alerts.PAGERDUTY_ENV, "routing-key-123")
    client = Recorder()
    alerts.deliver(
        [
            _result("fct_orders_freshness", "breach", "6h old"),
            _result("dim_users_volume", "ok", "fine now"),
        ],
        client=client,
    )

    assert len(client.calls) == 2, "one PagerDuty event per monitor, not one per sweep"
    by_key = {c[1]["dedup_key"]: c[1] for c in client.calls}
    assert "dataspine/fct_orders_freshness" in by_key
    assert by_key["dataspine/fct_orders_freshness"]["event_action"] == "trigger"
    assert by_key["dataspine/dim_users_volume"]["event_action"] == "resolve"


def test_every_configured_channel_receives_the_alert(monkeypatch):
    monkeypatch.setenv(alerts.SLACK_ENV, "https://hooks.slack.com/services/T/B/X")
    monkeypatch.setenv(alerts.WEBHOOK_ENV, "https://example.invalid/hook")
    client = Recorder()
    alerts.deliver([_result("x", "breach", "y")], client=client)
    assert len(client.calls) == 2


# ------------------------------------------------------------------- failure


def test_a_failing_channel_does_not_raise(monkeypatch):
    """A 500 from Slack must not fail the check run that found the problem.

    The alert is already lost; losing the monitoring sweep as well would turn a
    delivery outage into a detection outage.
    """
    monkeypatch.setenv(alerts.WEBHOOK_ENV, "https://example.invalid/hook")
    client = Recorder(boom=True)
    assert alerts.deliver([_result("x", "breach", "y")], client=client) == 0


def test_a_failing_channel_does_not_stop_the_others(monkeypatch):
    monkeypatch.setenv(alerts.SLACK_ENV, "https://hooks.slack.com/services/T/B/X")
    monkeypatch.setenv(alerts.WEBHOOK_ENV, "https://example.invalid/hook")

    class HalfBroken(Recorder):
        def post(self, url, json=None, headers=None, **kwargs):
            if "slack" in url:
                raise httpx.ConnectError("refused")
            return super().post(url, json=json, headers=headers, **kwargs)

    client = HalfBroken()
    assert alerts.deliver([_result("x", "breach", "y")], client=client) == 1


# ---------------------------------------------------------------- audit trail


def test_delivery_is_recorded_so_a_missing_page_is_explainable(conn, monkeypatch):
    """"Why did I not get paged?" must be answerable six weeks later."""
    monkeypatch.setenv(alerts.WEBHOOK_ENV, "https://example.invalid/hook")
    monitors.apply_specs(
        conn,
        [monitors.MonitorSpec("fresh", "freshness", "dataset", "x", {"max_age_minutes": 5},
                              source="t.yml")],
        sources=["t.yml"],
    )
    monitor = monitors.get_monitor(conn, "fresh")

    client = Recorder(status=500)
    alerts.deliver(
        [_result("fresh", "breach", "6h old")], client=client, conn=conn
    )

    rows = conn.execute("select * from alerts order by id").fetchall()
    assert len(rows) == 1
    assert rows[0]["monitor_id"] == monitor["id"]
    assert rows[0]["status"] == "breach"
    assert rows[0]["delivered"] is False
    assert "500" in rows[0]["error"]


def test_successful_delivery_is_recorded_too(conn, monkeypatch):
    monkeypatch.setenv(alerts.WEBHOOK_ENV, "https://example.invalid/hook")
    monitors.apply_specs(
        conn,
        [monitors.MonitorSpec("fresh", "freshness", "dataset", "x", {"max_age_minutes": 5},
                              source="t.yml")],
        sources=["t.yml"],
    )
    alerts.deliver([_result("fresh", "breach", "6h old")], client=Recorder(), conn=conn)

    row = conn.execute("select * from alerts order by id").fetchone()
    assert row["delivered"] is True
    assert row["channel"] == "webhook"


# ------------------------------------------------------------- wired into check


def test_check_all_delivers_transitions(conn, monkeypatch):
    """Alerting has to be part of the sweep, not a step someone remembers."""
    monkeypatch.setenv(alerts.WEBHOOK_ENV, "https://example.invalid/hook")

    job = conn.execute(
        "insert into jobs (namespace, name, integration) values ('t','j','DBT') returning id"
    ).fetchone()["id"]
    conn.execute(
        "insert into runs (run_id, job_id, state, started_at, ended_at) "
        "values (gen_random_uuid(), %s, 'FAILED', %s, %s)",
        (job, NOW - timedelta(hours=1), NOW - timedelta(hours=1)),
    )
    monitors.apply_specs(
        conn,
        [monitors.MonitorSpec("rel", "job_failure_rate", "job", "j",
                              {"max_rate": 0.0, "window_hours": 24}, source="t.yml")],
        sources=["t.yml"],
    )

    client = Recorder()
    results = checks.check_all(conn, now=NOW, client=client)

    assert results[0]["status"] == "breach"
    assert len(client.calls) == 1
    assert "rel" in json.dumps(client.calls[0][1])


def test_check_all_can_be_run_without_alerting(conn, monkeypatch):
    """`--no-alert` exists so a backfill or a threshold experiment cannot page the
    on-call for six weeks of history at once."""
    monkeypatch.setenv(alerts.WEBHOOK_ENV, "https://example.invalid/hook")
    monitors.apply_specs(
        conn,
        [monitors.MonitorSpec("fresh", "freshness", "dataset", "nothing",
                              {"max_age_minutes": 5}, source="t.yml")],
        sources=["t.yml"],
    )
    client = Recorder()
    checks.check_all(conn, now=NOW, alert=False, client=client)
    assert client.calls == []


@pytest.mark.parametrize("status", ["ok", "breach", "error"])
def test_alertable_statuses(status):
    assert alerts.is_alertable(status) is True


def test_insufficient_data_is_not_alertable():
    assert alerts.is_alertable("insufficient_data") is False


def test_arming_a_monitor_does_not_consume_its_first_alert(conn, monkeypatch):
    """`apply` builds history and reports status; it must not silently absorb the
    transition that `check` would have alerted on.

    Found end to end: tightening a threshold so a monitor breached, then running
    `check`, produced no alert — because `apply`'s arming evaluation had already
    moved `last_status` to `breach`, leaving nothing for `check` to transition
    from. The breach was real, visible in the UI, and never delivered.
    """
    job = conn.execute(
        "insert into jobs (namespace, name, integration) values ('t','j','DBT') returning id"
    ).fetchone()["id"]
    conn.execute(
        "insert into runs (run_id, job_id, state, started_at, ended_at) "
        "values (gen_random_uuid(), %s, 'COMPLETED', %s, %s)",
        (job, NOW - timedelta(hours=1), NOW - timedelta(hours=1) + timedelta(minutes=30)),
    )
    monitors.apply_specs(
        conn,
        [monitors.MonitorSpec("dur", "job_duration", "job", "j", {"max_seconds": 60},
                              source="t.yml")],
        sources=["t.yml"],
    )
    monitor = monitors.get_monitor(conn, "dur")

    # Arming: collects history, judges, reports — records nothing.
    armed = checks.evaluate(conn, monitor, since=None, now=NOW, record=False)
    assert armed["status"] == "breach"
    assert armed["collected"] > 0
    assert monitors.get_monitor(conn, "dur")["last_status"] is None
    assert conn.execute("select count(*) as n from monitor_results").fetchone()["n"] == 0

    # The first real sweep still sees a fresh transition, and alerts.
    monkeypatch.setenv(alerts.WEBHOOK_ENV, "https://example.invalid/hook")
    client = Recorder()
    results = checks.check_all(conn, now=NOW, client=client)

    assert results[0]["transitioned"] is True
    assert len(client.calls) == 1
