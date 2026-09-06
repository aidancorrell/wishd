"""Run failures, incidents and digests: deciding what is worth saying, once.

Monitor alerting gets its dedup decision handed to it — `monitor_results` already
records whether a status transitioned. Nothing here has that, so the tests are
mostly about the ledger doing the same job for events that have no state machine
behind them.

The three rules that matter, in the order they bite:

  **One notification per pipeline, not per task.** A failed dbt model fails the
  Airflow task above it and the Spark job below it. One problem, three rows.
  **A retry that succeeded is not a failure.** Airflow retries. Paging inside the
  retry delay is how a channel learns that dataspine cries wolf.
  **Never notify about history.** A first sweep against an existing database must
  not page anyone about six weeks of old failures.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest

from dataspine import identity, incidents, lineage, monitors, notify, slack

NOW = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)


class Recorder:
    def __init__(self, status=200, body=None):
        self.calls: list[tuple[str, dict]] = []
        self.status = status
        self.body = {"ok": True} if body is None else body

    def post(self, url, json=None, headers=None, **kwargs):
        self.calls.append((url, json))
        return httpx.Response(
            self.status, json=self.body, request=httpx.Request("POST", url)
        )


@pytest.fixture()
def slack_bot(monkeypatch):
    monkeypatch.setenv(slack.BOT_TOKEN_ENV, "xoxb-test")
    monkeypatch.setenv(slack.CHANNEL_ENV, "#data")
    return Recorder()


def _job(conn, name, integration="AIRFLOW") -> int:
    return conn.execute(
        "insert into jobs (namespace, name, integration) values ('t', %s, %s) "
        "on conflict (namespace, name) do update set integration = excluded.integration "
        "returning id",
        (name, integration),
    ).fetchone()["id"]


def _run(
    conn,
    name,
    *,
    state="FAILED",
    root=None,
    parent=None,
    depth=0,
    ended=None,
    error=None,
    integration="AIRFLOW",
):
    run_id = uuid4()
    ended = NOW - timedelta(hours=1) if ended is None else ended
    conn.execute(
        """
        insert into runs (run_id, job_id, parent_run_id, root_run_id, depth, state,
                          started_at, ended_at, error_message)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (run_id, _job(conn, name, integration), parent, root or run_id, depth, state,
         ended, ended, error),
    )
    return run_id


def _failed_pipeline(conn, *, root_state="FAILED", ended=None):
    """An Airflow DAG, its task, and the Spark job underneath — all failed.

    The realistic shape: three run rows, one problem, and only the deepest one
    knows what actually went wrong.
    """
    root = _run(conn, "analytics_daily", state=root_state, ended=ended)
    task = _run(conn, "analytics_daily.dbt_run", root=root, parent=root, depth=1,
                ended=ended)
    _run(conn, "dbt_spark.fct_orders", root=root, parent=task, depth=2, ended=ended,
         integration="SPARK",
         error="org.apache.spark.SparkException: Job aborted\n\tat org.apache...")
    return root


# ------------------------------------------------------------------ run failures


def test_one_notification_per_pipeline_not_per_task(conn):
    _failed_pipeline(conn)
    notes = notify.run_failures(conn, since=NOW - timedelta(hours=24), now=NOW)

    assert len(notes) == 1, "three failed run rows, one problem"
    assert notes[0].event == "run_failure"
    assert notes[0].job == "analytics_daily"


def test_the_deepest_failure_that_said_something_is_the_cause(conn):
    """An Airflow task reporting "task failed" above a Spark job holding the
    stack trace is the less useful of the two to put in a page."""
    _failed_pipeline(conn)
    note = notify.run_failures(conn, since=NOW - timedelta(hours=24), now=NOW)[0]

    assert "dbt_spark.fct_orders" in note.summary
    assert "SparkException" in note.summary
    assert "\tat org.apache" not in note.summary, "first line only; the link has the rest"


def test_a_retry_that_succeeded_is_not_a_failure(conn):
    """Airflow retries tasks. A root that has since completed is not worth
    waking anyone for."""
    _failed_pipeline(conn, root_state="COMPLETED")
    assert notify.run_failures(conn, since=NOW - timedelta(hours=24), now=NOW) == []


def test_a_failure_inside_the_grace_period_waits(conn):
    """Airflow's default retry_delay is five minutes. Paging inside that window
    is paging about something the scheduler was already fixing."""
    _failed_pipeline(conn, ended=NOW - timedelta(minutes=1))
    assert notify.run_failures(
        conn, since=NOW - timedelta(hours=24), now=NOW, grace_minutes=5
    ) == []


def test_old_failures_are_not_news(conn):
    """A first sweep against an existing database must not page about history."""
    _failed_pipeline(conn, ended=NOW - timedelta(days=3))
    assert notify.run_failures(conn, since=NOW - timedelta(hours=24), now=NOW) == []


def test_an_aborted_run_counts_as_a_failure(conn):
    _run(conn, "killed_job", state="ABORTED")
    notes = notify.run_failures(conn, since=NOW - timedelta(hours=24), now=NOW)
    assert len(notes) == 1


def test_two_unrelated_pipelines_are_two_notifications(conn):
    _failed_pipeline(conn)
    _run(conn, "marketing_daily", state="FAILED")
    notes = notify.run_failures(conn, since=NOW - timedelta(hours=24), now=NOW)
    assert {n.job for n in notes} == {"analytics_daily", "marketing_daily"}


def test_the_notification_links_back_to_the_run(conn, monkeypatch):
    monkeypatch.setenv(notify.BASE_URL_ENV, "https://dataspine.acme.internal")
    root = _failed_pipeline(conn)
    note = notify.run_failures(conn, since=NOW - timedelta(hours=24), now=NOW)[0]
    assert note.url == f"https://dataspine.acme.internal/runs/{root}"


# ----------------------------------------------------------------------- ledger


def test_a_pipeline_is_notified_once(conn, slack_bot):
    _failed_pipeline(conn)

    first = notify.sweep(
        conn, since=NOW - timedelta(hours=24), now=NOW, incidents_on=False,
        client=slack_bot,
    )
    second = notify.sweep(
        conn, since=NOW - timedelta(hours=24), now=NOW, incidents_on=False,
        client=slack_bot,
    )

    assert len(first["sent"]) == 1
    assert second["sent"] == []
    assert len(second["skipped"]) == 1
    assert len(slack_bot.calls) == 1


def test_nothing_is_claimed_when_slack_is_not_configured(conn):
    """Claiming with nowhere to send would burn the dedup key, so the first sweep
    after someone finally sets a token would report nothing to say."""
    _failed_pipeline(conn)
    notes = notify.run_failures(conn, since=NOW - timedelta(hours=24), now=NOW)

    summary = notify.send(conn, notes)

    assert summary["sent"] == []
    assert len(summary["skipped"]) == 1
    assert conn.execute("select count(*) as n from notifications").fetchone()["n"] == 0


def test_a_failed_delivery_is_recorded_with_its_error(conn, monkeypatch):
    monkeypatch.setenv(slack.BOT_TOKEN_ENV, "xoxb-test")
    monkeypatch.setenv(slack.CHANNEL_ENV, "#data")
    _failed_pipeline(conn)

    summary = notify.sweep(
        conn, since=NOW - timedelta(hours=24), now=NOW, incidents_on=False,
        client=Recorder(status=500),
    )

    assert len(summary["failed"]) == 1
    row = conn.execute("select * from notifications").fetchone()
    assert row["delivered"] is False
    assert "500" in row["error"]


def test_a_notification_routed_nowhere_is_recorded_as_such(conn, tmp_path, monkeypatch):
    """"Why did nobody get told?" has to be answerable, and "no route matched" is
    a much faster answer than reading the routes file."""
    monkeypatch.setenv(slack.BOT_TOKEN_ENV, "xoxb-test")
    path = tmp_path / "slack.yml"
    path.write_text("routes:\n  - match: {event: digest}\n    channel: '#reports'\n")
    monkeypatch.setenv(slack.ROUTES_ENV, str(path))
    _failed_pipeline(conn)

    notify.sweep(
        conn, since=NOW - timedelta(hours=24), now=NOW, incidents_on=False,
        client=Recorder(),
    )

    row = conn.execute("select * from notifications").fetchone()
    assert row["error"] == "no matching route"
    assert row["destinations"] == []


def test_the_ledger_records_where_it_went(conn, slack_bot):
    _failed_pipeline(conn)
    notify.sweep(
        conn, since=NOW - timedelta(hours=24), now=NOW, incidents_on=False,
        client=slack_bot,
    )
    row = conn.execute("select * from notifications").fetchone()
    assert row["delivered"] is True
    assert row["destinations"] == ["#data"]
    assert row["event"] == "run_failure"


def test_a_claim_is_granted_while_retries_remain_and_refused_after(conn):
    """The upsert is the whole dedup decision, which is what makes two
    overlapping cron runs safe: exactly one of them wins each claim."""
    note = notify.Notification(
        event="run_failure", status="failed", title="t", summary="s", dedup_key="k"
    )
    granted = [notify.claim(conn, note) for _ in range(notify.MAX_DELIVERY_ATTEMPTS + 2)]
    assert granted == [True] * notify.MAX_DELIVERY_ATTEMPTS + [False, False]


def test_a_claim_is_refused_once_the_notification_was_delivered(conn):
    note = notify.Notification(
        event="run_failure", status="failed", title="t", summary="s", dedup_key="k"
    )
    assert notify.claim(conn, note) is True
    notify.record(conn, note, destinations=["#data"], delivered=True, error=None)
    assert notify.claim(conn, note) is False


# ----------------------------------------------------------------------- digest


def test_the_quiet_digest_is_still_a_message(conn):
    """A channel that only speaks when something is wrong gives nobody a way to
    tell "quiet" from "broken and silent"."""
    note = notify.digest(conn, hours=24, now=NOW)
    assert note.event == "digest"
    assert note.status == "ok"
    assert "Nothing is currently broken" in note.summary


def test_the_digest_counts_failures_the_way_the_alerts_do(conn):
    """A digest saying "0 failures" on a morning when an alert went out is how a
    team learns to trust neither."""
    _failed_pipeline(conn)
    note = notify.digest(conn, hours=24, now=NOW)
    assert dict(note.fields)["failures"] == "1", "one pipeline, not three run rows"
    assert note.status == "attention"


def test_the_digest_is_sent_once_an_hour(conn, slack_bot):
    note = notify.digest(conn, hours=24, now=NOW)
    assert notify.send(conn, [note], client=slack_bot)["sent"]
    assert notify.send(conn, [note], client=slack_bot)["sent"] == []


def test_a_digest_can_be_forced(conn, slack_bot):
    note = notify.digest(conn, hours=24, now=NOW)
    notify.send(conn, [note], client=slack_bot)
    assert notify.send(conn, [note], client=slack_bot, force=True)["sent"]


# -------------------------------------------------------------------- incidents


def _dataset(conn, name) -> int:
    return conn.execute(
        "insert into datasets (namespace, name) values ('file', %s) "
        "on conflict (namespace, name) do update set updated_at = now() returning id",
        (f"/warehouse/{name}",),
    ).fetchone()["id"]


def _model(conn, name):
    run = _run(conn, f"build_{name}", state="COMPLETED", integration="SPARK")
    conn.execute(
        "insert into run_datasets (run_id, dataset_id, direction) values (%s, %s, 'OUTPUT')",
        (run, _dataset(conn, name)),
    )


def _breaching_monitor(conn, name, target):
    source = f"{name}.yml"
    monitors.apply_specs(
        conn,
        [monitors.MonitorSpec(name, "freshness", "dataset", target,
                              {"max_age_minutes": 60}, source=source)],
        sources=[source],
    )
    monitor = monitors.get_monitor(conn, name)
    conn.execute(
        "insert into monitor_results (monitor_id, evaluated_at, status, message, "
        "transitioned, context) values (%s, %s, 'breach', 'stale', true, '{}'::jsonb)",
        (monitor["id"], NOW),
    )
    conn.execute(
        "update monitors set last_status = 'breach', last_evaluated_at = %s where id = %s",
        (NOW, monitor["id"]),
    )
    return monitor


def test_an_incident_is_notified_once_however_long_it_stays_open(conn, slack_bot):
    """The dedup key is the incident id, and `detect(persist=True)` is what keeps
    that id stable across sweeps — so a breach persisting for six hours is one
    message, not six."""
    _model(conn, "fct_orders")
    identity.resolve(conn)
    lineage.resolve(conn)
    _breaching_monitor(conn, "fct_orders_freshness", "fct_orders")

    first = notify.sweep(
        conn, since=NOW - timedelta(hours=24), now=NOW, run_failures_on=False,
        client=slack_bot,
    )
    second = notify.sweep(
        conn, since=NOW - timedelta(hours=24), now=NOW, run_failures_on=False,
        client=slack_bot,
    )

    assert len(first["sent"]) == 1
    assert second["sent"] == []
    assert len(slack_bot.calls) == 1


def test_an_incident_notification_carries_the_blast_radius(conn):
    _model(conn, "fct_orders")
    identity.resolve(conn)
    lineage.resolve(conn)
    _breaching_monitor(conn, "fct_orders_freshness", "fct_orders")

    note = notify.incident_notifications(conn, now=NOW)[0]

    assert note.event == "incident"
    assert note.monitor == "fct_orders_freshness"
    assert dict(note.fields)["cause"] == "fct_orders_freshness"


def test_a_broken_graph_does_not_lose_the_run_failures(conn, slack_bot, monkeypatch):
    """Incident grouping is a nicety; a failed pipeline is the alert. One must
    not be able to take the other down."""
    monkeypatch.setattr(
        incidents, "detect", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    _failed_pipeline(conn)

    summary = notify.sweep(
        conn, since=NOW - timedelta(hours=24), now=NOW, client=slack_bot
    )

    assert len(summary["sent"]) == 1


def test_two_events_may_share_a_dedup_key(conn, slack_bot):
    """A dedup key is only unique within an event — incident 1 and a run failure
    could both be "1" — so the ledger and the delivery bookkeeping both key on
    the pair."""
    a = notify.Notification(
        event="incident", status="open", title="a", summary="s", dedup_key="1"
    )
    b = notify.Notification(
        event="run_failure", status="failed", title="b", summary="s", dedup_key="1"
    )

    summary = notify.send(conn, [a, b], client=slack_bot)

    assert len(summary["sent"]) == 2
    rows = conn.execute("select event from notifications order by event").fetchall()
    assert [r["event"] for r in rows] == ["incident", "run_failure"]


# ------------------------------------------------------------------ retry bounds


def test_a_transient_failure_is_retried_on_the_next_sweep(conn, monkeypatch):
    """The trade migration 020 made looked worse against a real Slack than it did
    on paper: one 500 permanently ate a page about a failed pipeline."""
    monkeypatch.setenv(slack.BOT_TOKEN_ENV, "xoxb-test")
    monkeypatch.setenv(slack.CHANNEL_ENV, "#data")
    _failed_pipeline(conn)
    notes = notify.run_failures(conn, since=NOW - timedelta(hours=24), now=NOW)

    down = Recorder(status=500)
    assert notify.send(conn, notes, client=down, now=NOW)["failed"]

    back = Recorder()
    assert notify.send(conn, notes, client=back, now=NOW)["sent"]
    assert len(back.calls) == 1

    row = conn.execute("select * from notifications").fetchone()
    assert row["delivered"] is True
    assert row["attempts"] == 2
    assert row["error"] is None, "the recovered attempt clears the stale error"


def test_a_delivered_notification_is_never_resent(conn, slack_bot):
    """The retry must not reopen the dedup rule it sits inside."""
    _failed_pipeline(conn)
    notes = notify.run_failures(conn, since=NOW - timedelta(hours=24), now=NOW)

    notify.send(conn, notes, client=slack_bot, now=NOW)
    for _ in range(3):
        assert notify.send(conn, notes, client=slack_bot, now=NOW)["sent"] == []
    assert len(slack_bot.calls) == 1


def test_retrying_stops_after_a_few_attempts(conn, monkeypatch):
    """A channel misconfigured for a week must not replay the same message on
    every sweep — that is the alert fatigue the ledger exists to prevent."""
    monkeypatch.setenv(slack.BOT_TOKEN_ENV, "xoxb-test")
    monkeypatch.setenv(slack.CHANNEL_ENV, "#data")
    _failed_pipeline(conn)
    notes = notify.run_failures(conn, since=NOW - timedelta(hours=24), now=NOW)

    down = Recorder(status=500)
    for _ in range(6):
        notify.send(conn, notes, client=down, now=NOW)

    assert len(down.calls) == notify.MAX_DELIVERY_ATTEMPTS
    assert conn.execute("select attempts from notifications").fetchone()["attempts"] == (
        notify.MAX_DELIVERY_ATTEMPTS
    )


def test_a_stale_notification_is_not_retried(conn, monkeypatch):
    """Past the window it is history rather than news, and an alert about a
    pipeline that failed and was fixed hours ago teaches people to ignore the
    channel."""
    monkeypatch.setenv(slack.BOT_TOKEN_ENV, "xoxb-test")
    monkeypatch.setenv(slack.CHANNEL_ENV, "#data")
    _failed_pipeline(conn)
    notes = notify.run_failures(conn, since=NOW - timedelta(hours=24), now=NOW)

    notify.send(conn, notes, client=Recorder(status=500), now=NOW)

    later = NOW + timedelta(hours=notify.RETRY_WINDOW_HOURS + 1)
    client = Recorder()
    assert notify.send(conn, notes, client=client, now=later)["sent"] == []
    assert client.calls == []


def test_a_run_failure_carries_every_integration_that_failed(conn):
    """Airflow orchestrated it, dbt modelled it, Spark executed it — and all
    three reported a failure. Routing needs all three, not the deepest."""
    _failed_pipeline(conn)
    note = notify.run_failures(conn, since=NOW - timedelta(hours=24), now=NOW)[0]
    assert set(note.integrations) == {"AIRFLOW", "SPARK"}


# ------------------------------------------- links out to what actually broke

AIRFLOW_BASE = "https://airflow.example.com"
SPARK_UI = "https://spark.example.com/proxy/application_1"


def test_a_failure_links_both_what_scheduled_it_and_what_broke(conn, monkeypatch):
    """The two ends of the tree link to different useful places.

    The root is what *scheduled* the work — the Airflow DAG run. The deepest
    failure is what *broke* — the Spark UI with the stage that threw. A reader
    wants whichever is nearer their question, so the message offers both.
    """
    monkeypatch.setenv("DATASPINE_AIRFLOW_BASE_URL", AIRFLOW_BASE)
    root = _failed_pipeline(conn)
    conn.execute(
        "update runs set facets = %s where run_id = %s",
        (
            json.dumps({"airflow": {
                "dag": {"dag_id": "analytics_daily"},
                "dagRun": {"run_id": "manual__2026-09-05T11:00:00+00:00"},
            }}),
            root,
        ),
    )
    conn.execute(
        "update runs set facets = %s where depth = 2",
        (json.dumps({"spark_applicationDetails": {"uiWebUrl": SPARK_UI}}),),
    )

    note = notify.run_failures(conn, now=NOW)[0]
    labels = dict((label, url) for label, url in note.links)
    assert labels["Spark UI"] == SPARK_UI
    assert labels["Airflow"].startswith(f"{AIRFLOW_BASE}/dags/analytics_daily/runs/")


def test_a_failure_with_nothing_to_link_says_nothing(conn):
    """No guessed URLs, which is the rule the whole of `links.py` follows."""
    _failed_pipeline(conn)
    assert notify.run_failures(conn, now=NOW)[0].links == ()


def test_links_are_deduplicated(conn, monkeypatch):
    """A tree whose root and deepest failure are the same producer must not
    offer the same URL twice."""
    monkeypatch.setenv("DATASPINE_SPARK_HISTORY_URL", "https://history.example.com")
    root = _run(conn, "spark_only", integration="SPARK")
    conn.execute(
        "update runs set facets = %s where run_id = %s",
        (json.dumps({"spark_applicationDetails": {"uiWebUrl": SPARK_UI}}), root),
    )
    note = notify.run_failures(conn, now=NOW)[0]
    assert len([url for _, url in note.links]) == len({url for _, url in note.links})
