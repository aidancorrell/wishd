"""The live pipeline feed: one message per execution, edited as it runs.

A live feed is the classic way to get a Slack channel muted, which is the failure
this whole project keeps refusing. The answer is that the message is *mutated*,
not repeated — so the tests that matter are the ones about restraint:

  **One message per execution**, however many steps it has.
  **No update when nothing changed**, or every sweep rewrites the same text and
  burns the rate limit that a genuine update will need a minute later.
  **A finished pipeline is closed** and never touched again.
  **A webhook refuses**, rather than degrading into one message per step.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest

from dataspine import notify, slack

NOW = datetime(2026, 9, 4, 12, 0, tzinfo=UTC)


class Slack:
    """Records posts and edits, and answers the way the Web API does."""

    def __init__(self, status=200, ok=True):
        self.posts: list[dict] = []
        self.updates: list[dict] = []
        self.status = status
        self.ok = ok
        self._ts = 1700000000.0

    def post(self, url, json=None, headers=None, **kwargs):
        body = {"ok": self.ok}
        if not self.ok:
            body["error"] = "channel_not_found"
        if url == slack.POST_MESSAGE_URL:
            self.posts.append(json)
            self._ts += 1
            body |= {"channel": "C0LIVE", "ts": f"{self._ts:.6f}"}
        else:
            self.updates.append(json)
        return httpx.Response(
            self.status, json=body, request=httpx.Request("POST", url)
        )


@pytest.fixture()
def bot(monkeypatch):
    monkeypatch.setenv(slack.BOT_TOKEN_ENV, "xoxb-test")
    monkeypatch.setenv(slack.CHANNEL_ENV, "#pipelines")
    return Slack()


def _job(conn, name, integration="AIRFLOW") -> int:
    return conn.execute(
        "insert into jobs (namespace, name, integration) values ('t', %s, %s) "
        "on conflict (namespace, name) do update set integration = excluded.integration "
        "returning id",
        (name, integration),
    ).fetchone()["id"]


def _pipeline(conn, *, steps_done=1, steps_running=1, failed=0, started=None,
              last_event=None, name="analytics_daily"):
    """A root plus its children, in whatever mix of states the test needs."""
    started = started or NOW - timedelta(minutes=5)
    last_event = last_event or started
    root = uuid4()
    conn.execute(
        "insert into runs (run_id, job_id, root_run_id, depth, state, started_at, "
        "last_event_at) values (%s, %s, %s, 0, 'RUNNING', %s, %s)",
        (root, _job(conn, name), root, started, last_event),
    )
    for index in range(steps_done):
        state = "FAILED" if index < failed else "COMPLETED"
        conn.execute(
            "insert into runs (run_id, job_id, parent_run_id, root_run_id, depth, state, "
            "started_at, ended_at, last_event_at) "
            "values (%s, %s, %s, %s, 1, %s, %s, %s, %s)",
            (uuid4(), _job(conn, f"{name}.step_{index}", "DBT"), root, root, state,
             started, started + timedelta(minutes=1), last_event),
        )
    for index in range(steps_running):
        conn.execute(
            "insert into runs (run_id, job_id, parent_run_id, root_run_id, depth, state, "
            "started_at, last_event_at) "
            "values (%s, %s, %s, %s, 1, 'RUNNING', %s, %s)",
            (uuid4(), _job(conn, f"{name}.live_{index}", "SPARK"), root, root,
             started, last_event),
        )
    return root


def _finish(conn, root, *, state="COMPLETED"):
    """Every run in the tree reaches a terminal state, as it would in life."""
    conn.execute(
        "update runs set state = %s, ended_at = %s where state = 'RUNNING' "
        "and (run_id = %s or root_run_id = %s)",
        (state, NOW, root, root),
    )


# ------------------------------------------------------------------- rendering


def test_a_running_pipeline_reports_progress_not_a_forecast(conn):
    """OpenLineage has no "about to run" event, so the tree grows as it executes.
    A percentage would be a forecast, and it would be wrong."""
    _pipeline(conn, steps_done=3, steps_running=2)
    note = notify.pipeline_feed(conn, now=NOW)[0]

    assert note.event == "pipeline"
    assert note.status == "running"
    assert "3 of 6" in note.summary and "so far" in note.summary
    assert "%" not in note.summary


def test_a_finished_pipeline_reports_how_it_ended(conn):
    root = _pipeline(conn, steps_done=2, steps_running=1)
    _finish(conn, root)
    note = notify.pipeline_feed(conn, now=NOW, tracking={str(root)})[0]

    assert note.status == "completed"
    assert notify.is_terminal(note)


def test_a_failed_step_makes_the_pipeline_failed(conn):
    root = _pipeline(conn, steps_done=3, steps_running=1, failed=1)
    _finish(conn, root)
    note = notify.pipeline_feed(conn, now=NOW, tracking={str(root)})[0]

    assert note.status == "failed"
    assert "1* step(s) failed" in note.summary


def test_a_silent_pipeline_is_flagged_rather_than_called_healthy(conn):
    """OpenLineage has no heartbeat, so a cluster that dies mid-run leaves its
    runs RUNNING forever. Silence is the only evidence there is."""
    _pipeline(
        conn, started=NOW - timedelta(hours=4),
        last_event=NOW - timedelta(hours=3),
    )
    note = notify.pipeline_feed(conn, now=NOW)[0]

    assert note.status == "stalled"
    assert "may be gone" in note.summary
    assert not notify.is_terminal(note), "it may still report in and finish"


def test_the_content_is_stable_while_nothing_happens(conn):
    """An elapsed that re-renders every minute would change the hash every
    minute, and every sweep would call chat.update to move a number by one."""
    _pipeline(conn, steps_done=2, steps_running=1)
    first = notify.pipeline_feed(conn, now=NOW)[0]
    later = notify.pipeline_feed(conn, now=NOW + timedelta(minutes=7))[0]

    assert notify._content_hash(first) == notify._content_hash(later)


def test_progress_changes_the_content(conn):
    root = _pipeline(conn, steps_done=1, steps_running=2)
    before = notify.pipeline_feed(conn, now=NOW)[0]
    conn.execute(
        "update runs set state = 'COMPLETED', ended_at = %s where root_run_id = %s "
        "and state = 'RUNNING' and depth = 1",
        (NOW, root),
    )
    after = notify.pipeline_feed(conn, now=NOW)[0]

    assert notify._content_hash(before) != notify._content_hash(after)


# ---------------------------------------------------------------------- track


def test_one_pipeline_is_one_message_however_many_steps(conn, bot):
    root = _pipeline(conn, steps_done=1, steps_running=3)

    assert notify.track(conn, now=NOW, client=bot)["posted"] == [str(root)]
    for _ in range(3):
        conn.execute(
            "update runs set state = 'COMPLETED', ended_at = %s where run_id = ("
            "  select run_id from runs where root_run_id = %s and state = 'RUNNING'"
            "  and depth = 1 limit 1)",
            (NOW, root),
        )
        notify.track(conn, now=NOW, client=bot)

    assert len(bot.posts) == 1, "one message for the whole execution"
    assert len(bot.updates) == 3, "edited as it advanced"


def test_an_unchanged_sweep_issues_no_update_at_all(conn, bot):
    """Rewriting identical text burns the rate limit that a genuine update will
    need a minute later."""
    _pipeline(conn, steps_done=2, steps_running=1)
    notify.track(conn, now=NOW, client=bot)

    result = notify.track(conn, now=NOW + timedelta(minutes=1), client=bot)

    assert bot.updates == []
    assert len(result["unchanged"]) == 1


def test_the_message_is_edited_to_its_final_state_and_then_closed(conn, bot):
    root = _pipeline(conn, steps_done=2, steps_running=1)
    notify.track(conn, now=NOW, client=bot)
    _finish(conn, root)

    result = notify.track(conn, now=NOW, client=bot)

    assert result["closed"] == [str(root)]
    assert "completed" in json.dumps(bot.updates[-1])
    row = conn.execute("select live from notifications").fetchone()
    assert row["live"] is False


def test_a_closed_pipeline_is_never_touched_again(conn, bot):
    """Or a months-old message silently rewrites itself when a run id recurs."""
    root = _pipeline(conn, steps_done=2, steps_running=1)
    notify.track(conn, now=NOW, client=bot)
    _finish(conn, root)
    notify.track(conn, now=NOW, client=bot)
    before = len(bot.updates)

    for _ in range(3):
        notify.track(conn, now=NOW, client=bot)

    assert len(bot.updates) == before
    assert len(bot.posts) == 1


def test_a_pipeline_that_began_and_ended_between_sweeps_is_one_message(conn, bot):
    root = _pipeline(conn, steps_done=3, steps_running=1)
    _finish(conn, root)

    result = notify.track(conn, now=NOW, client=bot)

    assert result["posted"] == [str(root)]
    assert len(bot.posts) == 1
    assert bot.updates == []
    assert conn.execute("select live from notifications").fetchone()["live"] is False


def test_the_message_address_is_stored_so_it_can_be_edited(conn, bot):
    """`chat.update` needs the channel *id* and the ts, and postMessage's reply is
    the only place that address ever exists."""
    _pipeline(conn)
    notify.track(conn, now=NOW, client=bot)

    stored = conn.execute("select messages from notifications").fetchone()["messages"]
    assert stored[0]["channel"] == "C0LIVE"
    assert stored[0]["ts"].startswith("17")
    assert stored[0]["route"] == "#pipelines"


def test_a_webhook_refuses_rather_than_flooding(conn, monkeypatch):
    """Degrading would mean one message per step, which is the flood the feed
    exists to prevent."""
    monkeypatch.delenv(slack.BOT_TOKEN_ENV, raising=False)
    monkeypatch.setenv(slack.WEBHOOK_ENV, "https://hooks.slack.com/services/x")
    _pipeline(conn)

    result = notify.track(conn, now=NOW)

    assert result["skipped"] and slack.BOT_TOKEN_ENV in result["skipped"]
    assert result["posted"] == []
    assert conn.execute("select count(*) as n from notifications").fetchone()["n"] == 0


def test_a_failed_post_is_retried_rather_than_leaving_a_dead_message(conn, monkeypatch):
    monkeypatch.setenv(slack.BOT_TOKEN_ENV, "xoxb-test")
    monkeypatch.setenv(slack.CHANNEL_ENV, "#pipelines")
    _pipeline(conn)

    down = Slack(ok=False)
    assert notify.track(conn, now=NOW, client=down)["failed"]
    row = conn.execute("select live, delivered from notifications").fetchone()
    assert row["live"] is False, "nothing exists to edit"
    assert row["delivered"] is False

    back = Slack()
    assert notify.track(conn, now=NOW, client=back)["posted"]
    assert len(back.posts) == 1


def test_routing_can_send_the_feed_to_its_own_channel(conn, bot, tmp_path, monkeypatch):
    path = tmp_path / "slack.yml"
    path.write_text(
        "routes:\n"
        "  - match: {event: pipeline}\n    channel: '#pipelines'\n"
        "  - channel: '#data-alerts'\n"
    )
    monkeypatch.setenv(slack.ROUTES_ENV, str(path))
    _pipeline(conn)

    notify.track(conn, now=NOW, client=bot)

    assert bot.posts[0]["channel"] == "#pipelines"


def test_history_is_not_announced_on_a_first_sweep(conn, bot):
    """A first run against an existing database must not narrate last week."""
    root = _pipeline(
        conn, steps_done=2, steps_running=1,
        started=NOW - timedelta(days=2), last_event=NOW - timedelta(days=2),
    )
    conn.execute(
        "update runs set state = 'COMPLETED', ended_at = %s "
        "where run_id = %s or root_run_id = %s",
        (NOW - timedelta(days=2), root, root),
    )

    assert notify.track(conn, now=NOW, client=bot)["posted"] == []
    assert bot.posts == []


def test_the_ledger_records_what_the_message_now_says(conn, bot):
    """It answers "what did we tell people?", so it must not still read
    `running` for a pipeline that finished hours ago."""
    root = _pipeline(conn, steps_done=2, steps_running=1)
    notify.track(conn, now=NOW, client=bot)
    assert "running" in conn.execute("select title from notifications").fetchone()["title"]

    _finish(conn, root)
    notify.track(conn, now=NOW, client=bot)

    assert "completed" in conn.execute("select title from notifications").fetchone()["title"]


# ------------------------------------------------------- links out of the feed

DBT_CLOUD_HREF = (
    "https://abc123.us1.dbt.com/deploy/11111111111110"
    "/projects/22222222222220/runs/55555555555551/"
)


def _dbt_cloud_pipeline(conn, *, href=DBT_CLOUD_HREF):
    """A dbt Cloud root run, carrying the facet the courier attaches."""
    root = uuid4()
    conn.execute(
        "insert into runs (run_id, job_id, root_run_id, depth, state, started_at, "
        "last_event_at, facets) values (%s, %s, %s, 0, 'RUNNING', %s, %s, %s)",
        (
            root,
            _job(conn, "aidanAX.Nightly Build", "DBT"),
            root,
            NOW - timedelta(minutes=3),
            NOW - timedelta(minutes=1),
            json.dumps({"dbt_cloud": {"href": href, "runId": "55555555555551"}}),
        ),
    )
    return root


def test_the_feed_links_to_where_the_pipeline_is_running(conn):
    """A pipeline someone is watching go past is one they may want to open
    where it is actually running."""
    _dbt_cloud_pipeline(conn)
    note = notify.pipeline_feed(conn, now=NOW)[0]
    assert ("dbt Cloud", DBT_CLOUD_HREF) in note.links


def test_the_link_appears_in_the_rendered_message(conn, monkeypatch):
    monkeypatch.setenv(notify.BASE_URL_ENV, "https://dataspine.example.com")
    _dbt_cloud_pipeline(conn)
    note = notify.pipeline_feed(conn, now=NOW)[0]
    _, blocks = slack.render([note])
    context = next(b for b in blocks if b["type"] == "context")["elements"][0]["text"]
    assert f"<{DBT_CLOUD_HREF}|dbt Cloud>" in context
    assert context.index("|dbt Cloud>") < context.index("open in wish:d")


def test_a_pipeline_with_no_facets_simply_says_less(conn):
    """The rule the whole of `links.py` follows: no guessed URLs. A run that
    told us nothing gets our own link and no other."""
    _pipeline(conn)
    note = notify.pipeline_feed(conn, now=NOW)[0]
    assert note.links == ()


def test_becoming_linkable_is_a_content_change(conn, bot):
    """dbt Cloud's `href` arrives with the first event that carries the facet.

    If links were left out of the content hash the feed would decide nothing
    had changed and never edit the message into its most useful state.
    """
    root = _dbt_cloud_pipeline(conn, href=None)
    conn.execute("update runs set facets = '{}'::jsonb where run_id = %s", (root,))
    notify.track(conn, now=NOW, client=bot)
    assert len(bot.posts) == 1

    conn.execute(
        "update runs set facets = %s where run_id = %s",
        (json.dumps({"dbt_cloud": {"href": DBT_CLOUD_HREF}}), root),
    )
    notify.track(conn, now=NOW, client=bot)
    assert len(bot.updates) == 1
    assert DBT_CLOUD_HREF in json.dumps(bot.updates[0])
