"""One message per dbt job, its failures in the thread.

Most teams run several dbt jobs against one project — a build, an hourly
incremental, a test-only pass — so the unit a human thinks in is the *job*, and
the channel should show one line per job that had trouble. The detail belongs
underneath it, one reply per failure, because a reply is something a person can
react to, quote or answer under. That is the difference between an alert that
gets read and one that gets picked up.

The tests that matter are about what does *not* happen: a clean job saying
nothing, one failure producing one story rather than three, and a wide `dbt
build` not spending a minute of Slack rate limit on a single thread.
"""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path

import httpx
import pytest

from dataspine import dbt_artifacts as dbt
from dataspine import notify, slack

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="module")
def artifacts() -> tuple[dict, dict]:
    return (
        json.loads((FIXTURES / "dbt_run_results_1.12.3.json").read_text()),
        json.loads((FIXTURES / "dbt_manifest_1.12.3.json").read_text()),
    )


@pytest.fixture()
def invocation(artifacts):
    return dbt.parse(*artifacts)


class Slack:
    """Records parents and thread replies separately, as Slack sees them."""

    def __init__(self):
        self.posts: list[dict] = []
        self._ts = 1788600000.0

    def post(self, url, json=None, headers=None, **kwargs):
        self.posts.append(json)
        self._ts += 1
        return httpx.Response(
            200,
            json={"ok": True, "channel": "C0DBT", "ts": f"{self._ts:.6f}"},
            request=httpx.Request("POST", url),
        )

    @property
    def parents(self):
        return [p for p in self.posts if "thread_ts" not in p]

    @property
    def replies(self):
        return [p for p in self.posts if "thread_ts" in p]


@pytest.fixture()
def bot(monkeypatch):
    monkeypatch.setenv(slack.BOT_TOKEN_ENV, "xoxb-test")
    monkeypatch.setenv(slack.CHANNEL_ENV, "#data-alerts")
    return Slack()


# ------------------------------------------------------------------- the shape


def test_a_job_with_failures_gets_one_parent_and_a_thread(invocation):
    note = notify.dbt_job_notification(invocation, job_name="Nightly Production")

    assert note.event == "dbt_job"
    assert note.title.startswith("❌ dbt · Nightly Production — 1 failure")
    assert len(note.thread) == 2, "the failing test and the warning"


def test_a_clean_job_says_nothing(artifacts):
    """A green line from each of several jobs is a channel nobody reads by the
    second week. The live feed is where "everything ran" belongs."""
    run_results, manifest = artifacts
    clean = json.loads(json.dumps(run_results))
    for result in clean["results"]:
        result["status"] = "pass" if result["unique_id"].startswith("test.") else "success"
        result["failures"] = 0

    assert notify.dbt_job_notification(dbt.parse(clean, manifest)) is None


def test_warnings_are_in_the_thread_but_not_in_the_count(invocation):
    """Their authors said in writing they were not worth waking anyone, so they
    must not inflate the number that decides whether someone opens this at 3am."""
    note = notify.dbt_job_notification(invocation)

    assert "1 failure" in note.title
    assert "1 warning(s)" in note.summary
    warned = [c for c in note.thread if c.status == "warn"]
    assert [c.title for c in warned] == ["⚠️  test  unique_fct_order_items_item_id"]


def test_a_thread_reply_carries_the_detail_you_would_go_looking_for(invocation):
    note = notify.dbt_job_notification(invocation)
    failing = next(c for c in note.thread if c.status == "failed")

    assert "not_null_fct_order_items_order_id" in failing.title
    assert "1* failing row(s)" in failing.summary
    assert "order_id" in failing.summary
    assert failing.dataset == "postgres.analytics_marts.fct_order_items"


def test_the_summary_says_which_command_ran(invocation):
    """"3 failures" reads very differently for a test-only job than a build."""
    note = notify.dbt_job_notification(invocation)
    assert "dbt build" in note.summary
    assert dict(note.fields)["command"] == "build"


def test_models_come_before_tests_in_the_thread(artifacts):
    """A model that did not build is why its tests did not run. Reading the other
    way round means scrolling past symptoms to find the cause."""
    run_results, manifest = artifacts
    broken = json.loads(json.dumps(run_results))
    for result in broken["results"]:
        if result["unique_id"].startswith("model."):
            result["status"] = "error"
            result["message"] = "Database Error: column does not exist"

    note = notify.dbt_job_notification(dbt.parse(broken, manifest))

    kinds = [c.title.split()[1] for c in note.thread]
    assert kinds[0] == "model", "the cause first"


# ------------------------------------------------------------------- delivery


def test_the_parent_posts_once_and_each_failure_replies_to_it(conn, invocation, bot):
    notify.announce_dbt_job(conn, invocation, job_name="Nightly Production", client=bot)

    assert len(bot.parents) == 1, "one line in the channel"
    assert len(bot.replies) == 2, "one reply per failure, reactable on its own"
    assert {r["thread_ts"] for r in bot.replies} == {bot.parents[0] and "1788600001.000000"}


def test_a_thread_reply_carries_no_header_block(conn, invocation, bot):
    """A header in a thread reply is a headline where a line will do."""
    notify.announce_dbt_job(conn, invocation, client=bot)

    assert not any(
        b["type"] == "header" for r in bot.replies for b in r["blocks"]
    )
    assert any(b["type"] == "header" for b in bot.parents[0]["blocks"])


def test_the_same_run_is_announced_once(conn, invocation, bot):
    """The webhook and the backstop poll will both see it."""
    notify.announce_dbt_job(conn, invocation, client=bot)
    notify.announce_dbt_job(conn, invocation, client=bot)

    assert len(bot.parents) == 1


def test_a_wide_failure_does_not_spend_a_minute_of_rate_limit(conn, artifacts, bot):
    """Slack allows roughly one message per second per channel, so sixty replies
    is a minute that a different alert then queues behind."""
    run_results, manifest = artifacts
    wide = json.loads(json.dumps(run_results))
    template = next(r for r in wide["results"] if r["unique_id"].startswith("test."))
    wide["results"] = [
        dict(template, unique_id=f"test.analytics.check_{i}.abc{i}", status="fail",
             failures=i + 1)
        for i in range(60)
    ]
    invocation = dbt.parse(wide, manifest)

    notify.announce_dbt_job(conn, invocation, client=bot)

    assert len(bot.replies) == slack.MAX_THREAD_REPLIES + 1, "capped, plus the overflow line"
    assert "40 more" in json.dumps(bot.replies[-1])


def test_a_failed_reply_does_not_invalidate_the_parent(conn, invocation, monkeypatch, bot):
    """A summary that arrived is worth more than a thread that did not."""
    calls = {"n": 0}
    real = bot.post

    def flaky(url, json=None, headers=None, **kwargs):
        calls["n"] += 1
        if json and "thread_ts" in json:
            raise httpx.ConnectError("boom")
        return real(url, json=json, headers=headers, **kwargs)

    monkeypatch.setattr(bot, "post", flaky)
    result = notify.announce_dbt_job(conn, invocation, client=bot)

    assert result["sent"], "the parent still counts as delivered"


# ---------------------------------------------------------------- one story


def test_a_dbt_failure_does_not_also_raise_a_run_failure(conn, invocation, bot):
    """Two notifications for one event is how a reader learns the second one is
    never worth opening."""
    from datetime import timedelta

    from dataspine.events import RunEvent
    from dataspine.ingest import ingest_run_event

    for event in dbt.events(invocation, job_name="Nightly Production"):
        ingest_run_event(conn, RunEvent.model_validate(event))
    notify.announce_dbt_job(conn, invocation, job_name="Nightly Production", client=bot)

    failures = notify.run_failures(
        conn, since=invocation.started_at - timedelta(days=1),
        now=invocation.generated_at + timedelta(hours=1), grace_minutes=0,
    )

    assert failures == []


def test_a_non_dbt_failure_still_raises_one(conn, invocation, bot):
    """The stand-down is for dbt invocations, not for pipelines generally."""
    from datetime import timedelta
    from uuid import uuid4

    job = conn.execute(
        "insert into jobs (namespace, name, integration) values ('t','spark_job','SPARK') "
        "returning id"
    ).fetchone()["id"]
    run = uuid4()
    conn.execute(
        "insert into runs (run_id, job_id, root_run_id, state, started_at, ended_at) "
        "values (%s, %s, %s, 'FAILED', %s, %s)",
        (run, job, run, invocation.started_at, invocation.generated_at),
    )

    failures = notify.run_failures(
        conn, since=invocation.started_at - timedelta(days=1),
        now=invocation.generated_at + timedelta(hours=1), grace_minutes=0,
    )

    assert [f.job for f in failures] == ["spark_job"]


# ----------------------------------------------------------------- many jobs


def test_two_jobs_in_one_project_are_two_jobs(conn, artifacts):
    """The reason the job name is fetched at all. Without it a build and an
    hourly incremental are one job here, with their durations averaged together
    and no way for a route to tell them apart."""
    from dataspine.events import RunEvent
    from dataspine.ingest import ingest_run_event

    run_results, manifest = artifacts
    for index, name in enumerate(("Nightly Production", "Hourly Incremental")):
        variant = json.loads(json.dumps(run_results))
        variant["metadata"]["invocation_id"] = f"0000000{index}-0000-0000-0000-00000000000{index}"
        for event in dbt.events(dbt.parse(variant, manifest), job_name=name):
            ingest_run_event(conn, RunEvent.model_validate(event))

    names = {
        r["job_name"] for r in conn.execute(
            "select job_name from run_summary where depth = 0"
        ).fetchall()
    }
    assert names == {"analytics.Nightly Production", "analytics.Hourly Incremental"}


def test_a_route_can_send_one_job_somewhere_else(invocation, tmp_path):
    routes = tmp_path / "slack.yml"
    routes.write_text(
        "routes:\n"
        "  - match: {event: dbt_job, job: '*Finance*'}\n    channel: '#finance-data'\n"
        "  - match: {event: dbt_job}\n    channel: '#data-alerts'\n"
    )
    loaded = slack.load_routes(routes)

    finance = notify.dbt_job_notification(invocation, job_name="Finance Nightly")
    other = notify.dbt_job_notification(invocation, job_name="Nightly Production")

    assert slack.destinations(finance, loaded) == ("#finance-data",)
    assert slack.destinations(other, loaded) == ("#data-alerts",)


# ------------------------------------------------- links out to where it ran

# A real dbt Cloud run against Snowflake, captured from the Admin API. It is
# the fixture that carries what the Postgres one cannot: `adapter_response`
# holds a warehouse `query_id`, and the relations are Snowflake's.
SNOWFLAKE_ARTIFACTS = (
    "dbt_cloud_snowflake_run_results.json",
    "dbt_cloud_snowflake_manifest.json",
)


@pytest.fixture(scope="module")
def snowflake_invocation() -> dbt.Invocation:
    return dbt.parse(
        *(json.loads((FIXTURES / name).read_text()) for name in SNOWFLAKE_ARTIFACTS)
    )


@pytest.fixture()
def snowsight(monkeypatch):
    from dataspine import links

    monkeypatch.setenv(
        links.SNOWFLAKE_ACCOUNT_ENV, "https://app.snowflake.com/myorg/ab12345"
    )


def test_a_failing_test_carries_the_query_the_warehouse_ran(snowflake_invocation):
    """dbt records the warehouse statement id, and it is the whole feature.

    It means the alert can link the query that found the bad rows without
    dataspine holding a warehouse credential or writing anything into the
    customer's account.
    """
    failing = next(n for n in snowflake_invocation.failed if n.is_test and not n.warn_only)
    assert failing.query_id
    assert failing.compiled_code
    # The compiled test is the investigation query, fully qualified.
    assert "ANALYTICS_DB.PUBLIC" in failing.compiled_code


def test_compiled_sql_loses_dbts_templating_blank_lines(snowflake_invocation):
    """A rendered test opens with several empty lines where the `{% test %}`
    block was, which would bury the query below the fold in Slack."""
    failing = next(n for n in snowflake_invocation.failed if n.is_test)
    assert not failing.compiled_code.startswith("\n")
    assert "\n\n" not in failing.compiled_code


def test_postgres_run_has_no_query_id_and_that_is_fine(invocation):
    """Only some adapters report one. Absent is ordinary, not a fault: the
    alert simply carries one link fewer."""
    assert all(node.query_id is None for node in invocation.nodes)
    note = notify.dbt_job_notification(invocation, job_name="Nightly")
    assert all(label != "Snowflake query" for label, _ in note.thread[0].links)


def test_the_thread_reply_links_only_the_failing_query(
    snowflake_invocation, snowsight
):
    """Keep the investigation link without an additional table link."""
    note = notify.dbt_job_notification(snowflake_invocation, job_name="Nightly Build")
    reply = next(n for n in note.thread if "not_null" in n.title)
    labels = [label for label, _ in reply.links]
    assert labels[0] == "Snowflake query"
    assert labels == ["Snowflake query"]

    _, blocks = slack.render([reply], header=False)
    context = next(b for b in blocks if b["type"] == "context")["elements"][0]["text"]
    assert "/#/compute/history/queries/" in context
    assert "/table/" not in context


def test_the_compiled_query_stays_out_of_slack(snowflake_invocation):
    """Investigation SQL belongs in the linked editor, not the alert."""
    note = notify.dbt_job_notification(snowflake_invocation, job_name="Nightly Build")
    reply = next(n for n in note.thread if "not_null" in n.title)
    assert "where order_id is null" not in reply.summary


def test_a_monstrous_query_cannot_take_the_alert_down(snowflake_invocation):
    """Slack rejects an oversized block with a 400 for the *whole* message, so
    a generated `dbt_utils` test must not be able to lose the alert with it."""
    huge = "\n".join(f"select {i} union all" for i in range(500))
    node = snowflake_invocation.failed[0]
    swollen = dataclasses.replace(node, compiled_code=huge, status="fail")
    inv = dataclasses.replace(
        snowflake_invocation, nodes=(swollen, *snowflake_invocation.nodes[1:])
    )
    _, blocks = slack.render([notify.dbt_job_notification(inv).thread[0]], header=False)
    assert all(len(json.dumps(block)) < slack.SECTION_LIMIT for block in blocks)


def test_the_job_message_links_the_dbt_cloud_run(snowflake_invocation, monkeypatch):
    """The parent says where the job ran; the replies say what broke."""
    from dataspine import dbt_cloud

    monkeypatch.setenv(notify.BASE_URL_ENV, "https://dataspine.example.com")

    facets = dbt_cloud.run_facet(
        55555555555551,
        {
            "href": (
                "https://abc123.us1.dbt.com/deploy/11111111111110"
                "/projects/22222222222220/runs/55555555555551/"
            ),
            "job_definition_id": 33333333333330,
            "project_id": 22222222222220,
        },
    )
    note = notify.dbt_job_notification(
        snowflake_invocation, job_name="Nightly Build", run_facets=facets
    )
    assert ("dbt Cloud", facets["dbt_cloud"]["href"]) in note.links

    _, blocks = slack.render([note])
    context = next(b for b in blocks if b["type"] == "context")["elements"][0]["text"]
    assert "|dbt Cloud>" in context
    # dataspine's own link comes last: it is the one that always exists, so
    # leading with it would put the same words at the front of every message.
    assert context.index("|dbt Cloud>") < context.index("open in wish:d")
