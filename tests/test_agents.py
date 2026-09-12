"""Handing a failing check to a coding agent.

The tests that matter here are the ones about *not* offering something. A button
is a promise that a click will land somewhere useful, and the ways this feature
can break that promise are all quiet: an unsigned key that turns the handoff page
into an oracle for anyone who can guess a check name, a button rendered without
a base URL that goes nowhere, a briefing built for a test whose author said in
writing they did not want waking.

The deep-link contract in `claude_cli_url` is asserted against literally, because
it was read out of the handler binary rather than documentation: the host must be
`open`, `repo` must be `owner/repo`, and the prompt rides in `q`. If any of that
drifts, the link silently stops opening and the failure looks like "nothing
happened when I clicked it".
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlparse

import pytest

from dataspine import agents, dbt_artifacts, dq, notify, slack


@pytest.fixture()
def configured(monkeypatch):
    monkeypatch.setenv("DATASPINE_AGENT_SECRET", "test-secret")
    monkeypatch.setenv("DATASPINE_AGENT_TARGETS", "claude-cli,claude-cloud,codex")
    monkeypatch.setenv("DATASPINE_AGENT_CWD", "/srv/analytics")
    monkeypatch.setenv("DATASPINE_AGENT_REPO", "acme/analytics")
    monkeypatch.setenv("DATASPINE_BASE_URL", "https://ds.acme.io")


def briefing(**over):
    base = dict(
        title="fct_orders — not_null_amount failed",
        what="The `not_null_amount` check on `analytics.public.fct_orders` failed.",
        table="analytics.public.fct_orders",
        column="amount",
        check="not_null_amount",
        error="Database Error: column 'amount' contains 412 null values",
        sql="select * from analytics.public.fct_orders where amount is null",
        failures=412,
    )
    base.update(over)
    return agents.Briefing(**base)


# ----------------------------------------------------------------- configuration


def test_no_secret_means_no_buttons(monkeypatch):
    """The secret is not optional hardening; it is what makes the key a key.

    Without it `/handoff` would accept any `(event, dedup_key)` a caller cared to
    construct and hand back the failing SQL behind it.
    """
    monkeypatch.setenv("DATASPINE_AGENT_TARGETS", "claude-cli")
    monkeypatch.delenv("DATASPINE_AGENT_SECRET", raising=False)

    assert agents.configured() is False
    assert agents.handoff_urls("data_test", "k", "https://ds.acme.io") == ()


def test_no_base_url_means_no_buttons(configured, monkeypatch):
    """A relative link is no use in Slack."""
    assert agents.handoff_urls("data_test", "k", None) == ()


def test_unknown_target_is_dropped_not_accepted(monkeypatch):
    monkeypatch.setenv("DATASPINE_AGENT_SECRET", "s")
    monkeypatch.setenv("DATASPINE_AGENT_TARGETS", "claude-cli,codx,codex")
    assert agents.targets() == ("claude-cli", "codex")


def test_malformed_repo_is_absent_rather_than_fatal(configured, monkeypatch):
    """A typo costs the link its `repo`, never the alert it was attached to."""
    monkeypatch.setenv("DATASPINE_AGENT_REPO", "not-a-repo-slug")
    assert agents.repo() is None
    # The link still builds, because `cwd` alone is enough to open somewhere real.
    assert agents.claude_cli_url(briefing()) is not None


def test_no_cwd_and_no_repo_produces_no_link(configured, monkeypatch):
    """The guessed link this module exists to refuse."""
    monkeypatch.delenv("DATASPINE_AGENT_CWD", raising=False)
    monkeypatch.delenv("DATASPINE_AGENT_REPO", raising=False)
    assert agents.claude_cli_url(briefing()) is None


# ------------------------------------------------------------------------ the key


def test_key_round_trips(configured):
    key = agents.sign("data_test", "dbt/a.b.c/nn/2026-09-05T12:00:00+00:00")
    assert agents.unsign(key) == ("data_test", "dbt/a.b.c/nn/2026-09-05T12:00:00+00:00")


def test_tampered_key_does_not_verify(configured):
    key = agents.sign("data_test", "dbt/a.b.c/nn/x")
    body, _, mac = key.rpartition(".")
    forged = agents.sign("data_test", "dbt/other.table/nn/x").split(".")[0]
    assert agents.unsign(f"{forged}.{mac}") is None


def test_key_from_another_secret_does_not_verify(configured, monkeypatch):
    key = agents.sign("data_test", "k")
    monkeypatch.setenv("DATASPINE_AGENT_SECRET", "a-different-secret")
    assert agents.unsign(key) is None


# -------------------------------------------------------------- the deep link


def test_claude_cli_url_matches_the_handler_contract(configured):
    """Asserted literally: this came from the handler, not from documentation."""
    url = agents.claude_cli_url(briefing())
    parsed = urlparse(url)
    assert parsed.scheme == "claude-cli"
    # The parser rejects any other host outright.
    assert parsed.netloc == "open"

    params = parse_qs(parsed.query)
    assert params["cwd"] == ["/srv/analytics"]
    assert params["repo"] == ["acme/analytics"]
    assert "not_null_amount" in params["q"][0]


def test_prompt_survives_url_encoding_intact(configured):
    """The briefing carries SQL, quotes and newlines; none of it may be mangled.

    Verified end to end against the real handler once — it base64s the prompt
    before it reaches a shell — so what this guards is our half: that the value
    we encode is the value that comes back out.
    """
    b = briefing(error="it's broken: `x` <> \"y\" & more", sql="select 'a''b' from t")
    url = agents.claude_cli_url(b)
    recovered = parse_qs(urlparse(url).query)["q"][0]
    assert recovered == agents.prompt(b)
    assert "it's broken" in recovered
    assert "select 'a''b' from t" in recovered


def test_codex_command_escapes_quotes_for_the_shell(configured):
    """Codex takes the briefing as an argument, and SQL is full of apostrophes."""
    command = agents.codex_command(briefing(sql="select * from t where x = 'y'"))
    assert command.startswith("codex exec '")
    # The shell's own idiom for a quote inside a single-quoted string.
    assert "'\\''" in command


def test_codex_app_link_carries_no_prompt(configured):
    """It cannot, and pretending otherwise is what the page text exists to avoid.

    `codex app` accepts a workspace path and nothing else, so this link opens the
    right directory and the command carries the briefing.
    """
    url = agents.codex_app_url()
    assert parse_qs(urlparse(url).query) == {"cwd": ["/srv/analytics"]}


# ------------------------------------------------------------------- the prompt


def test_prompt_says_nothing_about_what_is_missing(configured):
    """An agent told "the SQL is unavailable" wastes a turn looking for it."""
    text = agents.prompt(briefing(sql=None, error=None, column=None))
    assert "SQL" not in text.replace("sql", "")
    assert "unavailable" not in text
    assert "not_null_amount" in text


def test_prompt_leads_with_a_failing_upstream_when_there_is_one(configured):
    """The one thing dataspine knows that the repository does not."""
    text = agents.prompt(briefing(
        upstreams=("stg.orders", "raw.orders"), failing_upstreams=("stg.orders",)
    ))
    assert "Upstream runs that also failed" in text
    assert "likely cause" in text


def test_prompt_does_not_authorise_a_pull_request(configured):
    """An unasked-for PR turns an alert into a code review someone now owes."""
    assert "Do not commit, push, or open a pull request" in agents.prompt(briefing())


def test_long_briefing_is_trimmed_rather_than_truncating_the_url(configured):
    text = agents.prompt(briefing(sql="select 1\n" * 5000))
    assert len(text) <= agents.MAX_PROMPT_CHARS + 40


# ------------------------------------------------------------------ in the alert


def test_buttons_appear_on_a_single_alert(configured):
    note = notify.Notification(
        event="data_test", status="fail", title="t", summary="s", dedup_key="k",
        actions=agents.handoff_urls("data_test", "k", "https://ds.acme.io"),
    )
    _, blocks = slack.render([note])
    actions = [b for b in blocks if b["type"] == "actions"]
    assert len(actions) == 1
    assert [e["text"]["text"] for e in actions[0]["elements"]] == [
        "Claude Code", "Claude Cloud", "Codex",
    ]
    assert all(e["url"].startswith("https://ds.acme.io/handoff/") for e in actions[0]["elements"])


def test_no_buttons_on_a_grouped_message(configured):
    """There is no answer to "which of these twelve would it investigate?"."""
    notes = [
        notify.Notification(
            event="data_test", status="fail", title=f"t{i}", summary="s", dedup_key=f"k{i}",
            actions=agents.handoff_urls("data_test", f"k{i}", "https://ds.acme.io"),
        )
        for i in range(3)
    ]
    _, blocks = slack.render(notes)
    assert not [b for b in blocks if b["type"] == "actions"]


def test_thread_replies_each_get_their_own_buttons(configured):
    """Which is the point of the single-note rule, not an exception to it.

    A dbt run's summary groups and offers nothing; each failing test below it is
    rendered on its own and is individually actionable.
    """
    child = notify.Notification(
        event="dbt_job", status="failed", title="test x", summary="s", dedup_key="root/x",
        actions=agents.handoff_urls("dbt_job", "root/x", "https://ds.acme.io"),
    )
    _, blocks = slack.render([child], header=False)
    assert [b["type"] for b in blocks if b["type"] == "actions"] == ["actions"]


def test_a_warning_is_never_offered_an_agent(configured):
    """`severity: warn` means the author said they did not want waking.

    Offering to put an agent on it is a louder version of the same interruption,
    and it is the one case where the button must be absent from a message that
    otherwise has everything needed to build it.
    """
    warn, error = _test_node(severity="warn"), _test_node(severity="error")
    invocation = _invocation(warn, error)

    assert notify._dbt_node_notification(warn, invocation, "root").actions == ()
    assert notify._dbt_node_notification(error, invocation, "root").actions != ()


# ------------------------------------------------------- rebuilding the briefing


def test_briefing_is_rebuilt_from_the_stored_check(conn, configured):
    """No new storage: the SQL and the error are already in the check details."""
    measured = datetime.now(UTC) - timedelta(minutes=5)
    dq.import_results(conn, source="dbt", rows=[{
        "table": "analytics.public.fct_orders",
        "check": "not_null_amount",
        "status": "fail",
        "value": 412,
        "measured_at": measured,
        "details": {
            "message": "column 'amount' contains 412 null values",
            "compiled_sql": "select * from fct_orders where amount is null",
            "column": "amount",
            "unique_id": "test.analytics.not_null_amount.abc",
            "query_id": "01b2-c3d4",
        },
    }])
    conn.commit()

    key = f"dbt/analytics.public.fct_orders/not_null_amount/{measured.isoformat()}"
    result = agents.briefing_for(conn, "data_test", key)

    assert result is not None
    assert result.table == "analytics.public.fct_orders"
    assert result.column == "amount"
    assert result.failures == 412
    assert "amount is null" in result.sql
    assert "412 null values" in result.error


def test_missing_check_returns_none_rather_than_an_empty_briefing(conn, configured):
    """Retention outlives alerts, and the page says so rather than opening an
    agent with nothing in its hands."""
    key = "dbt/gone/check/2020-01-01T00:00:00+00:00"
    assert agents.briefing_for(conn, "data_test", key) is None


def test_unknown_event_returns_none(conn, configured):
    assert agents.briefing_for(conn, "digest", "anything") is None


def test_malformed_key_does_not_raise(conn, configured):
    """Delivery must never break detection — including on the click path."""
    assert agents.briefing_for(conn, "data_test", "not-a-key") is None


# --------------------------------------------------------------------- helpers
#
# The real dataclasses rather than stand-ins. `_dbt_node_notification` walks
# `invocation.nodes` to find the table a test asserted on, so a fake thin enough
# to be convenient is also thin enough to pass while the real path is broken.


def _test_node(**over):
    fields = dict(
        unique_id="test.analytics.not_null_amount.abc",
        resource_type="test",
        name="not_null_amount",
        status="fail",
        started_at=None,
        ended_at=None,
        execution_time=1.0,
        message="column 'amount' contains 412 null values",
        failures=412,
        relation=None,
        depends_on=("model.analytics.fct_orders",),
        attached_node="model.analytics.fct_orders",
        test_type="not_null",
        column="amount",
        severity="error",
        compiled_code="select * from fct_orders where amount is null",
        query_id="01b2-c3d4",
    )
    fields.update(over)
    return dbt_artifacts.Node(**fields)


def _model_node():
    return dbt_artifacts.Node(
        unique_id="model.analytics.fct_orders",
        resource_type="model",
        name="fct_orders",
        status="success",
        started_at=None,
        ended_at=None,
        execution_time=2.0,
        message=None,
        failures=None,
        relation="analytics.public.fct_orders",
        depends_on=(),
        attached_node=None,
        test_type=None,
        column=None,
        severity="error",
    )


def _invocation(*tests):
    now = datetime.now(UTC)
    return dbt_artifacts.Invocation(
        invocation_id="inv-1",
        project="analytics",
        adapter="snowflake",
        dbt_version="1.8.0",
        generated_at=now,
        started_at=now,
        nodes=(_model_node(), *tests),
        command="build",
    )


# ------------------------------------------------------------------ the endpoint


def test_handoff_page_renders_the_briefing(api_client, conn, configured):
    """The click always lands somewhere useful, even without the agent installed.

    Which is why this is a page and not a redirect: the reader who most needs the
    briefing is the one whose machine has no handler for the scheme, and a
    redirect would give them a blank tab.
    """
    measured = datetime.now(UTC) - timedelta(minutes=5)
    dq.import_results(conn, source="dbt", rows=[{
        "table": "analytics.public.fct_orders",
        "check": "not_null_amount",
        "status": "fail",
        "value": 412,
        "measured_at": measured,
        "details": {
            "message": "column 'amount' contains 412 null values",
            "compiled_sql": "select * from fct_orders where amount is null",
            "column": "amount",
        },
    }])
    conn.commit()

    key = agents.sign(
        "data_test",
        f"dbt/analytics.public.fct_orders/not_null_amount/{measured.isoformat()}",
    )
    page = api_client.get(f"/handoff/{key}?target=claude-cli")

    assert page.status_code == 200
    assert "not_null_amount" in page.text
    assert "amount is null" in page.text
    # The deep link is offered, and it is the contract the handler parses.
    assert "claude-cli://open?" in page.text


def test_handoff_refuses_an_unsigned_key(api_client, configured):
    """Otherwise the page is an oracle for anyone who can guess a check name."""
    forged = "eyJlIjoiZGF0YV90ZXN0IiwiayI6ImRidC94L3kvIn0.0000000000000000"
    assert api_client.get(f"/handoff/{forged}?target=claude-cli").status_code == 404


def test_handoff_refuses_an_unknown_target(api_client, configured):
    key = agents.sign("data_test", "dbt/a/b/c")
    assert api_client.get(f"/handoff/{key}?target=nonsense").status_code == 400


def test_handoff_says_so_when_the_check_is_gone(api_client, configured):
    """Retention outlives alerts. Better to say that than to open an agent with
    nothing in its hands."""
    key = agents.sign("data_test", "dbt/gone.table/check/2020-01-01T00:00:00+00:00")
    page = api_client.get(f"/handoff/{key}?target=claude-cli")

    assert page.status_code == 200
    assert "Nothing to hand over" in page.text


def test_codex_page_offers_the_command_not_a_promise_of_a_prompt(
    api_client, conn, configured
):
    """The Codex app link cannot carry the briefing, and the page must not imply
    it does — the command is the path that actually sends the error."""
    measured = datetime.now(UTC) - timedelta(minutes=5)
    dq.import_results(conn, source="dbt", rows=[{
        "table": "analytics.public.fct_orders", "check": "unique_id", "status": "fail",
        "value": 3, "measured_at": measured, "details": {"message": "dupes"},
    }])
    conn.commit()

    key = agents.sign(
        "data_test", f"dbt/analytics.public.fct_orders/unique_id/{measured.isoformat()}"
    )
    page = api_client.get(f"/handoff/{key}?target=codex")

    assert page.status_code == 200
    assert "codex exec" in page.text
    assert "cannot carry a prompt" in page.text


# ------------------------------------------------------- the direct deep link


def test_claude_cli_button_is_the_deep_link_itself(configured):
    """Measured: Slack navigates `claude-cli://` straight from a button.

    So the one target whose URL carries the whole briefing skips the page. The
    other two cannot — neither has a URL that takes a prompt — and still address
    `/handoff`, which is the asymmetry working rather than an inconsistency.
    """
    actions = dict(
        agents.handoff_urls("data_test", "k", "https://ds.acme.io", briefing=briefing())
    )
    assert actions["Claude Code"].startswith("claude-cli://open?")
    assert actions["Claude Cloud"].startswith("https://ds.acme.io/handoff/")
    assert actions["Codex"].startswith("https://ds.acme.io/handoff/")


def test_without_a_briefing_every_target_falls_back_to_the_page(configured):
    """The page has no length limit and can explain itself, so it is the safe
    default whenever the direct link cannot be built."""
    actions = dict(agents.handoff_urls("data_test", "k", "https://ds.acme.io"))
    assert all(url.startswith("https://ds.acme.io/handoff/") for url in actions.values())


def test_an_oversized_briefing_falls_back_rather_than_being_rejected(configured):
    """Slack refuses a button url over 3000 characters, and a refused message is
    a lost alert — so the direct link yields to the page instead."""
    huge = briefing(sql="select " + "x" * 40_000)
    url = dict(agents.handoff_urls("data_test", "k", "https://ds.acme.io", briefing=huge))[
        "Claude Code"
    ]
    assert len(url) <= agents.SLACK_URL_LIMIT
    # Either a trimmed deep link or the page — never an over-length URL.
    assert url.startswith(("claude-cli://open?", "https://ds.acme.io/handoff/"))


def test_dbt_briefing_names_upstreams_without_claiming_they_failed(configured):
    """No connection on the dbt path, so lineage degrades to dbt's own graph.

    Naming the upstreams is useful; asserting one of them failed would be a cause
    we never checked.
    """
    node = _test_node()
    b = agents.briefing_from_node(node, _invocation(node))

    assert b.upstreams == ("fct_orders",)
    assert b.failing_upstreams == ()
    assert "amount is null" in b.sql or "order_id is null" in b.sql
    assert "Upstream runs that also failed" not in agents.prompt(b)


def test_dbt_node_button_is_a_direct_deep_link(configured):
    node = _test_node()
    note = notify._dbt_node_notification(node, _invocation(node), "root")
    assert dict(note.actions)["Claude Code"].startswith("claude-cli://open?")


# -------------------------------------------------- acknowledging a button click


def test_interaction_signature_verifies(monkeypatch):
    import hashlib
    import hmac
    import time

    monkeypatch.setenv("DATASPINE_SLACK_SIGNING_SECRET", "shh")
    body, ts = b"payload=%7B%7D", str(int(time.time()))
    sig = "v0=" + hmac.new(b"shh", b"v0:" + ts.encode() + b":" + body, hashlib.sha256).hexdigest()

    assert slack.verify_signature(body, ts, sig) is True
    assert slack.verify_signature(b"tampered", ts, sig) is False


def test_a_replayed_interaction_is_refused(monkeypatch):
    """A signature stays valid forever; without a window a captured request could
    be replayed at us indefinitely."""
    import hashlib
    import hmac

    monkeypatch.setenv("DATASPINE_SLACK_SIGNING_SECRET", "shh")
    body, ts = b"payload=%7B%7D", "1000000000"  # 2001
    sig = "v0=" + hmac.new(b"shh", b"v0:" + ts.encode() + b":" + body, hashlib.sha256).hexdigest()

    assert slack.verify_signature(body, ts, sig) is False


def test_no_signing_secret_refuses_everything(monkeypatch):
    monkeypatch.delenv("DATASPINE_SLACK_SIGNING_SECRET", raising=False)
    assert slack.verify_signature(b"x", "1", "v0=abc") is False


def test_interactivity_endpoint_acks_a_signed_click(api_client, monkeypatch):
    """Slack renders a warning triangle beside every button when the app cannot
    answer. The buttons still work; the alert just looks broken."""
    import hashlib
    import hmac
    import time

    monkeypatch.setenv("DATASPINE_SLACK_SIGNING_SECRET", "shh")
    body, ts = b"payload=%7B%7D", str(int(time.time()))
    sig = "v0=" + hmac.new(b"shh", b"v0:" + ts.encode() + b":" + body, hashlib.sha256).hexdigest()

    ok = api_client.post(
        "/slack/interactivity", content=body,
        headers={"x-slack-request-timestamp": ts, "x-slack-signature": sig},
    )
    assert ok.status_code == 200

    bad = api_client.post(
        "/slack/interactivity", content=body,
        headers={"x-slack-request-timestamp": ts, "x-slack-signature": "v0=deadbeef"},
    )
    assert bad.status_code == 401


# ------------------------------------------------------ the failing upstream


def test_failing_upstream_is_found_across_producer_spellings(conn, configured):
    """The briefing's whole edge, and it silently never worked.

    `dataset_entities.name` is a leaf (`stg_orders`); `datasets.name` is whatever
    the producer called it (`AIDAN_DEV.PUBLIC.stg_orders`, `/warehouse/…/stg_orders`).
    Matching one against the other found nothing, so every briefing reported "no
    upstream failed" regardless of the truth — the worst shape of wrong, because
    it reads as a diagnosis rather than as a gap.

    The names below deliberately disagree with each other, which is the case
    `identity.py` exists for and the case a same-name fixture would have missed.
    """
    from uuid import uuid4

    fact = conn.execute(
        "insert into dataset_entities (name) values ('fct_order_items') returning id"
    ).fetchone()["id"]
    stg = conn.execute(
        "insert into dataset_entities (name) values ('stg_orders') returning id"
    ).fetchone()["id"]
    conn.execute(
        "insert into lineage_edges (upstream_id, downstream_id) values (%s, %s)", (stg, fact)
    )

    # Two spellings of each table, as two producers would report them.
    ids = {}
    for name in ("AIDAN_DEV.PUBLIC.fct_order_items", "/warehouse/iceberg/marts/stg_orders"):
        ids[name] = conn.execute(
            "insert into datasets (namespace, name) values ('snowflake://acct', %s) returning id",
            (name,),
        ).fetchone()["id"]
    conn.execute(
        "insert into dataset_identities (dataset_id, entity_id, match_reason) "
        "values (%s,%s,'test')",
        (ids["AIDAN_DEV.PUBLIC.fct_order_items"], fact),
    )
    conn.execute(
        "insert into dataset_identities (dataset_id, entity_id, match_reason) "
        "values (%s,%s,'test')",
        (ids["/warehouse/iceberg/marts/stg_orders"], stg),
    )

    job = conn.execute(
        "insert into jobs (namespace, name) values ('dbt', 'stg_orders') returning id"
    ).fetchone()["id"]
    run = uuid4()
    conn.execute(
        "insert into runs (run_id, job_id, state, ended_at) "
        "values (%s, %s, 'FAILED', now() - interval '10 minutes')",
        (run, job),
    )
    conn.execute(
        "insert into run_datasets (run_id, dataset_id, direction) values (%s, %s, 'OUTPUT')",
        (run, ids["/warehouse/iceberg/marts/stg_orders"]),
    )

    upstreams, failing = agents._lineage_context(conn, "AIDAN_DEV.PUBLIC.fct_order_items")

    assert upstreams == ("stg_orders",)
    # The assertion that was false before the fix.
    assert failing == ("stg_orders",)


def test_a_healthy_upstream_is_not_reported_as_failing(conn, configured):
    """The complement, and the reason the fix cannot just be a looser match: a
    briefing that names a healthy upstream as the cause sends an agent to rewrite
    working code."""
    from uuid import uuid4

    fact = conn.execute(
        "insert into dataset_entities (name) values ('fct_order_items') returning id"
    ).fetchone()["id"]
    stg = conn.execute(
        "insert into dataset_entities (name) values ('stg_orders') returning id"
    ).fetchone()["id"]
    conn.execute(
        "insert into lineage_edges (upstream_id, downstream_id) values (%s, %s)", (stg, fact)
    )
    d_fact = conn.execute(
        "insert into datasets (namespace, name) "
        "values ('s://a', 'DB.S.fct_order_items') returning id"
    ).fetchone()["id"]
    d_stg = conn.execute(
        "insert into datasets (namespace, name) values ('s://a', 'DB.S.stg_orders') returning id"
    ).fetchone()["id"]
    for did, eid in ((d_fact, fact), (d_stg, stg)):
        conn.execute(
            "insert into dataset_identities (dataset_id, entity_id, match_reason) "
            "values (%s,%s,'test')", (did, eid))

    job = conn.execute(
        "insert into jobs (namespace, name) values ('dbt','stg_orders') returning id"
    ).fetchone()["id"]
    run = uuid4()
    conn.execute(
        "insert into runs (run_id, job_id, state, ended_at) "
        "values (%s, %s, 'COMPLETE', now() - interval '10 minutes')", (run, job))
    conn.execute(
        "insert into run_datasets (run_id, dataset_id, direction) values (%s,%s,'OUTPUT')",
        (run, d_stg))

    upstreams, failing = agents._lineage_context(conn, "DB.S.fct_order_items")
    assert upstreams == ("stg_orders",)
    assert failing == ()


def test_the_run_page_offers_the_same_actions_the_alert_does(api_client, conn, configured):
    """A reader who followed the tree down to the failure is asking the question
    the alert's buttons answer.

    The alert is not the only way in. Someone who opened the run page from the
    run list has the same failing test in front of them, and sending them to find
    the Slack message first is what teaches people the page is the lesser
    surface. Same key, so both open the same briefing.
    """
    import json
    from pathlib import Path

    from dataspine import dbt_artifacts

    fixtures = Path(__file__).parent / "fixtures"
    run_results = json.loads((fixtures / "dbt_run_results_1.12.3.json").read_text())
    manifest = json.loads((fixtures / "dbt_manifest_1.12.3.json").read_text())
    invocation = dbt_artifacts.parse(run_results, manifest)

    events = dbt_artifacts.events(invocation, job_name="Nightly Build")
    api_client.post("/api/v1/lineage/batch", json=events)
    dq.import_results(conn, source="dbt", rows=dbt_artifacts.test_results(invocation))
    conn.commit()

    failing = api_client.get("/api/v1/dq/failing").json()["failing"]
    assert failing, "fixture should carry a failing test"

    runs = api_client.get("/api/v1/runs", params={"state": "FAILED"}).json()["runs"]
    node = next(r for r in runs if "not_null_fct_order_items_order_id" in r["job_name"])

    body = api_client.get(f"/runs/{node['run_id']}").text
    assert "Claude Code" in body
    assert "Claude Cloud" in body
    assert "Codex" in body
    assert "link-chip action" in body


def test_a_run_page_with_nothing_to_act_on_offers_no_buttons(api_client, configured):
    """A run that is not a dbt node has no check row, no warehouse query and
    nothing to brief an agent with. An empty row of buttons would be furniture."""
    from datetime import UTC, datetime

    from dataspine.simulate import build_pipeline

    events = build_pipeline(fail_model=None, start=datetime(2026, 8, 2, tzinfo=UTC))
    api_client.post("/api/v1/lineage/batch", json=events)
    run_id = api_client.get("/api/v1/runs", params={"roots_only": True}).json()["runs"][0]["run_id"]

    assert "link-chip action" not in api_client.get(f"/runs/{run_id}").text
