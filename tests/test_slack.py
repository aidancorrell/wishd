"""Slack routing, transport and rendering.

`test_alerts.py` holds the rules about *when* something is worth sending. This
file is about where it lands, and the tests that matter are the ones about
silence: a route that matches nothing, a channel name a webhook cannot honour, a
Slack error arriving dressed as an HTTP 200. Each of those looks like a working
integration from the outside, and each one means a team hears nothing while
believing they are covered.
"""

from __future__ import annotations

import json

import httpx
import pytest

from dataspine import alerts, notify, slack


class Recorder:
    """An httpx client stand-in that records posts instead of sending them."""

    def __init__(self, status=200, body=None, boom=False):
        self.calls: list[tuple[str, dict, dict]] = []
        self.status = status
        self.body = {"ok": True} if body is None else body
        self.boom = boom

    def post(self, url, json=None, headers=None, **kwargs):
        if self.boom:
            raise httpx.ConnectError("connection refused")
        self.calls.append((url, json, headers or {}))
        return httpx.Response(
            self.status, json=self.body, request=httpx.Request("POST", url)
        )


def _note(event="monitor", status="breach", *, monitor=None, job=None, title="t", summary="s"):
    return notify.Notification(
        event=event,
        status=status,
        title=title,
        summary=summary,
        dedup_key=f"{event}/{monitor or job or title}",
        monitor=monitor,
        job=job,
    )


def _routes(yaml_text):
    import yaml as _yaml

    return slack.parse_routes(_yaml.safe_load(yaml_text), source="t.yml")


@pytest.fixture()
def bot(monkeypatch):
    monkeypatch.setenv(slack.BOT_TOKEN_ENV, "xoxb-test")
    return Recorder()


# ----------------------------------------------------------------------- routing


def test_first_matching_route_wins():
    """Read top-down like a firewall. The alternative — every match fires — is
    fine until someone adds a catch-all, at which point everything goes
    everywhere and the routing has stopped meaning anything."""
    routes = _routes(
        """
        routes:
          - match: {event: monitor, status: breach}
            channel: "#oncall"
          - match: {event: monitor}
            channel: "#data"
          - channel: "#everything"
        """
    )
    assert slack.destinations(_note(status="breach"), routes) == ("#oncall",)
    assert slack.destinations(_note(status="ok"), routes) == ("#data",)
    assert slack.destinations(_note(event="digest", status="ok"), routes) == ("#everything",)


def test_a_route_can_fan_out_to_several_channels():
    routes = _routes(
        """
        routes:
          - match: {status: breach}
            channel: ["#oncall", "#data"]
        """
    )
    assert slack.destinations(_note(), routes) == ("#oncall", "#data")


def test_name_globs_split_by_team():
    routes = _routes(
        """
        routes:
          - match: {event: run_failure, job: "finance_*"}
            channel: "#finance-data"
          - channel: "#data"
        """
    )
    finance = _note(event="run_failure", status="failed", job="finance_nightly")
    other = _note(event="run_failure", status="failed", job="marketing_nightly")
    assert slack.destinations(finance, routes) == ("#finance-data",)
    assert slack.destinations(other, routes) == ("#data",)


def test_a_route_matching_a_key_the_notification_lacks_does_not_match():
    """The important negative. Treating a missing value as a wildcard would send
    every monitor breach to the channel someone set up for one pipeline, and the
    mistake would look exactly like the routing working."""
    routes = _routes(
        """
        routes:
          - match: {job: "*"}
            channel: "#pipelines"
        """
    )
    assert slack.destinations(_note(event="monitor", monitor="fct_orders"), routes) == ()


def test_no_matching_route_means_nowhere_on_purpose():
    """A routes file with no catch-all is how you say "only these are worth a
    message". The notification is dropped deliberately, and `notify.send`
    records that it was."""
    routes = _routes(
        """
        routes:
          - match: {event: run_failure}
            channel: "#pipelines"
        """
    )
    assert slack.destinations(_note(event="monitor"), routes) == ()


def test_without_routes_everything_goes_to_the_one_channel(monkeypatch):
    monkeypatch.setenv(slack.CHANNEL_ENV, "#data")
    assert slack.destinations(_note(), []) == ("#data",)


# ------------------------------------------------------------------- validation


@pytest.mark.parametrize(
    "text",
    [
        "channels: []",                                    # no routes key
        "routes: []",                                      # empty
        "routes:\n  - match: {event: monitor}",            # no channel
        "routes:\n  - channel: '#a'\n    match: {team: x}",  # unknown match key
        "routes:\n  - channel: '#a'\n    match: {event: explosion}",  # unknown event
        "routes:\n  - channel: '#a'\n    nope: 1",         # unknown key
        "routes:\n  - channel: ''",                        # empty channel
    ],
)
def test_a_bad_route_file_is_an_error_not_a_shrug(text):
    """Strict, for the reason `monitors.py` is strict: config that is quietly
    accepted half-formed is config that silently never does its job. Here the
    failure is worse than a monitor that never fires — the channel looks quiet
    because it is wrong, not because nothing is broken."""
    with pytest.raises(slack.RouteError):
        _routes(text)


def test_the_route_error_names_the_file():
    with pytest.raises(slack.RouteError, match="t.yml"):
        _routes("routes:\n  - match: {event: monitor}")


def test_a_missing_routes_file_is_an_error(tmp_path, monkeypatch):
    monkeypatch.setenv(slack.ROUTES_ENV, str(tmp_path / "nope.yml"))
    with pytest.raises(slack.RouteError, match="not found"):
        slack.load_routes()


def test_no_routes_file_is_not_an_error(monkeypatch):
    monkeypatch.delenv(slack.ROUTES_ENV, raising=False)
    assert slack.load_routes() == []


def test_routes_load_from_disk(tmp_path, monkeypatch):
    path = tmp_path / "slack.yml"
    path.write_text("routes:\n  - match: {event: digest}\n    channel: '#reports'\n")
    monkeypatch.setenv(slack.ROUTES_ENV, str(path))
    routes = slack.load_routes()
    assert routes[0].channels == ("#reports",)


# -------------------------------------------------------------------- transport


def test_bot_token_wins_over_webhook(monkeypatch):
    """Someone who has done the app setup has made a choice, and it is the only
    transport that can honour a routes file."""
    monkeypatch.setenv(slack.WEBHOOK_ENV, "https://hooks.slack.com/services/x")
    monkeypatch.setenv(slack.BOT_TOKEN_ENV, "xoxb-test")
    assert slack.transport() == ("bot", "xoxb-test")


def test_nothing_configured_sends_nothing(monkeypatch):
    monkeypatch.delenv(slack.BOT_TOKEN_ENV, raising=False)
    monkeypatch.delenv(slack.WEBHOOK_ENV, raising=False)
    assert slack.deliver([_note()]) == []


def test_the_bot_addresses_the_channel_and_authenticates(bot, monkeypatch):
    monkeypatch.setenv(slack.CHANNEL_ENV, "#data")
    attempts = slack.deliver([_note()], client=bot, routes=[])

    url, payload, headers = bot.calls[0]
    assert url == slack.POST_MESSAGE_URL
    assert payload["channel"] == "#data"
    assert payload["unfurl_links"] is False
    assert payload["unfurl_media"] is False
    assert headers["Authorization"] == "Bearer xoxb-test"
    assert attempts[0]["delivered"]


def test_a_slack_error_inside_an_http_200_is_a_failure(monkeypatch):
    """The whole reason this module does not reuse `alerts._post`.
    `chat.postMessage` answers 200 with `{"ok": false}` for a typo'd channel, and
    status-code-only checking would record that as a successful delivery — the
    exact failure the audit trail exists to catch."""
    monkeypatch.setenv(slack.BOT_TOKEN_ENV, "xoxb-test")
    monkeypatch.setenv(slack.CHANNEL_ENV, "#typo")
    client = Recorder(status=200, body={"ok": False, "error": "channel_not_found"})

    attempt = slack.deliver([_note()], client=client, routes=[])[0]

    assert not attempt["delivered"]
    assert "channel_not_found" in attempt["error"]


def test_a_transport_failure_is_recorded_not_raised(monkeypatch):
    monkeypatch.setenv(slack.BOT_TOKEN_ENV, "xoxb-test")
    monkeypatch.setenv(slack.CHANNEL_ENV, "#data")
    attempt = slack.deliver([_note()], client=Recorder(boom=True), routes=[])[0]
    assert not attempt["delivered"]
    assert "ConnectError" in attempt["error"]


def test_an_http_error_is_recorded(monkeypatch):
    monkeypatch.setenv(slack.BOT_TOKEN_ENV, "xoxb-test")
    monkeypatch.setenv(slack.CHANNEL_ENV, "#data")
    attempt = slack.deliver([_note()], client=Recorder(status=500), routes=[])[0]
    assert attempt["error"] == "HTTP 500"


def test_a_webhook_ignores_channels_and_posts_once(monkeypatch):
    """An incoming webhook is bound to one channel chosen in Slack. Three routes
    naming three channels must not become three copies of one alert."""
    monkeypatch.delenv(slack.BOT_TOKEN_ENV, raising=False)
    monkeypatch.setenv(slack.WEBHOOK_ENV, "https://hooks.slack.com/services/x")
    routes = _routes(
        """
        routes:
          - match: {status: breach}
            channel: ["#a", "#b", "#c"]
        """
    )
    client = Recorder()
    attempts = slack.deliver([_note()], client=client, routes=routes)

    assert len(client.calls) == 1
    assert client.calls[0][0] == "https://hooks.slack.com/services/x"
    assert "channel" not in client.calls[0][1]
    assert attempts[0]["channel"] == slack.WEBHOOK_DESTINATION


def test_a_broken_routes_file_falls_back_rather_than_going_silent(
    tmp_path, monkeypatch, bot
):
    """Refusing to deliver because the routes file is broken would turn a config
    typo into exactly the silence the file exists to prevent."""
    path = tmp_path / "slack.yml"
    path.write_text("routes:\n  - channel: '#a'\n    match: {team: finance}\n")
    monkeypatch.setenv(slack.ROUTES_ENV, str(path))
    monkeypatch.setenv(slack.CHANNEL_ENV, "#fallback")

    attempts = slack.deliver([_note()], client=bot)

    assert attempts[0]["channel"] == "#fallback"
    assert attempts[0]["delivered"]


# --------------------------------------------------------------------- grouping


def test_one_message_per_channel_not_per_notification(bot):
    routes = _routes(
        """
        routes:
          - match: {status: breach}
            channel: "#oncall"
          - match: {status: ok}
            channel: "#data"
        """
    )
    notes = [
        _note(status="breach", monitor="a"),
        _note(status="breach", monitor="b"),
        _note(status="ok", monitor="c"),
    ]
    attempts = slack.deliver(notes, client=bot, routes=routes)

    assert len(bot.calls) == 2, "one message per channel, not one per line"
    by_channel = {a["channel"]: a for a in attempts}
    assert len(by_channel["#oncall"]["notifications"]) == 2
    assert len(by_channel["#data"]["notifications"]) == 1


# -------------------------------------------------------------------- rendering


def test_the_fallback_text_says_what_happened():
    """It is what the phone notification and the sidebar preview show. A preview
    reading only "dataspine" tells nobody whether to open it."""
    text, _ = slack.render([_note(title="fct_orders — in breach")])
    assert "fct_orders" in text


def test_a_long_error_cannot_take_the_message_down():
    """Slack rejects an oversized block with a 400, which would lose the alert at
    the moment it matters most."""
    _, blocks = slack.render([_note(summary="x" * 10_000)])
    assert all(len(json.dumps(b)) < 3200 for b in blocks)


def test_reserved_characters_are_escaped():
    _, blocks = slack.render([_note(summary="expected <int> & got none")])
    body = json.dumps(blocks)
    assert "&lt;int&gt;" in body
    assert "&amp;" in body


def test_a_huge_sweep_stays_inside_the_block_limit():
    _, blocks = slack.render([_note(monitor=f"m{i}") for i in range(200)])
    assert len(blocks) <= 50
    assert "more" in json.dumps(blocks)


def test_the_link_back_is_carried_when_a_base_url_is_set(monkeypatch):
    monkeypatch.setenv(notify.BASE_URL_ENV, "https://dataspine.acme.internal/")
    note = notify.Notification(
        event="monitor", status="breach", title="t", summary="s",
        dedup_key="k", monitor="fct_orders",
        url=notify._link("/monitors/fct_orders"),
    )
    assert "https://dataspine.acme.internal/monitors/fct_orders" in json.dumps(
        slack.render([note])[1]
    )


def test_no_base_url_means_no_guessed_link(monkeypatch):
    """`links.py`'s rule: a URL that 404s costs someone a click and their trust in
    every other link."""
    monkeypatch.delenv(notify.BASE_URL_ENV, raising=False)
    assert notify._link("/overview") is None


# ---------------------------------------------------- monitors reach it too


def test_monitor_transitions_route_through_slack(bot, monkeypatch, tmp_path):
    """The point of moving routing out of `alerts.py`: a team that wants its own
    channel wants everything about its tables in it, not only the half that
    happens to be a monitor."""
    path = tmp_path / "slack.yml"
    path.write_text(
        "routes:\n"
        "  - match: {monitor: 'finance_*'}\n"
        "    channel: '#finance-data'\n"
        "  - channel: '#data'\n"
    )
    monkeypatch.setenv(slack.ROUTES_ENV, str(path))

    alerts.deliver(
        [
            {"monitor": "finance_revenue", "status": "breach", "message": "6h old",
             "value": 1.0, "transitioned": True},
            {"monitor": "fct_orders", "status": "breach", "message": "7h old",
             "value": 2.0, "transitioned": True},
        ],
        client=bot,
    )

    channels = {payload["channel"] for _, payload, _ in bot.calls}
    assert channels == {"#finance-data", "#data"}


def test_slack_counts_as_a_configured_channel_with_a_bot_token(monkeypatch):
    monkeypatch.delenv(slack.WEBHOOK_ENV, raising=False)
    monkeypatch.setenv(slack.BOT_TOKEN_ENV, "xoxb-test")
    assert "slack" in alerts.configured_channels()


# ------------------------------------------------------------------ diagnostics


def test_describe_names_the_webhook_routing_trap(tmp_path, monkeypatch):
    """The most expensive misconfiguration available, because it looks like it is
    working: messages arrive, just never where the file says."""
    monkeypatch.delenv(slack.BOT_TOKEN_ENV, raising=False)
    monkeypatch.setenv(slack.WEBHOOK_ENV, "https://hooks.slack.com/services/x")
    path = tmp_path / "slack.yml"
    path.write_text("routes:\n  - match: {event: monitor}\n    channel: '#oncall'\n")
    monkeypatch.setenv(slack.ROUTES_ENV, str(path))

    state = slack.describe()

    assert state["transport"] == "webhook"
    assert any("#oncall" in p and slack.BOT_TOKEN_ENV in p for p in state["problems"])


def test_describe_is_quiet_when_the_setup_is_sound(tmp_path, monkeypatch):
    monkeypatch.setenv(slack.BOT_TOKEN_ENV, "xoxb-test")
    path = tmp_path / "slack.yml"
    path.write_text("routes:\n  - channel: '#data'\n")
    monkeypatch.setenv(slack.ROUTES_ENV, str(path))
    assert slack.describe()["problems"] == []


def test_a_bot_token_with_nowhere_to_post_sends_nothing(monkeypatch):
    """Rather than addressing a channel literally named "webhook", which would be
    `channel_not_found` on every alert forever. `describe()` reports it instead."""
    monkeypatch.setenv(slack.BOT_TOKEN_ENV, "xoxb-test")
    monkeypatch.delenv(slack.CHANNEL_ENV, raising=False)
    client = Recorder()

    assert slack.deliver([_note()], client=client, routes=[]) == []
    assert client.calls == []
    assert any(slack.CHANNEL_ENV in p for p in slack.describe()["problems"])


def test_a_webhook_with_no_channel_named_still_delivers(monkeypatch):
    """The classic single-webhook install names no channel anywhere, because the
    webhook carries its own. It must not be caught by the rule above."""
    monkeypatch.delenv(slack.BOT_TOKEN_ENV, raising=False)
    monkeypatch.setenv(slack.WEBHOOK_ENV, "https://hooks.slack.com/services/x")
    client = Recorder()

    assert slack.deliver([_note()], client=client, routes=[])[0]["delivered"]
    assert len(client.calls) == 1


# ------------------------------------------------------------------ rate limits


class Limited:
    """429s once, then succeeds — Slack's ordinary behaviour under a fan-out."""

    def __init__(self, retry_after="0"):
        self.calls = 0
        self.retry_after = retry_after

    def post(self, url, json=None, headers=None, **kwargs):
        self.calls += 1
        request = httpx.Request("POST", url)
        if self.calls == 1:
            return httpx.Response(
                429, headers={"Retry-After": self.retry_after}, json={"ok": False},
                request=request,
            )
        return httpx.Response(200, json={"ok": True}, request=request)


def test_a_rate_limit_is_waited_out_once(bot, monkeypatch):
    """`chat.postMessage` allows roughly one message per second per channel, so a
    sweep fanning out to several channels is the ordinary case that trips it —
    and it is the one failure certain to succeed shortly after."""
    monkeypatch.setenv(slack.CHANNEL_ENV, "#data")
    client = Limited(retry_after="0")

    attempt = slack.deliver([_note()], client=client, routes=[])[0]

    assert client.calls == 2
    assert attempt["delivered"]


def test_a_long_rate_limit_is_not_waited_out(bot, monkeypatch):
    """Sleeping five minutes would block every remaining channel in the sweep to
    rescue one message. The ledger's bounded retry gets it next time."""
    monkeypatch.setenv(slack.CHANNEL_ENV, "#data")
    client = Limited(retry_after="300")

    attempt = slack.deliver([_note()], client=client, routes=[])[0]

    assert client.calls == 1, "no sleep, no retry"
    assert not attempt["delivered"]
    assert "429" in attempt["error"]


def test_a_second_rate_limit_is_not_retried_again(bot, monkeypatch):
    monkeypatch.setenv(slack.CHANNEL_ENV, "#data")

    class AlwaysLimited(Limited):
        def post(self, url, json=None, headers=None, **kwargs):
            self.calls += 1
            return httpx.Response(
                429, headers={"Retry-After": "0"}, json={"ok": False},
                request=httpx.Request("POST", url),
            )

    client = AlwaysLimited()
    attempt = slack.deliver([_note()], client=client, routes=[])[0]

    assert client.calls == 2, "one retry, not a loop"
    assert attempt["error"] == "HTTP 429"


# ----------------------------------------------------------------- integration


def _failure(*integrations, job="analytics_daily"):
    return notify.Notification(
        event="run_failure", status="failed", title="t", summary="s",
        dedup_key="k", job=job, integrations=tuple(integrations),
    )


def test_routing_on_what_kind_of_thing_broke():
    """The split between "the data is wrong" and "the machinery stopped" is not
    expressible as a job-name glob."""
    routes = _routes(
        """
        routes:
          - match: {event: run_failure, integration: DBT}
            channel: "#data-alerts"
          - match: {event: run_failure}
            channel: "#pipelines"
        """
    )
    assert slack.destinations(_failure("AIRFLOW", "DBT"), routes) == ("#data-alerts",)
    assert slack.destinations(_failure("AIRFLOW"), routes) == ("#pipelines",)


def test_any_failing_integration_counts_as_a_match():
    """On dbt-over-Spark the model fails and so does the Spark job under it.
    Routing on only the deepest would file a dbt failure as a Spark one."""
    routes = _routes(
        """
        routes:
          - match: {integration: DBT}
            channel: "#data-alerts"
        """
    )
    assert slack.destinations(
        _failure("AIRFLOW", "DBT", "SPARK"), routes
    ) == ("#data-alerts",)


def test_an_integration_route_never_matches_a_monitor_breach():
    """A monitor has no integration, and treating that as a wildcard would put
    every breach in the channel meant for pipeline failures."""
    routes = _routes(
        """
        routes:
          - match: {integration: "*"}
            channel: "#pipelines"
        """
    )
    assert slack.destinations(_note(event="monitor", monitor="fct_orders"), routes) == ()


# ----------------------------------------------------------------- link lines


def _link_note(**kwargs) -> notify.Notification:
    base = dict(
        event="run_failure", status="failed", title="nightly failed",
        summary="it broke", dedup_key="k",
    )
    return notify.Notification(**{**base, **kwargs})


def test_external_links_come_before_our_own(monkeypatch):
    """dataspine's link is the one that always exists, so leading with it would
    put the same words at the front of every message in the channel. The reader
    about to act wants the system that ran the thing."""
    note = _link_note(
        links=(("dbt Cloud", "https://abc123.us1.dbt.com/deploy/1/runs/2/"),),
        url="https://dataspine.example.com/runs/abc",
    )
    _, blocks = slack.render([note])
    context = next(b for b in blocks if b["type"] == "context")["elements"][0]["text"]
    assert context.index("|dbt Cloud>") < context.index("open in wish:d")


def test_a_message_with_no_external_links_is_unchanged():
    """The overwhelmingly common case must not gain a stray separator."""
    note = _link_note(url="https://dataspine.example.com/runs/abc")
    _, blocks = slack.render([note])
    context = next(b for b in blocks if b["type"] == "context")["elements"][0]["text"]
    assert context == "<https://dataspine.example.com/runs/abc|open in wish:d>"


def test_links_are_capped_so_the_line_stays_readable():
    """Past a handful they stop being a route to the answer and become a wall."""
    note = _link_note(links=tuple((f"link {i}", f"https://e.example/{i}") for i in range(9)))
    _, blocks = slack.render([note])
    context = next(b for b in blocks if b["type"] == "context")["elements"][0]["text"]
    assert context.count("https://e.example/") == slack.MAX_LINKS


def test_a_link_label_cannot_break_out_of_the_link():
    """Slack's three reserved characters, in the one place a producer's own
    text lands inside link syntax."""
    note = _link_note(links=(("a > b & <c>", "https://e.example/1"),))
    _, blocks = slack.render([note])
    context = next(b for b in blocks if b["type"] == "context")["elements"][0]["text"]
    assert "|a &gt; b &amp; &lt;c&gt;>" in context


@pytest.mark.parametrize("event", ["dbt_job", "pipeline"])
def test_dbt_cloud_title_is_clickable(event):
    url = "https://dbt.example/deploy/1/runs/2/"
    note = _link_note(event=event, title="Nightly <Build>", links=(("dbt Cloud", url),))
    _, blocks = slack.render([note])
    section = next(b for b in blocks if b["type"] == "section")["text"]["text"]
    assert f"<{url}|Nightly &lt;Build&gt;>" in section
