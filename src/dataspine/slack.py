"""Getting a notification into the right Slack channel.

`alerts.py` states the rules about *when* to tell a human something. This module
is about *where* it lands, and it exists because one channel stops being enough
almost immediately: the on-call wants breaches, the finance team wants their own
tables, and nobody wants a daily digest in the channel that pages them.

Three decisions worth stating up front.

  **Routing is committed; credentials are not.** A channel name is not a secret
  and belongs in review — "why does finance get paged for this?" is a question
  with a diff attached. A bot token is a credential and stays in the environment.
  So routes are YAML (`DATASPINE_SLACK_ROUTES`) and the token is an env var, the
  same split `sources.py` already makes for warehouse DSNs.

  **First match wins.** Routes are read top-down like a firewall, and the first
  one that matches decides. The alternative — every matching route fires — reads
  fine until someone adds a catch-all, at which point every alert quietly goes
  everywhere and the routing has stopped meaning anything. Fan-out is still
  available where it is *intended*, by naming several channels on one route.

  **A bad route file is an error, not a shrug.** Same reasoning as monitors: a
  monitor we cannot parse has produced nothing yet, so accepting it half-formed
  means one that silently never fires. A route we cannot parse means alerts
  silently going to the wrong place, which is worse — the channel looks quiet
  because it is wrong, not because nothing is broken.

Two transports, because the setup costs are genuinely different:

  **Bot token** (`DATASPINE_SLACK_BOT_TOKEN`, needs `chat:write`) can post to any
  channel by name, so it is the only one that can actually honour a routes file.

  **Incoming webhook** (`DATASPINE_SLACK_WEBHOOK`) is one URL bound to one channel
  and needs no app scopes. It remains supported because it is thirty seconds of
  setup and it is what existing installs use. With a webhook configured, routing
  still runs — it decides *whether* to send — but every message lands in that one
  channel, and `dataspine slack-check` says so rather than letting someone believe
  in a fan-out that is not happening.

The Slack Web API is why `_post` here does not simply reuse the one in `alerts.py`:
`chat.postMessage` answers **HTTP 200 with `{"ok": false, "error":
"channel_not_found"}`**. Status-code-only checking would record a typo'd channel
as a successful delivery, which is precisely the failure the audit trail exists
to catch.
"""

from __future__ import annotations

import fnmatch
import hashlib
import hmac
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .config import env
from .notify import Notification

log = logging.getLogger("dataspine.slack")

BOT_TOKEN_ENV = "DATASPINE_SLACK_BOT_TOKEN"
CHANNEL_ENV = "DATASPINE_SLACK_CHANNEL"
WEBHOOK_ENV = "DATASPINE_SLACK_WEBHOOK"
ROUTES_ENV = "DATASPINE_SLACK_ROUTES"

# The Slack app's Signing Secret, used only to verify that an interaction
# callback really came from Slack. Distinct from the bot token: that one lets us
# talk to Slack, this one lets us believe Slack when it talks to us.
SIGNING_SECRET_ENV = "DATASPINE_SLACK_SIGNING_SECRET"

# Slack's own replay window for signed requests. Older than this and the
# signature may be valid while the request is a recording.
SIGNATURE_MAX_AGE_S = 300

POST_MESSAGE_URL = "https://slack.com/api/chat.postMessage"
UPDATE_MESSAGE_URL = "https://slack.com/api/chat.update"

# The pseudo-destination used when the transport is an incoming webhook. The
# webhook's channel is chosen in Slack, not by us, and recording a channel name
# we did not actually address would make the audit trail a work of fiction.
WEBHOOK_DESTINATION = "webhook"

# What a route may match on. `event` and `status` are the notification's own
# vocabulary; `monitor` and `job` are the name of the thing it is about, which is
# what a per-team split is really keyed on; `integration` is what kind of thing
# broke, which is what separates "the data is wrong" from "the machinery stopped".
MATCH_KEYS = ("event", "status", "monitor", "job", "dataset", "integration")

# Route keys are singular because that is how they read in YAML
# (`integration: DBT`); the attribute behind one may hold several values.
_MATCH_ATTR = {"integration": "integrations"}

# Slack's own limits. Exceeding any of them is a 400 for the whole message, so a
# stack trace in an error field would take the alert down with it -- the one
# moment the alert matters most.
HEADER_LIMIT = 150
SECTION_LIMIT = 2900
MAX_BLOCKS = 45
ERROR_LIMIT = 700

# How many thread replies one parent may carry. A wide `dbt build` can fail
# sixty nodes, and Slack allows roughly one message per second per channel --
# so posting all of them would spend a minute of rate limit that a *different*
# alert then has to queue behind. Past this the thread says how many more
# there are and links to the run.
MAX_THREAD_REPLIES = 20

# The longest a delivery will block waiting out a rate limit. `notify` and
# `check` are cron jobs with other channels still to serve, so past this the
# honest move is to fail now and let the ledger's bounded retry pick it up.
MAX_RETRY_WAIT = 10

# How many external links one message may carry. A context line is one line in
# the channel, and past a handful the links stop being a route to the answer and
# become a wall to read. `links.py` orders them most-useful-first.
MAX_LINKS = 4

# Slack's own cap on a button label is 75 characters; ours is tighter because
# these sit in a row and a wrapped button reads as a broken one.
BUTTON_LIMIT = 24

# Slack allows 25 elements in an actions block. The real limit is the reader's:
# past a handful, a row of buttons stops being a choice and becomes a menu to
# be decided about, which is the opposite of what an alert wants.
MAX_ACTIONS = 5


class RouteError(ValueError):
    """A routes file that cannot be used. Carries the file, because "invalid
    config" without a location is a scavenger hunt."""


# ----------------------------------------------------------------------- routes


@dataclass(frozen=True)
class Route:
    """One rule: what it matches, and where that goes."""

    channels: tuple[str, ...]
    match: dict[str, tuple[str, ...]] = field(default_factory=dict)
    source: str | None = None

    def matches(self, note: Notification) -> bool:
        """Every stated key must match; unstated keys match anything.

        A route that matches on `job` never matches a notification with no job.
        The alternative -- treating a missing value as a wildcard -- would send
        monitor breaches to the channel someone set up for one pipeline, and the
        mistake would look like the routing working.
        """
        for key, patterns in self.match.items():
            value = getattr(note, _MATCH_ATTR.get(key, key), None)
            if not value:
                return False
            # One key may stand for several values (a failure involving both dbt
            # and Spark). Any of them matching is a match, because the question
            # a route asks is "did this involve dbt?", not "was it only dbt?".
            candidates = (value,) if isinstance(value, str) else tuple(value)
            if not any(
                fnmatch.fnmatchcase(c, p) for c in candidates for p in patterns
            ):
                return False
        return True


def parse_routes(raw: Any, *, source: str | None = None) -> list[Route]:
    """Validate one parsed YAML document into routes."""
    where = f" in {source}" if source else ""
    if not isinstance(raw, dict):
        raise RouteError(f"slack route file{where} must be a mapping with a `routes:` key")

    entries = raw.get("routes")
    if entries is None:
        raise RouteError(f"no `routes:` key{where}")
    if not isinstance(entries, list):
        raise RouteError(f"`routes:`{where} must be a list")
    if not entries:
        raise RouteError(f"`routes:`{where} is empty — remove the file or add a route")

    routes = []
    for index, entry in enumerate(entries):
        routes.append(_parse_route(entry, index=index, source=source))
    return routes


def _parse_route(entry: Any, *, index: int, source: str | None) -> Route:
    where = f" in {source}" if source else ""
    at = f"route {index + 1}{where}"
    if not isinstance(entry, dict):
        raise RouteError(f"{at} must be a mapping")

    unknown = set(entry) - {"match", "channel", "channels"}
    if unknown:
        raise RouteError(f"{at} has unknown key(s): {', '.join(sorted(unknown))}")

    raw_channels = entry.get("channel", entry.get("channels"))
    if raw_channels is None:
        raise RouteError(f"{at} needs a `channel:`")
    channels = [raw_channels] if isinstance(raw_channels, str) else raw_channels
    if not isinstance(channels, list) or not channels:
        raise RouteError(f"{at}: `channel:` must be a channel name or a list of them")
    for channel in channels:
        if not isinstance(channel, str) or not channel.strip():
            raise RouteError(f"{at}: `channel:` entries must be non-empty strings")

    raw_match = entry.get("match", {})
    if not isinstance(raw_match, dict):
        raise RouteError(f"{at}: `match:` must be a mapping")
    unknown = set(raw_match) - set(MATCH_KEYS)
    if unknown:
        raise RouteError(
            f"{at}: cannot match on {', '.join(sorted(unknown))} — "
            f"expected one of {', '.join(MATCH_KEYS)}"
        )

    match: dict[str, tuple[str, ...]] = {}
    for key, value in raw_match.items():
        patterns = [value] if isinstance(value, str) else value
        if not isinstance(patterns, list) or not patterns:
            raise RouteError(f"{at}: `match.{key}` must be a string or a list of strings")
        for pattern in patterns:
            if not isinstance(pattern, str) or not pattern.strip():
                raise RouteError(f"{at}: `match.{key}` entries must be non-empty strings")
        match[key] = tuple(p.strip() for p in patterns)

    # A route matching an event nobody emits is a typo, and it fails silently:
    # the alerts it was meant to catch fall through to whatever comes next.
    for event in match.get("event", ()):
        if not any(fnmatch.fnmatchcase(known, event) for known in Notification.EVENTS):
            raise RouteError(
                f"{at}: unknown event {event!r} — "
                f"expected one of {', '.join(Notification.EVENTS)}"
            )

    return Route(
        channels=tuple(c.strip() for c in channels), match=match, source=source
    )


def load_routes(path: Path | None = None) -> list[Route]:
    """Read the routes file, or return [] when none is configured.

    No file is not an error: a single-channel install is the common case and
    should not have to write YAML to say so.
    """
    target = path or (Path(env[ROUTES_ENV]) if env.get(ROUTES_ENV) else None)
    if target is None:
        return []
    if not target.exists():
        raise RouteError(f"slack routes file not found: {target}")

    try:
        import yaml
    except ImportError as exc:  # pragma: no cover - dependency is declared
        raise RouteError("PyYAML is required to read slack routes") from exc

    try:
        raw = yaml.safe_load(target.read_text())
    except Exception as exc:
        raise RouteError(f"{target}: {exc}") from exc
    return parse_routes(raw, source=str(target))


def default_channel() -> str | None:
    channel = env.get(CHANNEL_ENV, "").strip()
    return channel or None


def destinations(note: Notification, routes: list[Route]) -> tuple[str, ...]:
    """Where one notification goes. Empty means nowhere, which is a valid answer.

    A routes file with no catch-all is how you say "only these things are worth
    a message" — so an unmatched notification is dropped deliberately, not lost.
    Without routes, everything goes to the single configured channel.
    """
    if not routes:
        channel = default_channel()
        if channel:
            return (channel,)
        # An incoming webhook carries its own channel, so "no channel named" is
        # the normal single-webhook install. A bot token with nowhere to post is
        # a misconfiguration, and inventing a channel name for it would turn that
        # into a `channel_not_found` on every alert forever; `describe()` says so
        # instead.
        return (WEBHOOK_DESTINATION,) if _kind() == "webhook" else ()
    for route in routes:
        if route.matches(note):
            return route.channels
    return ()


# -------------------------------------------------------------------- transport


def transport() -> tuple[str, str] | None:
    """`("bot", token)`, `("webhook", url)`, or None when Slack is not configured.

    The bot token wins when both are set. Someone who has done the app setup has
    made a choice, and it is the only transport that can honour a routes file.
    """
    token = env.get(BOT_TOKEN_ENV, "").strip()
    if token:
        return ("bot", token)
    url = env.get(WEBHOOK_ENV, "").strip()
    if url:
        return ("webhook", url)
    return None


def configured() -> bool:
    return transport() is not None


def _kind() -> str | None:
    kind_secret = transport()
    return kind_secret[0] if kind_secret else None


def _retry_after(response: Any) -> float | None:
    """Seconds to wait, or None when Slack is asking for longer than we will."""
    try:
        raw = response.headers.get("Retry-After")
    except Exception:  # noqa: BLE001 - a stand-in client need not have headers
        raw = None
    try:
        # Slack always sends the header on a 429; a missing or unparseable one
        # still means "too fast", and one second is the documented floor.
        delay = 1.0 if raw is None else float(raw)
    except (TypeError, ValueError):
        delay = 1.0
    return None if delay > MAX_RETRY_WAIT else max(delay, 0.0)


def _post_to(client: Any, url: str, payload: dict[str, Any]) -> dict[str, Any]:
    """One authenticated Web API call, reduced to delivered/error.

    Shared by `chat.postMessage` and `chat.update` because everything that makes
    Slack awkward is the same for both: the 429, and the `ok: false` hiding
    inside an HTTP 200.

    Never raises. A 500 from Slack is a lost alert, and letting it propagate
    would turn a lost alert into a lost monitoring sweep, which is worse.
    """
    kind_secret = transport()
    if kind_secret is None or kind_secret[0] != "bot":
        return {"delivered": False, "error": "slack bot token not configured"}
    headers = {"Authorization": f"Bearer {kind_secret[1]}"}

    response, error = _send(client, url, payload, headers)
    if error is not None:
        return error

    body = _body(response)
    if body is None:
        return {"delivered": False, "error": "slack returned a non-JSON 200"}
    if not body.get("ok"):
        problem = body.get("error", "unknown")
        log.warning("slack rejected the message: %s", problem)
        return {"delivered": False, "error": f"slack: {problem}"}
    # Slack resolves "#name" to a channel id and assigns a `ts`. Both are needed
    # to edit the message later, and `chat.update` will not accept the name --
    # so this reply is the only place that address ever exists.
    return {
        "delivered": True,
        "error": None,
        "message_channel": body.get("channel"),
        "message_ts": body.get("ts"),
    }


def _body(response: Any) -> dict[str, Any] | None:
    try:
        parsed = response.json()
    except Exception:  # noqa: BLE001 - a 200 with no JSON is still suspect
        return None
    return parsed if isinstance(parsed, dict) else None


def _send(
    client: Any, url: str, payload: dict[str, Any], headers: dict[str, str]
) -> tuple[Any, dict[str, Any] | None]:
    """POST with one retry, and only for a 429.

    Slack rate-limits to roughly one message per second per channel, so a sweep
    fanning out to several channels is the ordinary case that trips it -- and a
    rate limit is the one failure certain to succeed shortly after. Returns
    `(response, None)` on an HTTP success, or `(None, failure)`.
    """
    for attempt in range(2):
        try:
            response = client.post(url, json=payload, headers=headers)
        except Exception as exc:  # noqa: BLE001 - a lost alert must not lose the sweep
            log.warning("slack delivery failed: %s", exc)
            return None, {"delivered": False, "error": f"{type(exc).__name__}: {exc}"}

        status = getattr(response, "status_code", 0)
        if status != 429 or attempt:
            break

        delay = _retry_after(response)
        if delay is None:
            # Slack is asking for longer than a cron run should sit still for.
            # Sleeping anyway would block every remaining channel in the sweep to
            # rescue one message; the ledger's bounded retry gets it next time.
            log.warning("slack rate-limited beyond the wait budget")
            return None, {
                "delivered": False,
                "error": f"HTTP 429 (retry-after over {MAX_RETRY_WAIT}s)",
            }
        log.warning("slack rate-limited, retrying in %ss", delay)
        time.sleep(delay)

    if not (200 <= status < 300):
        log.warning("slack delivery returned %s", status)
        return None, {"delivered": False, "error": f"HTTP {status}"}
    return response, None


def _post(
    client: Any,
    note_channel: str,
    text: str,
    blocks: list[dict[str, Any]],
    *,
    thread_ts: str | None = None,
) -> dict:
    """Send one message to one destination, by whichever transport is configured."""
    kind_secret = transport()
    if kind_secret is None:
        return {"delivered": False, "error": "slack not configured"}
    kind, secret = kind_secret

    payload: dict[str, Any] = {
        "text": text, "blocks": blocks, "unfurl_links": False, "unfurl_media": False,
    }
    if kind == "bot":
        payload["channel"] = note_channel
        if thread_ts:
            payload["thread_ts"] = thread_ts
        return _post_to(client, POST_MESSAGE_URL, payload)
    if thread_ts:
        # An incoming webhook has no concept of a thread. Posting the replies
        # flat would put sixty lines in the channel that the threading exists to
        # keep out of it, so they are dropped and the parent says how many.
        return {"delivered": False, "error": "incoming webhooks cannot reply in a thread"}

    # An incoming webhook carries its own channel and returns no address, so a
    # message sent through one can never be edited. `notify.track` refuses that
    # transport rather than degrading into one message per step.
    response, error = _send(client, secret, payload, {})
    if error is not None:
        return error
    return {"delivered": True, "error": None, "message_channel": None, "message_ts": None}


# --------------------------------------------------------------------- rendering


def _escape(text: str) -> str:
    """Slack's three reserved characters. An error message containing `<` would
    otherwise be read as the start of a link and swallow the rest of the line."""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _clip(text: str, limit: int) -> str:
    text = str(text)
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def _section(text: str) -> dict[str, Any]:
    return {"type": "section", "text": {"type": "mrkdwn", "text": _clip(text, SECTION_LIMIT)}}


def render(
    notes: list[Notification], *, header: bool = True
) -> tuple[str, list[dict[str, Any]]]:
    """One message for a group of notifications: `(fallback text, blocks)`.

    The fallback text is not decorative — it is what the phone notification and
    the sidebar preview show, and a message whose preview reads "dataspine" tells
    someone nothing about whether to open it.
    """
    heading = _heading(notes)
    blocks: list[dict[str, Any]] = []
    if header:
        blocks.append(
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": _clip(f"wish:d — {heading}", HEADER_LIMIT),
                },
            }
        )

    # Two blocks per notification, so the cap on how many are rendered has to
    # leave room for the header and the overflow note.
    room = (MAX_BLOCKS - 2) // 2
    for note in notes[:room]:
        title = _escape(note.title)
        if note.event in ("dbt_job", "pipeline"):
            cloud_url = next((url for label, url in note.links if label == "dbt Cloud"), None)
            if cloud_url:
                title = f"<{cloud_url}|{title}>"
        blocks.append(_section(f"*{title}*\n{_escape(note.summary)}"))
        context = _context(note)
        if context:
            blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": context}]})
    if len(notes) > room:
        blocks.append(_section(f"_…and {len(notes) - room} more._"))

    # Only ever on a message about one thing. In a group of twelve there is no
    # answer to "which of these would the button investigate?", and a button
    # that silently picks one is worse than none — the reader would find out
    # which by reading the agent's first reply. Thread replies are rendered one
    # per call, so each failing test in a dbt run still gets its own.
    if len(notes) == 1 and notes[0].actions:
        blocks.append(_actions(notes[0]))

    return f"wish:d — {heading}", blocks


def _actions(note: Notification) -> dict[str, Any]:
    """The row of handoff buttons.

    `url` buttons rather than `action_id` ones, so this needs no Slack app, no
    request URL and no interactivity subscription — the link is the whole
    mechanism. What sits behind it is dataspine's own `/handoff` endpoint, which
    is what lets a target that is not addressable by URL still be offered here.
    See `agents.py` for why none of them point at an agent directly.
    """
    return {
        "type": "actions",
        "elements": [
            {
                "type": "button",
                "text": {"type": "plain_text", "text": _clip(label, BUTTON_LIMIT)},
                "url": url,
            }
            for label, url in note.actions[:MAX_ACTIONS]
        ],
    }


def _heading(notes: list[Notification]) -> str:
    if len(notes) == 1:
        return notes[0].title
    kinds = {n.event for n in notes}
    what = kinds.pop() if len(kinds) == 1 else "alert"
    label = what.replace("_", " ")
    return f"{len(notes)} {label}{'s' if not label.endswith('s') else ''}"


def _context(note: Notification) -> str:
    """The supporting line: fields, then everywhere this can be opened.

    A link is the difference between an alert and a task. Without one, the first
    thing anybody does is go and find the thing by hand.

    dataspine's own link goes *last*, deliberately. It is the one that always
    exists, so leading with it would put the same words at the front of every
    message in the channel; and the reader who is about to act usually wants the
    system that ran the thing -- the dbt Cloud run, the failing Snowflake query
    -- rather than our summary of it. Ours is the fallback, and it reads like one.
    """
    parts = [f"{_escape(k)}: *{_escape(_clip(v, 120))}*" for k, v in note.fields if v]
    parts += [f"<{url}|{_escape(label)}>" for label, url in note.links[:MAX_LINKS]]
    if note.url:
        parts.append(f"<{note.url}|open in wish:d>")
    return "  ·  ".join(parts)


# ---------------------------------------------------------------------- delivery


def deliver(
    notes: list[Notification],
    *,
    client: Any = None,
    routes: list[Route] | None = None,
) -> list[dict[str, Any]]:
    """Route, group and send. Returns one attempt per message actually posted.

    Grouping is per channel rather than per notification, for the reason
    `alerts.py` groups per sweep: twelve breaches from one bad upstream is one
    message with twelve lines. Splitting them back out per channel is the least
    surprising reading of "these all go to #data-oncall".
    """
    if not notes or not configured():
        return []

    try:
        routes = load_routes() if routes is None else routes
    except RouteError as exc:
        # Refusing to deliver because the routes file is broken would turn a
        # config typo into silence, which is the outcome the file exists to
        # prevent. Say so loudly and fall back to the single channel.
        log.error("slack routes unusable, falling back to the default channel: %s", exc)
        routes = []

    kind = _kind()

    # Channel names are meaningless to an incoming webhook, which is bound to one
    # channel in Slack. Collapsing here rather than at render time means routing
    # still decides *whether* to send, and the same alert is not posted three
    # times to one channel because three routes named three names.
    grouped: dict[str, list[Notification]] = {}
    for note in notes:
        for channel in destinations(note, routes):
            key = WEBHOOK_DESTINATION if kind == "webhook" else channel
            grouped.setdefault(key, []).append(note)
            if kind == "webhook":
                break

    if not grouped:
        return []

    owns_client = client is None
    if owns_client:
        import httpx

        client = httpx.Client(timeout=10)

    attempts = []
    try:
        for channel, group in grouped.items():
            text, blocks = render(group)
            attempt = _post(client, channel, text, blocks)
            attempt = dict(attempt, channel=channel, notifications=group)
            attempt["thread"] = _reply_in_thread(client, channel, group, attempt)
            attempts.append(attempt)
    finally:
        if owns_client:
            client.close()
    return attempts


def _reply_in_thread(
    client: Any, channel: str, group: list[Notification], parent: dict[str, Any]
) -> list[dict[str, Any]]:
    """Post each notification's `thread` under the message that summarised it.

    One reply per failure, deliberately, rather than one message listing them
    all. A thread reply is a thing a person can react to, quote or reply under —
    which is the difference between an alert someone reads and an alert someone
    picks up. The channel still shows one line.

    Never raises, and a failed reply never invalidates the parent: a summary that
    arrived is worth more than a thread that did not.
    """
    ts = parent.get("message_ts")
    if not parent.get("delivered") or not ts:
        return []

    replies: list[dict[str, Any]] = []
    for note in group:
        children = list(note.thread)
        if not children:
            continue
        for child in children[:MAX_THREAD_REPLIES]:
            text, blocks = render([child], header=False)
            replies.append(dict(_post(client, channel, text, blocks, thread_ts=ts), note=child))
        overflow = len(children) - MAX_THREAD_REPLIES
        if overflow > 0:
            text = f"…and {overflow} more. {note.url or 'See wish:d for the rest.'}"
            replies.append(
                dict(_post(client, channel, text, [_section(text)], thread_ts=ts), note=None)
            )
    return replies


# ------------------------------------------------------------------ live edits


def can_update() -> bool:
    """Whether the configured transport can edit a message it already sent.

    Only the bot token can. An incoming webhook is fire-and-forget by design, and
    a live feed that cannot edit degrades into one message per step — which is
    the flood the feed exists to avoid, so callers refuse rather than degrade.
    """
    return _kind() == "bot"


def update(
    note: Notification,
    targets: list[dict[str, Any]],
    *,
    client: Any = None,
) -> list[dict[str, Any]]:
    """Rewrite messages already posted for `note`. One attempt per target.

    `targets` are the addresses `deliver` handed back: `{"channel": "C0123",
    "ts": "170.1"}`. Never raises, for the reason everything else here never
    raises — a failed edit must not fail the sweep that produced it.
    """
    if not targets or not can_update():
        return []

    text, blocks = render([note])
    owns_client = client is None
    if owns_client:
        import httpx

        client = httpx.Client(timeout=10)

    attempts = []
    try:
        for target in targets:
            attempt = _post_to(
                client,
                UPDATE_MESSAGE_URL,
                {
                    "channel": target["channel"],
                    "ts": target["ts"],
                    "text": text,
                    "blocks": blocks,
                    "attachments": [],
                },
            )
            attempts.append(dict(attempt, channel=target.get("route", target["channel"])))
    finally:
        if owns_client:
            client.close()
    return attempts


# ------------------------------------------------------------------ diagnostics


def describe() -> dict[str, Any]:
    """What `dataspine slack-check` reports. Pure inspection, sends nothing."""
    kind = _kind()

    problems: list[str] = []
    try:
        routes = load_routes()
        route_error = None
    except RouteError as exc:
        routes, route_error = [], str(exc)
        problems.append(str(exc))

    if kind is None:
        problems.append(
            f"no Slack transport configured — set {BOT_TOKEN_ENV} (preferred) or {WEBHOOK_ENV}"
        )
    if kind == "webhook" and routes:
        # The most expensive misconfiguration available here, because it looks
        # like it is working: messages arrive, just never where the file says.
        named = sorted({c for r in routes for c in r.channels})
        problems.append(
            f"{WEBHOOK_ENV} is an incoming webhook bound to one channel, so the "
            f"{len(routes)} configured route(s) cannot direct anything to "
            f"{', '.join(named)} — set {BOT_TOKEN_ENV} to route by channel"
        )
    if kind == "bot" and not routes and not default_channel():
        problems.append(
            f"{BOT_TOKEN_ENV} is set but no channel is — set {CHANNEL_ENV} or {ROUTES_ENV}"
        )

    return {
        "transport": kind,
        "routes": routes,
        "route_error": route_error,
        "default_channel": default_channel(),
        "problems": problems,
    }


# ------------------------------------------------------------------ interactions


def signing_secret() -> str | None:
    value = env.get(SIGNING_SECRET_ENV, "").strip()
    return value or None


def verify_signature(body: bytes, timestamp: str | None, signature: str | None) -> bool:
    """Whether an interaction callback genuinely came from Slack.

    Slack signs `v0:{timestamp}:{body}` with the app's Signing Secret. The
    timestamp is inside the signed string precisely so it cannot be edited, and
    we still check its age: a signature stays valid forever, so without a window
    a captured request could be replayed back at us indefinitely.

    Unset secret returns False rather than True. The endpoint this guards exists
    to answer Slack, and answering anyone who asks is not a smaller version of
    that — it is a different thing.
    """
    secret = signing_secret()
    if not secret or not timestamp or not signature:
        return False
    try:
        age = abs(time.time() - int(timestamp))
    except (TypeError, ValueError):
        return False
    if age > SIGNATURE_MAX_AGE_S:
        return False

    base = b"v0:" + timestamp.encode() + b":" + body
    expected = "v0=" + hmac.new(secret.encode(), base, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)
