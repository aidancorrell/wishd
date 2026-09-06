"""Getting an alert to a human.

Detection was the easy half. Almost every monitoring tool that fails in practice
fails here -- not by missing the problem, but by delivering so badly that the
channel gets muted, after which it may as well not have detected anything.

Four rules, each of which is a test in `test_alerts.py`:

  **Only transitions.** A table broken since 02:00 is one alert, not one an hour.
  The decision is already made and stored by `checks.store_result`; this module
  reads it rather than re-deriving it, so the two cannot drift apart.

  **Recoveries are alerts.** "It is fixed" is as load-bearing as "it broke". A
  channel that only ever carries bad news is a channel people learn to dread, and
  an unresolved incident nobody closed is indistinguishable from an ongoing one.

  **One message per sweep.** Twelve breaches from one bad upstream is one
  notification with twelve lines. (Phase 04 can group by lineage and say *why*
  they are related; until then the honest grouping is "found together".)

  **Delivery must never break monitoring.** A 500 from Slack is a lost alert.
  Letting it raise would turn a lost alert into a lost monitoring sweep, which is
  strictly worse. Failures are recorded in `alerts` and swallowed.

Credentials are environment variables rather than YAML, because a Slack webhook
URL and a PagerDuty routing key are secrets and the monitor files are meant to be
committed. *Routing* — which channel a given alert belongs in — is the opposite:
a channel name is not a secret, and "why does finance get paged for this?" is a
question that deserves a diff. That half lives in `slack.py`, in a file you check
in.
"""

from __future__ import annotations

import logging
from typing import Any

import psycopg

from .config import env

log = logging.getLogger("dataspine.alerts")

# Kept as a re-export: `slack.py` owns Slack configuration now, but this name is
# in the CLI help, the README and every existing deployment's environment.
SLACK_ENV = "DATASPINE_SLACK_WEBHOOK"
PAGERDUTY_ENV = "DATASPINE_PAGERDUTY_ROUTING_KEY"
WEBHOOK_ENV = "DATASPINE_ALERT_WEBHOOK"

PAGERDUTY_URL = "https://events.pagerduty.com/v2/enqueue"

# Statuses worth telling a human about.
#
# `insufficient_data` is excluded and that exclusion is load-bearing: it is the
# status every monitor transitions into on its first evaluation, so without this
# a fresh `dataspine apply` would page the on-call once per new monitor about
# tables that are merely young.
ALERTABLE = ("breach", "ok", "error")

STATUS_HEADLINE = {
    "breach": "in breach",
    "ok": "recovered",
    "error": "monitor error",
}


def is_alertable(status: str) -> bool:
    return status in ALERTABLE


def configured_channels() -> list[str]:
    from . import slack as slack_mod

    channels = []
    if slack_mod.configured():
        channels.append("slack")
    if env.get(PAGERDUTY_ENV):
        channels.append("pagerduty")
    if env.get(WEBHOOK_ENV):
        channels.append("webhook")
    return channels


def transitions(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The subset of a sweep's results worth sending, breaches first.

    Ordering is not cosmetic. A grouped message is read top-down and often on a
    phone; what is broken has to come before what is fixed.
    """
    worth = [r for r in results if r.get("transitioned") and is_alertable(r["status"])]
    rank = {"breach": 0, "error": 1, "ok": 2}
    return sorted(worth, key=lambda r: (rank.get(r["status"], 3), r["monitor"]))


# ------------------------------------------------------------------ delivery


def deliver(
    results: list[dict[str, Any]],
    *,
    client: Any = None,
    conn: psycopg.Connection | None = None,
) -> int:
    """Send a sweep's transitions to every configured channel.

    Returns the number of successful deliveries. Never raises: see the module
    docstring. `conn` is optional so the delivery path can be tested and reasoned
    about without a database, but when it is supplied every attempt is recorded.
    """
    sending = transitions(results)
    channels = configured_channels()
    if not sending or not channels:
        return 0

    owns_client = client is None
    if owns_client:
        import httpx

        client = httpx.Client(timeout=10)

    delivered = 0
    try:
        for channel in channels:
            handler = _HANDLERS[channel]
            for attempt in handler(client, sending):
                delivered += 1 if attempt["delivered"] else 0
                if conn is not None:
                    _record(conn, channel, attempt)
    finally:
        if owns_client:
            client.close()
    return delivered


def _post(client: Any, url: str, payload: dict[str, Any], *, headers=None) -> dict[str, Any]:
    """One POST, reduced to delivered/error. The only place exceptions are caught."""
    try:
        response = client.post(url, json=payload, headers=headers or {})
    except Exception as exc:  # noqa: BLE001 - a lost alert must not lose the sweep
        log.warning("alert delivery failed: %s", exc)
        return {"delivered": False, "error": f"{type(exc).__name__}: {exc}"}

    status = getattr(response, "status_code", 0)
    if 200 <= status < 300:
        return {"delivered": True, "error": None}
    log.warning("alert delivery returned %s", status)
    return {"delivered": False, "error": f"HTTP {status}"}


def _slack(client: Any, sending: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Hand the sweep to `slack.py`, which decides which channel each line is for.

    Grouping survives the move: a sweep is still one message, it is now one
    message *per channel*. Routing sits here rather than inside the loop because
    a monitor transition and a failed run should be routable by the same rules —
    a team that wants its own channel wants everything about its tables in it,
    not only the half that happens to be a monitor.
    """
    from . import notify
    from . import slack as slack_mod

    notes = [
        notify.Notification(
            event="monitor",
            status=result["status"],
            title=f"{result['monitor']} — {STATUS_HEADLINE[result['status']]}",
            summary=result["message"],
            dedup_key=f"{result['monitor']}/{result['status']}",
            monitor=result["monitor"],
            fields=(("value", _fmt_value(result.get("value"))),),
            url=notify._link(f"/monitors/{result['monitor']}"),
        )
        for result in sending
    ]

    attempts = slack_mod.deliver(notes, client=client)
    if not attempts:
        # Configured, but every line was routed nowhere. Recording it keeps
        # "why did nobody get paged?" answerable, which is the entire reason
        # this table exists.
        return [
            dict(delivered=False, error="no matching route", monitor=r["monitor"],
                 status=r["status"])
            for r in sending
        ]

    # One message covers several monitors, so the audit rows fan back out: the
    # question being answered later is per-monitor, not per-message.
    rows = []
    for attempt in attempts:
        for note in attempt["notifications"]:
            rows.append(
                {
                    "delivered": attempt["delivered"],
                    "error": attempt["error"],
                    "monitor": note.monitor,
                    "status": note.status,
                }
            )
    return rows


def _fmt_value(value: Any) -> str:
    if value is None:
        return "—"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _pagerduty(client: Any, sending: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One event per monitor, unlike the chat channels.

    PagerDuty is an incident tracker: a grouped message would open one incident
    for twelve problems and leave eleven of them with no way to resolve. The
    `dedup_key` is derived from the monitor name so the recovery closes the exact
    incident the breach opened, rather than leaving a human to tidy up.
    """
    routing_key = env[PAGERDUTY_ENV]
    attempts = []
    for result in sending:
        resolving = result["status"] == "ok"
        payload = {
            "routing_key": routing_key,
            "dedup_key": f"dataspine/{result['monitor']}",
            "event_action": "resolve" if resolving else "trigger",
        }
        if not resolving:
            payload["payload"] = {
                "summary": f"{result['monitor']} — {result['message']}",
                "source": "dataspine",
                # A monitor that errored is our problem, not a data incident, and
                # waking someone at the same severity for both is how the
                # distinction stops being made.
                "severity": "error" if result["status"] == "breach" else "warning",
            }
        attempt = _post(client, PAGERDUTY_URL, payload)
        attempts.append(dict(attempt, monitor=result["monitor"], status=result["status"]))
    return attempts


def _webhook(client: Any, sending: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Generic JSON, one POST for the sweep.

    Deliberately the raw result shape rather than a prettified one: whoever wired
    this up wants to route on the fields, and a message string would force them
    to parse English back into data.
    """
    url = env[WEBHOOK_ENV]
    payload = {
        "source": "dataspine",
        "breaches": sum(1 for r in sending if r["status"] == "breach"),
        "alerts": [
            {
                "monitor": r["monitor"],
                "status": r["status"],
                "headline": STATUS_HEADLINE[r["status"]],
                "message": r["message"],
                "value": r.get("value"),
            }
            for r in sending
        ],
    }
    attempt = _post(client, url, payload)
    return [dict(attempt, monitor=r["monitor"], status=r["status"]) for r in sending]


_HANDLERS = {"slack": _slack, "pagerduty": _pagerduty, "webhook": _webhook}


def _record(conn: psycopg.Connection, channel: str, attempt: dict[str, Any]) -> None:
    """Write the audit row. Best-effort by design.

    An audit failure must not escalate into a monitoring failure -- the whole
    point of this table is to explain outages, not to cause them.
    """
    try:
        conn.execute(
            """
            insert into alerts (monitor_id, result_id, status, channel, delivered, error)
            select m.id,
                   (select id from monitor_results
                     where monitor_id = m.id order by evaluated_at desc, id desc limit 1),
                   %(status)s, %(channel)s, %(delivered)s, %(error)s
            from monitors m where m.name = %(monitor)s
            """,
            {
                "monitor": attempt["monitor"],
                "status": attempt["status"],
                "channel": channel,
                "delivered": attempt["delivered"],
                "error": attempt["error"],
            },
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("could not record alert delivery: %s", exc)


def recent(conn: psycopg.Connection, *, limit: int = 50) -> list[dict[str, Any]]:
    return conn.execute(
        """
        select a.*, m.name as monitor
        from alerts a left join monitors m on m.id = a.monitor_id
        order by a.created_at desc, a.id desc
        limit %s
        """,
        (limit,),
    ).fetchall()
