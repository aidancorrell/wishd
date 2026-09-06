"""What is worth telling a human, beyond a monitor changing status.

Phase 03 built alerting around one shape of event: a monitor result that
*transitioned*. That shape carries its own dedup decision — `monitor_results`
already stores whether the status changed — and everything in `alerts.py` leans
on it.

The events here have no such column to lean on, because they are not repeated
evaluations of a stateful thing:

  **A pipeline run failed.** The most direct question a data team has, and the
  one dataspine was in the best position to answer all along: it already holds
  the correlated run tree, so it can say *which task* failed and *what it said*,
  not merely that a DAG went red.

  **An incident opened.** `incidents.py` already decides which breaches are one
  event and which of them is the cause. Delivering the cause with its blast
  radius is a strictly better page than the per-monitor line that goes out today.

  **A digest.** The counterweight to everything else in this module. A channel
  that only ever speaks when something is wrong gives you no way to tell "quiet"
  from "broken and silent", and the digest is what makes silence readable.

Dedup for all three is the `notifications` ledger, claimed before sending, with
a bounded retry on top. That combination is the important decision in this file,
and it arrived in two steps.

Claiming first is what makes two overlapping cron runs safe, but on its own it
loses a notification whenever delivery fails. Recording *after* delivery instead
would repeat one every sweep for as long as Slack stayed unhappy, and a repeating
alert gets the channel muted — which loses every future alert too, so that cure
is worse than the disease.

The way out is that the bad case comes from *unbounded* retry. `claim` grants the
key again while the notification is undelivered, recent and under
`MAX_DELIVERY_ATTEMPTS`, so one 500 or one 429 no longer eats a page, and a
channel that has been misconfigured for a week still cannot replay the same
message forever. Past `RETRY_WINDOW_HOURS` we stop trying on purpose: a pipeline
that failed and was fixed hours ago is history, and delivering it late is worse
than not delivering it.

Three rules the run-failure path follows, each of which is a test:

  **One notification per pipeline, not per task.** A failed dbt model fails the
  Airflow task above it and the Spark job below it. That is one problem with
  three run rows, so the notification is keyed on the root run and names the
  deepest failure as the cause — the same cause-and-blast-radius shape
  `incidents.py` uses for monitors.

  **A retry that succeeds is not a failure.** Airflow retries tasks. A root run
  that has since reached COMPLETED is not worth waking anyone for, and the grace
  period exists so a retry has a moment to land before we decide.

  **Never notify about history.** A first run against an existing database, or a
  replay, must not page anyone about failures from six weeks ago; `since` bounds
  what is even considered.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg

from . import agents, queries
from .config import env

log = logging.getLogger("dataspine.notify")

BASE_URL_ENV = "DATASPINE_BASE_URL"

# How long a failure is allowed to be a retry before we believe it. Airflow's
# default retry_delay is five minutes; paging inside that window is paging about
# something the scheduler was already fixing.
DEFAULT_GRACE_MINUTES = 5

# How far back a sweep looks. Generous relative to any sane cron interval,
# because the ledger -- not the window -- is what prevents repeats, and a window
# shorter than the gap between runs would silently drop failures.
DEFAULT_SINCE_HOURS = 24

# Bounds on retrying a delivery that failed. Small on purpose, on both axes: the
# point is to survive one bad response, not to let a channel that has been
# misconfigured for a week replay the same message on every sweep.
MAX_DELIVERY_ATTEMPTS = 3

# Past this a notification is history rather than news, and delivering it late is
# worse than not delivering it — an alert about a pipeline that failed and was
# fixed hours ago is the kind that teaches people to ignore the channel.
RETRY_WINDOW_HOURS = 1


@dataclass(frozen=True)
class Notification:
    """One thing worth saying, in the vocabulary routing matches on.

    Deliberately transport-agnostic: `slack.py` renders it, and the shape carries
    no Slack concepts so a second destination does not require reopening this.
    """

    EVENTS = (
        "monitor", "run_failure", "incident", "digest", "pipeline", "data_test",
        "dbt_job",
    )

    event: str
    status: str
    title: str
    summary: str
    dedup_key: str
    monitor: str | None = None
    job: str | None = None
    # The table a check asserted on, so a team can route its own tables.
    dataset: str | None = None
    # Which producers reported a failure in this tree -- AIRFLOW, DBT, SPARK.
    # A set rather than one value, because "what broke" is genuinely plural on a
    # dbt-on-Spark stack: the model failed and so did the Spark job under it, and
    # routing on only the deepest would file a dbt failure as a Spark one.
    integrations: tuple[str, ...] = ()
    fields: tuple[tuple[str, str], ...] = ()
    url: str | None = None
    # Where else this can be opened: `(label, url)` pairs for the system that
    # actually ran the thing — dbt Cloud, the Spark UI, Airflow, Snowsight.
    #
    # dataspine's own link is `url` and stays separate because it is the one link
    # that always exists. These come from `links.py`, which builds them only from
    # what a producer reported or an operator configured; an empty tuple means
    # nothing could be linked honestly, and the message simply says less.
    links: tuple[tuple[str, str], ...] = ()
    # Things to *do* about this, as `(label, url)` — handing the failure to a
    # coding agent. Separate from `links` because the two answer different
    # questions: a link goes and looks at the thing, an action starts work on
    # it, and a reader scanning for one should not have to read past the other.
    #
    # Still `(label, url)` rather than anything Slack-shaped, so this stays as
    # transport-agnostic as the rest: `slack.py` renders them as buttons, and a
    # second destination is free to render them as a list. See `agents.py`.
    actions: tuple[tuple[str, str], ...] = ()
    # Detail that belongs under this message rather than beside it. One reply
    # per item, so each is something a person can react to on its own while the
    # channel still shows a single line.
    thread: tuple[Notification, ...] = ()


def _base_url() -> str | None:
    url = env.get(BASE_URL_ENV, "").strip().rstrip("/")
    # No guessed links, for the reason `links.py` states: a URL that 404s costs
    # someone a click and their trust in every other link on the page.
    return url or None


def _link(path: str) -> str | None:
    base = _base_url()
    return f"{base}{path}" if base else None


def _external(*candidates: Any) -> tuple[tuple[str, str], ...]:
    """`links.py` entries reduced to the `(label, url)` pairs a message can use.

    Only the ones that are actually links. `links.py` also returns copyable
    references — an application id, a dbt invocation — which earn their place on
    a run page you are already reading but not in an alert, where every line
    competes with the sentence saying what broke.

    De-duplicated, because a tree spanning Airflow and dbt legitimately produces
    the same dataspine link twice.
    """
    seen: dict[str, str] = {}
    for candidate in candidates:
        for item in candidate if isinstance(candidate, list) else [candidate]:
            if not isinstance(item, dict):
                continue
            url, label = item.get("url"), item.get("label")
            if isinstance(url, str) and url and isinstance(label, str) and url not in seen:
                seen[url] = label
    return tuple((label, url) for url, label in seen.items())


def _run_facets(row: dict[str, Any]) -> dict[str, Any]:
    """A `runs.facets` column that may arrive as JSON text or already parsed."""
    facets = row.get("facets")
    if isinstance(facets, str):
        try:
            facets = json.loads(facets)
        except ValueError:
            return {}
    return facets if isinstance(facets, dict) else {}


# ------------------------------------------------------------------ run failures


def run_failures(
    conn: psycopg.Connection,
    *,
    since: datetime | None = None,
    now: datetime | None = None,
    grace_minutes: float = DEFAULT_GRACE_MINUTES,
    limit: int = 50,
) -> list[Notification]:
    """Failed pipeline executions in the window, one notification per root run."""
    now = now or datetime.now(UTC)
    since = since or now - timedelta(hours=DEFAULT_SINCE_HOURS)
    cutoff = now - timedelta(minutes=grace_minutes)

    rows = conn.execute(
        """
        with failed as (
            select r.run_id,
                   coalesce(r.root_run_id, r.run_id) as root_id,
                   r.depth,
                   r.error_message,
                   r.facets,
                   coalesce(r.ended_at, r.updated_at) as failed_at,
                   j.name as job_name,
                   j.integration
            from runs r
            join jobs j on j.id = r.job_id
            where r.state in ('FAILED', 'ABORTED')
              and not r.is_placeholder
              and coalesce(r.ended_at, r.updated_at) >= %(since)s
              and coalesce(r.ended_at, r.updated_at) <= %(cutoff)s
        )
        select f.root_id,
               coalesce(root_job.name, deepest.job_name) as root_job,
               root_run.state as root_state,
               count(*) as failed_runs,
               max(f.failed_at) as failed_at,
               deepest.job_name as leaf_job,
               deepest.integration as leaf_integration,
               array_remove(array_agg(distinct f.integration), null) as integrations,
               deepest.error_message,
               -- Facets from both ends of the tree, because they link to
               -- different useful places: the root is what *scheduled* the work
               -- (the Airflow DAG run), the deepest failure is what *broke*
               -- (the dbt Cloud run, the Spark UI). A reader wants whichever is
               -- nearer their question, so the message offers both.
               root_job.integration as root_integration,
               root_run.facets as root_facets,
               deepest.facets as leaf_facets
        from failed f
        left join runs root_run on root_run.run_id = f.root_id
        left join jobs root_job on root_job.id = root_run.job_id
        join lateral (
            -- The cause is the deepest failure, and among equals the one that
            -- actually said something: an Airflow task reporting "task failed"
            -- above a Spark job reporting the stack trace is the less useful of
            -- the two to put in the message.
            select job_name, integration, error_message, facets
            from failed d
            where d.root_id = f.root_id
            order by (d.error_message is null), d.depth desc, d.failed_at desc
            limit 1
        ) deepest on true
        -- A root that has since completed was a retry that worked. Paging for it
        -- is how a channel learns that dataspine cries wolf.
        where coalesce(root_run.state, 'UNKNOWN') <> 'COMPLETED'
          -- ...and a dbt invocation that already got its own message with the
          -- failures in the thread does not also need "the pipeline failed".
          -- Two notifications for one event is how a reader learns that the
          -- second one is never worth opening.
          and not exists (
              select 1 from notifications n
              where n.event = 'dbt_job' and n.dedup_key = f.root_id::text
          )
        group by f.root_id, root_job.name, root_job.integration, root_run.state,
                 root_run.facets, deepest.job_name, deepest.integration,
                 deepest.error_message, deepest.facets
        order by max(f.failed_at) desc
        limit %(limit)s
        """,
        {"since": since, "cutoff": cutoff, "limit": limit},
    ).fetchall()

    return [_run_failure_notification(row) for row in rows]


def _run_failure_notification(row: dict[str, Any]) -> Notification:
    root_job = row["root_job"] or "unknown pipeline"
    leaf = row["leaf_job"]
    failed = row["failed_runs"]

    if leaf and leaf != root_job:
        summary = f"`{leaf}` failed"
        if failed > 1:
            summary += f", and {failed - 1} run(s) above it"
    else:
        summary = "failed"

    error = (row.get("error_message") or "").strip()
    if error:
        # First line only. A page is a decision aid, not a log viewer, and the
        # link is right there for the rest of the stack trace.
        summary += f"\n```{error.splitlines()[0][:400]}```"

    fields = [("failed", _ago(row["failed_at"]))]
    if row.get("leaf_integration"):
        fields.append(("where", str(row["leaf_integration"]).lower()))
    if failed > 1:
        fields.append(("failed runs", str(failed)))

    from . import links as links_mod

    return Notification(
        event="run_failure",
        status="failed",
        title=f"{root_job} failed",
        summary=summary,
        dedup_key=str(row["root_id"]),
        job=root_job,
        integrations=tuple(row.get("integrations") or ()),
        fields=tuple(fields),
        url=_link(f"/runs/{row['root_id']}"),
        links=_external(
            links_mod.run_links(
                row.get("root_integration"), _run_facets({"facets": row.get("root_facets")})
            ),
            links_mod.run_links(
                row.get("leaf_integration"), _run_facets({"facets": row.get("leaf_facets")})
            ),
        ),
    )


def _ago(when: datetime | None) -> str:
    if when is None:
        return "—"
    delta = datetime.now(UTC) - when
    minutes = int(delta.total_seconds() // 60)
    if minutes < 1:
        return "just now"
    if minutes < 60:
        return f"{minutes}m ago"
    if minutes < 60 * 24:
        return f"{minutes // 60}h ago"
    return f"{minutes // (60 * 24)}d ago"


# -------------------------------------------------------------------- incidents


def incident_notifications(
    conn: psycopg.Connection, *, now: datetime | None = None
) -> list[Notification]:
    """Open incidents as notifications: the cause, and what it took down.

    Persisting is what makes the id stable, and the id is the dedup key — so an
    incident that stays open across sweeps is one message, not one an hour.
    """
    from . import incidents as incidents_mod

    current = incidents_mod.detect(conn, now=now, persist=True)
    notes = []
    for incident in current:
        downstream = len(incident["consequences"])
        subject = incident["cause_entity_name"] or incident["cause_monitor"]
        summary = incident["message"] or "in breach"
        if downstream:
            summary += (
                f"\n_{downstream} downstream monitor{'s' if downstream != 1 else ''} "
                f"also breaching — suppressed as consequences of this._"
            )
        notes.append(
            Notification(
                event="incident",
                status="open",
                title=f"Incident: {subject}",
                summary=summary,
                dedup_key=str(incident["id"]),
                monitor=incident["cause_monitor"],
                fields=(
                    ("cause", incident["cause_monitor"]),
                    ("blast radius", f"{downstream} monitor(s)"),
                ),
                url=_link("/incidents"),
            )
        )
    return notes


# ------------------------------------------------------------------- dbt jobs


def dbt_job_notification(
    invocation: Any,
    *,
    job_name: str | None = None,
    run_id: Any = None,
    run_facets: dict[str, Any] | None = None,
) -> Notification | None:
    """One message per dbt invocation that had something go wrong, failures in
    its thread. Returns None for a clean run.

    A clean run says nothing on purpose. Most teams run several dbt jobs -- a
    build, an hourly incremental, a test-only pass -- and a green line from each
    of them is a channel nobody reads by the second week. A successful dbt Cloud
    run still turns its live-feed message green, which is where "everything ran"
    belongs.

    The shape is a summary plus one reply per failure rather than one message
    listing them all, because a reply is something a person can react to, assign
    themselves or answer under. That is the difference between an alert that gets
    read and one that gets picked up.
    """
    from . import dbt_artifacts
    from . import links as links_mod

    failed = invocation.failed
    if not failed:
        return None

    root = run_id or dbt_artifacts.run_id_for(invocation.invocation_id)
    name = dbt_artifacts.root_job_name(invocation, job_name)
    display = job_name or name

    models = [n for n in failed if not n.is_test]
    tests = [n for n in failed if n.is_test and not n.warn_only]
    warned = [n for n in failed if n.warn_only]

    # Warnings are excluded from the count on the front. They are real and they
    # are in the thread, but their authors said in writing that they were not
    # worth waking anyone -- so they must not inflate the number that decides
    # whether someone opens this at 3am.
    breakages = len(models) + len(tests)
    parts = []
    if models:
        parts.append(f"*{len(models)}* model(s) failed")
    if tests:
        parts.append(f"*{len(tests)}* test(s) failed")
    if warned:
        parts.append(f"{len(warned)} warning(s)")

    counted = [
        f"{len(invocation.models)} model(s)",
        f"{len(invocation.tests)} test(s)",
    ]
    summary = " · ".join(parts) + "\n" + f"_dbt {invocation.command or 'run'} · " + \
        " · ".join(counted) + (f" · {_duration_s(invocation.elapsed)}_" if invocation.elapsed
                               else "_")

    return Notification(
        event="dbt_job",
        status="failed",
        title=(f"{'❌' if breakages else '⚠️'} dbt · {display} — "
               f"{breakages} failure{'s' if breakages != 1 else ''}"),
        summary=summary,
        # Keyed on the synthesised root run, which is derived from dbt's own
        # invocation_id -- so the webhook and the backstop poll seeing the same
        # run agree on one message, and `run_failures` can find it to stand down.
        dedup_key=str(root),
        job=name,
        integrations=("DBT",),
        fields=(("command", invocation.command or "run"),),
        url=_link(f"/runs/{root}"),
        links=_external(links_mod.run_links("DBT", run_facets or {})),
        thread=tuple(_dbt_node_notification(n, invocation, root) for n in failed),
    )


def _dbt_node_notification(node: Any, invocation: Any, root: Any) -> Notification:
    from . import dbt_artifacts
    from . import links as links_mod

    warn = node.warn_only
    icon = "⚠️" if warn else "❌"
    kind = "test" if node.is_test else node.resource_type

    table = dbt_artifacts.tested_relation(invocation, node) if node.is_test else node.relation

    detail = []
    if node.is_test:
        where = f" in `{node.column}`" if node.column else ""
        if node.failures:
            detail.append(f"*{node.failures:,}* failing row(s){where}")
        if table:
            detail.append(f"on `{table.rsplit('.', 1)[-1]}`")
    if node.message:
        # First line only: a thread reply is a decision aid, and the link is
        # right there for the stack trace.
        detail.append(f"```{node.message.splitlines()[0][:400]}```")

    fields = [("severity", node.severity)] if warn else []
    dedup_key = f"{root}/{node.unique_id}"
    return Notification(
        event="dbt_job",
        status="warn" if warn else "failed",
        title=f"{icon}  {kind}  {node.name}",
        summary="\n".join(detail) or "failed",
        dedup_key=dedup_key,
        job=dbt_artifacts.root_job_name(invocation),
        dataset=table,
        integrations=("DBT",),
        fields=tuple(fields),
        links=_external(
            links_mod.snowflake_query(node.query_id),
            # Optional second Snowflake link, off by default: the table's own
            # page in Snowsight. The query link above is strictly better when it
            # exists -- it carries the SQL, the results and the profile -- so
            # this earns its place only on adapters that report no `query_id`
            # (Postgres does not), where it is the sole warehouse link available.
            # Uncomment to always offer both; `MAX_LINKS` still caps the line.
            # links_mod.snowflake_table(table),
        ),
        # Never on a warning. The author of a `severity: warn` test said in
        # writing that they did not want waking for it, and offering to put an
        # agent on it is a louder version of the same interruption.
        actions=(
            ()
            if warn
            else agents.handoff_urls(
                "dbt_job",
                dedup_key,
                _base_url(),
                # Built here rather than at click time so the Claude Code button
                # can be the deep link itself. No connection on this path -- it
                # is a pure function over the invocation -- so the briefing names
                # upstreams from dbt's own graph and claims nothing about which
                # of them failed.
                briefing=agents.briefing_from_node(node, invocation),
            )
        ),
    )


def _duration_s(seconds: float | None) -> str:
    if not seconds:
        return "—"
    total = int(seconds)
    if total < 60:
        return f"{total}s"
    return f"{total // 60}m{total % 60:02d}s"


def dbt_artifacts_run_id(invocation: Any) -> Any:
    from . import dbt_artifacts

    return dbt_artifacts.run_id_for(invocation.invocation_id)


def _facets_of(conn: psycopg.Connection, run_id: Any) -> dict[str, Any]:
    """One run's facets, or `{}`. Never raises: a missing link is not a reason
    to lose the alert it would have decorated."""
    try:
        row = conn.execute(
            "select facets from runs where run_id = %s", (str(run_id),)
        ).fetchone()
    except Exception as exc:  # noqa: BLE001
        log.debug("could not read facets for run %s: %s", run_id, exc)
        return {}
    return _run_facets(dict(row)) if row else {}


def announce_dbt_job(
    conn: psycopg.Connection,
    invocation: Any,
    *,
    job_name: str | None = None,
    run_id: Any = None,
    client: Any = None,
) -> dict[str, Any]:
    """Build and send the job message. Never raises.

    Called on the ingest path rather than from a cron sweep, because "as soon as
    the job finished" is the whole point -- a webhook that has already done the
    work of fetching and parsing should not then wait five minutes for a poller
    to notice.
    """
    try:
        note = dbt_job_notification(
            invocation, job_name=job_name, run_id=run_id,
            # Read back rather than passed in: the events carrying this facet
            # were written moments ago by the same transaction, and reading the
            # run means the courier does not have to hand the same thing to two
            # different functions to keep them in agreement.
            run_facets=_facets_of(conn, run_id or dbt_artifacts_run_id(invocation)),
        )
        if note is None:
            return {"sent": [], "skipped": [], "failed": []}
        return send(conn, [note], client=client)
    except Exception as exc:  # noqa: BLE001 - a lost alert must not lose the ingest
        log.warning("could not announce dbt job: %s", exc)
        return {"sent": [], "skipped": [], "failed": [], "error": str(exc)}


# -------------------------------------------------------------------- dq tests


def data_test_notifications(
    conn: psycopg.Connection,
    *,
    since: datetime | None = None,
    now: datetime | None = None,
) -> list[Notification]:
    """dbt tests, and any other data-quality check, that changed verdict.

    This is the half of "is the data right?" the monitors cannot see. They watch
    freshness, volume, schema and column statistics; a `not_null` on a column
    nobody thought to profile is invisible to every one of them, and it is
    usually the assertion the analytics team actually wrote down.

    Warnings never become notifications. A dbt test with `severity: warn` is one
    whose author said, in writing, that they did not want waking -- delivering it
    anyway is how a team learns to mute the channel that also carries the errors.
    They are still recorded, and still visible in the UI and the digest.
    """
    from . import dq
    from . import links as links_mod

    now = now or datetime.now(UTC)
    since = since or now - timedelta(hours=DEFAULT_SINCE_HOURS)

    notes = []
    for row in dq.transitions(conn, since=since):
        if row["source"] == "dbt":
            # dbt's checks arrive with an invocation around them, and that
            # invocation gets one message with every failure in its thread.
            # Repeating them flat here would say the same thing twice, in the
            # shape the threading exists to avoid.
            continue
        details = row.get("details") or {}
        # Never, in either direction. Announcing the *recovery* of something we
        # deliberately never mentioned breaking is worse than noise -- it is a
        # message about an event the reader has no record of.
        if details.get("severity") == "warn":
            continue

        table = row["table_name"]
        leaf = table.rsplit(".", 1)[-1]
        recovered = row["status"] == "pass"
        failures = row.get("value")

        if recovered:
            summary = "passing again"
        else:
            summary = f"*{row['check_name']}* failed"
            if failures:
                summary += f" on *{failures:,.0f}* row(s)"
            if details.get("column"):
                summary += f" in `{details['column']}`"
            if details.get("message"):
                summary += f"\n{details['message']}"

        fields = [("check", row["check_name"]), ("source", row["source"])]
        if details.get("test_type"):
            fields.append(("test", str(details["test_type"])))

        dedup_key = (
            f"{row['source']}/{table}/{row['check_name']}/{row['measured_at'].isoformat()}"
        )
        notes.append(
            Notification(
                event="data_test",
                status="ok" if recovered else "fail",
                title=f"{leaf} — {'test recovered' if recovered else 'test failed'}",
                summary=summary,
                # The measurement time, not the check name alone: the same test
                # failing again after a recovery is genuinely new news, and
                # keying on the check would swallow it forever.
                dedup_key=dedup_key,
                dataset=table,
                fields=tuple(fields),
                url=_link("/catalog"),
                # A recovery omits the query: the statement it
                # links to is the one that *passed*, and sending someone to look
                # at a clean result is a wasted click.
                links=_external(
                    links_mod.snowflake_query(details.get("query_id"))
                    if not recovered
                    else None,
                    # Optional, off by default -- see the note in
                    # `_dbt_node_notification`. Worth more on this path than on
                    # that one: a check forwarded from a Snowflake DMF or a
                    # Databricks rule may carry no `query_id` at all, leaving the
                    # alert with no warehouse link unless this is enabled.
                    # links_mod.snowflake_table(table),
                ),
                # A recovery offers no agent, for the same reason it offers no
                # query: there is nothing left to investigate, and a button
                # saying otherwise invites someone to go and look at a pass.
                actions=(
                    ()
                    if recovered
                    else agents.handoff_urls(
                        "data_test",
                        dedup_key,
                        _base_url(),
                        # Full briefing, lineage included: this path has a
                        # connection, so the deep link carries what the page
                        # would have built.
                        briefing=agents.briefing_from_check_row(conn, row),
                    )
                ),
            )
        )
    return notes


# ----------------------------------------------------------------------- digest


def digest(
    conn: psycopg.Connection, *, hours: float = 24, now: datetime | None = None
) -> Notification:
    """A summary of the window: what ran, what broke, what is still broken.

    Always returns a notification, including the quiet one. "Nothing broke" is
    the whole point — it is the difference between a healthy channel and a dead
    one, and it is the only evidence anyone gets that dataspine is still running.
    """
    now = now or datetime.now(UTC)
    since = now - timedelta(hours=hours)

    runs = conn.execute(
        """
        select count(*) as executions,
               count(*) filter (where state = 'RUNNING') as running
        from runs
        where parent_run_id is null
          and not is_placeholder
          and coalesce(started_at, created_at) >= %(since)s
        """,
        {"since": since},
    ).fetchone()

    # Counted the same way the failure alerts count, so the digest cannot say
    # "0 failures" on a morning when three alerts went out.
    failed = conn.execute(
        """
        select count(distinct coalesce(root_run_id, run_id)) as failed
        from runs
        where state in ('FAILED', 'ABORTED')
          and not is_placeholder
          and coalesce(ended_at, updated_at) >= %(since)s
        """,
        {"since": since},
    ).fetchone()["failed"]

    monitors = conn.execute(
        """
        select count(*) filter (where last_status = 'breach') as breaching,
               count(*) as total
        from monitors where enabled
        """
    ).fetchone()

    open_incidents = conn.execute(
        "select count(*) as open from incidents where resolved_at is null"
    ).fetchone()["open"]

    recovered = conn.execute(
        """
        select count(*) as recovered
        from monitor_results
        where transitioned and status = 'ok' and evaluated_at >= %(since)s
        """,
        {"since": since},
    ).fetchone()["recovered"]

    window = f"{int(hours)}h" if float(hours).is_integer() else f"{hours}h"
    healthy = not failed and not monitors["breaching"] and not open_incidents

    lines = [
        f"*{runs['executions']}* pipeline execution(s), "
        f"*{failed}* with a failure, *{runs['running']}* still running",
        f"*{monitors['breaching']}* of {monitors['total']} monitor(s) in breach, "
        f"*{recovered}* recovered in the window",
    ]
    if open_incidents:
        lines.append(f"*{open_incidents}* open incident(s)")
    if healthy:
        lines.append("_Nothing is currently broken._")

    return Notification(
        event="digest",
        status="ok" if healthy else "attention",
        title=f"Last {window}",
        summary="\n".join(lines),
        # Bucketed by the hour so a cron that fires twice, or a retried CI job,
        # does not post the same summary again.
        dedup_key=f"{window}/{now:%Y-%m-%dT%H}",
        fields=(("failures", str(failed)), ("breaching", str(monitors["breaching"]))),
        url=_link("/overview"),
    )


# --------------------------------------------------------------- live pipelines

# How long a RUNNING pipeline may go without any event before the feed stops
# calling it healthy. OpenLineage has no heartbeat, so a cluster that dies
# mid-run leaves its runs RUNNING forever; silence is the only evidence there is.
STALE_AFTER_MINUTES = 45

# How long after a pipeline ends its message stays editable. Generous, because
# the only cost of a late close is one extra row considered per sweep, and the
# cost of closing early is a message frozen mid-run forever.
TRACK_FOR_HOURS = 12

# How far back to look for executions that finished without ever being seen
# running. Not an edge case: a dbt run of a handful of models finishes in
# seconds, so on any cron interval at all there are pipelines that begin and end
# between two sweeps. Without this the feed would silently never mention them --
# the fast, healthy pipelines would be exactly the ones it failed to report.
RECENT_COMPLETION_MINUTES = 30

_STATE_ICON = {"running": "🔄", "completed": "✅", "failed": "❌", "stalled": "⚠️"}


def pipeline_feed(
    conn: psycopg.Connection,
    *,
    now: datetime | None = None,
    tracking: set[str] | None = None,
    limit: int = 50,
) -> list[Notification]:
    """One notification per pipeline execution, describing it as it is right now.

    Called repeatedly. The same execution yields the same `dedup_key` every time
    and different content as it progresses, which is what lets `track` edit one
    message instead of posting a stream of them.

    `tracking` names executions with a message already open. They are fetched
    whatever their state, because an execution leaves "in flight" at the exact
    moment its message most needs its final update.
    """
    now = now or datetime.now(UTC)
    roots = set(tracking or ()) | _recently_ended(conn, now=now, limit=limit)
    rows = queries.live_pipelines(conn, limit=limit, root_ids=sorted(roots))
    return [_pipeline_notification(row, now=now) for row in rows]


def _recently_ended(
    conn: psycopg.Connection, *, now: datetime, limit: int
) -> set[str]:
    """Executions that finished lately, whether or not we ever saw them running.

    The ledger, not this window, is what stops a pipeline being announced twice:
    anything already posted fails its claim and is skipped. The window only
    bounds how much history a first sweep against an existing database is willing
    to talk about, which is the same rule `run_failures` follows.
    """
    rows = conn.execute(
        """
        select run_id
        from runs
        where parent_run_id is null
          and not is_placeholder
          and state in ('COMPLETED', 'FAILED', 'ABORTED')
          and coalesce(ended_at, updated_at) >= %(cutoff)s
        order by coalesce(ended_at, updated_at) desc
        limit %(limit)s
        """,
        {"cutoff": now - timedelta(minutes=RECENT_COMPLETION_MINUTES), "limit": limit},
    ).fetchall()
    return {str(row["run_id"]) for row in rows}


def _pipeline_notification(row: dict[str, Any], *, now: datetime) -> Notification:
    job = row["job_name"] or "unknown pipeline"
    done, total = row["done"] or 0, row["total"] or 0
    failed = row["failed"] or 0
    running = row["running"] or 0

    stale = _is_stale(row["last_event_at"], now=now)
    if running:
        status = "stalled" if stale else "running"
    else:
        status = "failed" if failed else "completed"

    lines = []
    if status in ("running", "stalled"):
        # "8 of 11 so far", never a percentage: OpenLineage has no "about to run"
        # event, so the tree grows as it executes and a forecast would be a lie.
        lines.append(f"*{done} of {total}* steps so far · started {_clock(row['started_at'])}")
    else:
        lines.append(f"*{done} of {total}* steps · {_duration(row['duration_ms'])}")
    if failed:
        lines.append(f"*{failed}* step(s) failed")
    if stale:
        lines.append(
            f"_No events for {_ago(row['last_event_at'])} — the cluster may be gone._"
        )

    fields = [("steps", f"{done}/{total}")]
    if row.get("integrations"):
        fields.append(("where", str(row["integrations"]).lower()))

    from . import links as links_mod

    return Notification(
        event="pipeline",
        status=status,
        title=f"{_STATE_ICON[status]}  {job} — {status}",
        summary="\n".join(lines),
        dedup_key=str(row["run_id"]),
        job=job,
        integrations=tuple(
            i for i in str(row.get("integrations") or "").split(",") if i
        ),
        fields=tuple(fields),
        url=_link(f"/runs/{row['run_id']}"),
        # The live feed is the one place this matters most: a pipeline someone is
        # watching go past is one they may want to open where it is running, and
        # the message is edited in place so the link appears the moment the first
        # event carries it.
        links=_external(links_mod.run_links(row.get("integration"), _run_facets(row))),
    )


def _is_stale(last_event_at: datetime | None, *, now: datetime) -> bool:
    if last_event_at is None:
        return False
    return (now - last_event_at) > timedelta(minutes=STALE_AFTER_MINUTES)


def _clock(when: datetime | None) -> str:
    """A wall-clock time, deliberately not a live-ticking elapsed.

    An elapsed that re-renders every minute would change the content hash every
    minute, and every sweep would then call `chat.update` to move a number by
    one — burning rate limit to achieve nothing. Progress is what makes the feed
    feel alive, and progress genuinely changes.
    """
    return "an unknown time" if when is None else f"{when:%H:%M} UTC"


def _duration(ms: float | None) -> str:
    if not ms:
        return "—"
    seconds = int(ms // 1000)
    if seconds < 60:
        return f"{seconds}s"
    return f"{seconds // 60}m{seconds % 60:02d}s"


def is_terminal(note: Notification) -> bool:
    """A stalled pipeline is not terminal: it may still report in and finish."""
    return note.status in ("completed", "failed")


def _content_hash(note: Notification) -> str:
    """What was rendered, so an unchanged sweep issues no update at all."""
    # Links are part of it: a dbt Cloud run that only becomes linkable once the
    # job finishes is a message whose *content* changed, and leaving them out
    # would freeze the feed's last update just short of its most useful state.
    payload = json.dumps(
        [note.title, note.summary, list(note.fields), list(note.links)],
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(payload.encode()).hexdigest()


# ----------------------------------------------------------------------- ledger


def claim(
    conn: psycopg.Connection,
    note: Notification,
    *,
    now: datetime | None = None,
) -> bool:
    """Take the key, or report that it is spent.

    One atomic upsert decides three things, and they are one statement because
    reading them apart would let two overlapping cron runs both conclude they
    should send:

      **First time** — the insert wins, `attempts` becomes 1, send it.
      **Already delivered** — the `where` on the update rejects it, skip.
      **Undelivered, recent, tries left** — `attempts` increments, send again.

    That last case is the retry migration 021 added. A single 500, or a 429
    during a burst, used to eat a page about a failed pipeline permanently — and
    it is transient by the time anybody reads the ledger. Both bounds stay tight,
    because the failure being avoided is a misconfigured channel replaying the
    same message on every sweep for a week.
    """
    now = now or datetime.now(UTC)
    row = conn.execute(
        """
        insert into notifications (event, dedup_key, title, attempts, created_at)
        values (%(event)s, %(key)s, %(title)s, 1, %(now)s)
        on conflict (event, dedup_key) do update
            set attempts = notifications.attempts + 1
          where not notifications.delivered
            and notifications.attempts < %(max_attempts)s
            and notifications.created_at >= %(cutoff)s
        returning id
        """,
        {
            "event": note.event,
            "key": note.dedup_key,
            "title": note.title,
            "max_attempts": MAX_DELIVERY_ATTEMPTS,
            # Stamped from the caller's clock rather than the database's, so the
            # age bound is measured against the same clock that set it. They are
            # the same clock in production and provably not in a test, which is
            # the cheapest place to find out that they can drift.
            "now": now,
            "cutoff": now - timedelta(hours=RETRY_WINDOW_HOURS),
        },
    ).fetchone()
    return row is not None


def record(
    conn: psycopg.Connection,
    note: Notification,
    *,
    destinations: list[str],
    delivered: bool,
    error: str | None,
) -> None:
    """Close out a claimed notification. Best-effort, like `alerts._record`:
    an audit failure must not escalate into a delivery failure."""
    try:
        conn.execute(
            """
            update notifications
            set destinations = %(destinations)s, delivered = %(delivered)s, error = %(error)s
            where event = %(event)s and dedup_key = %(key)s
            """,
            {
                "destinations": destinations,
                "delivered": delivered,
                "error": error,
                "event": note.event,
                "key": note.dedup_key,
            },
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("could not record notification delivery: %s", exc)


def recent(conn: psycopg.Connection, *, limit: int = 50) -> list[dict[str, Any]]:
    return conn.execute(
        "select * from notifications order by created_at desc, id desc limit %s", (limit,)
    ).fetchall()


# ------------------------------------------------------------------------ sweep


def send(
    conn: psycopg.Connection,
    notes: list[Notification],
    *,
    client: Any = None,
    force: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Claim, deliver and record. Returns what happened, for the CLI to print.

    Never raises: the same rule `alerts.deliver` follows, for the same reason.
    A delivery outage must not become a detection outage.
    """
    from . import slack

    summary: dict[str, Any] = {"sent": [], "skipped": [], "failed": [], "attempts": []}
    if not notes:
        return summary

    if not slack.configured():
        # Claiming with nowhere to send would burn the dedup keys, so the first
        # sweep after someone finally sets a token would report nothing to say.
        summary["skipped"] = [n.dedup_key for n in notes]
        return summary

    fresh = []
    for note in notes:
        if force or claim(conn, note, now=now):
            fresh.append(note)
        else:
            summary["skipped"].append(note.dedup_key)
    if not fresh:
        return summary

    attempts = slack.deliver(fresh, client=client)
    summary["attempts"] = attempts

    # A notification can fan out to several channels; it counts as delivered if
    # it reached at least one, and the error of the ones it missed is kept.
    # Keyed the way the ledger is keyed. A dedup key is only unique *within* an
    # event -- incident 1 and a run failure could both be "1" -- so keying this
    # on the key alone would merge two unrelated outcomes.
    outcomes: dict[tuple[str, str], dict[str, Any]] = {}
    for attempt in attempts:
        for note in attempt["notifications"]:
            outcome = outcomes.setdefault(
                (note.event, note.dedup_key),
                {"note": note, "destinations": [], "delivered": False, "errors": []},
            )
            if attempt["delivered"]:
                outcome["delivered"] = True
                outcome["destinations"].append(attempt["channel"])
            else:
                outcome["errors"].append(f"{attempt['channel']}: {attempt['error']}")

    for note in fresh:
        outcome = outcomes.get((note.event, note.dedup_key))
        if outcome is None:
            # Claimed, then routed nowhere. Recorded rather than dropped, because
            # "why did nobody get told?" has to be answerable, and "no route
            # matched" is a much faster answer than reading the routes file.
            record(conn, note, destinations=[], delivered=False, error="no matching route")
            summary["failed"].append(note.dedup_key)
            continue
        record(
            conn,
            note,
            destinations=outcome["destinations"],
            delivered=outcome["delivered"],
            error="; ".join(outcome["errors"]) or None,
        )
        target = summary["sent"] if outcome["delivered"] else summary["failed"]
        target.append(note.dedup_key)
    return summary


def open_messages(conn: psycopg.Connection, *, now: datetime | None = None) -> dict[str, dict]:
    """Pipeline notifications with a message still open for editing.

    Bounded by age as well as by `live`: an execution that never reported a
    terminal state would otherwise be reconsidered on every sweep forever, and
    the ledger is the largest table this feed touches.
    """
    now = now or datetime.now(UTC)
    rows = conn.execute(
        """
        select dedup_key, messages, content_hash
        from notifications
        where event = 'pipeline' and live and created_at >= %(cutoff)s
        """,
        {"cutoff": now - timedelta(hours=TRACK_FOR_HOURS)},
    ).fetchall()
    return {row["dedup_key"]: row for row in rows}


def _open_message(
    conn: psycopg.Connection,
    note: Notification,
    *,
    messages: list[dict[str, Any]],
    delivered: bool,
    error: str | None,
    live: bool,
) -> None:
    conn.execute(
        """
        update notifications
        set messages = %(messages)s::jsonb,
            destinations = %(destinations)s,
            delivered = %(delivered)s,
            error = %(error)s,
            live = %(live)s,
            content_hash = %(hash)s
        where event = %(event)s and dedup_key = %(key)s
        """,
        {
            "messages": json.dumps(messages),
            "destinations": [m.get("route") or m["channel"] for m in messages],
            "delivered": delivered,
            "error": error,
            "live": live,
            "hash": _content_hash(note),
            "event": note.event,
            "key": note.dedup_key,
        },
    )


def _edit_message(
    conn: psycopg.Connection, note: Notification, *, live: bool, error: str | None
) -> None:
    conn.execute(
        """
        update notifications
        set content_hash = %(hash)s, live = %(live)s, error = %(error)s,
            title = %(title)s
        where event = %(event)s and dedup_key = %(key)s
        """,
        {
            "hash": _content_hash(note),
            "live": live,
            "error": error,
            # The ledger answers "what did we tell people?". Leaving the title at
            # whatever the message said when it was first posted would have it
            # recording `running` for a pipeline that finished hours ago -- an
            # audit trail that disagrees with the channel it is auditing.
            "title": note.title,
            "event": note.event,
            "key": note.dedup_key,
        },
    )


def track(
    conn: psycopg.Connection,
    *,
    now: datetime | None = None,
    client: Any = None,
) -> dict[str, Any]:
    """Keep one Slack message per pipeline execution in step with the pipeline.

    Three cases, and the middle one is the whole point:

      **Not seen before** — post a message and remember where it landed.
      **Seen, still open, content changed** — edit that message in place. A
      pipeline advancing from 3 steps to 5 is not news worth a second message.
      **Seen, still open, content identical** — do nothing at all. Without this
      every sweep would call `chat.update` to rewrite the same text, burning
      rate limit and crowding out the updates that mattered.

    Never raises. A feed that can fail a cron is worse than a feed that is late.
    """
    from . import slack

    now = now or datetime.now(UTC)
    summary: dict[str, Any] = {
        "posted": [], "updated": [], "unchanged": [], "closed": [], "failed": [],
        "skipped": None,
    }

    if not slack.can_update():
        # Degrading would mean one message per step, which is the flood the feed
        # exists to prevent -- so it refuses, visibly, rather than quietly.
        summary["skipped"] = (
            f"a live feed needs {slack.BOT_TOKEN_ENV}: an incoming webhook cannot "
            f"edit a message it already sent"
        )
        return summary

    open_rows = open_messages(conn, now=now)
    notes = pipeline_feed(conn, now=now, tracking=set(open_rows))

    for note in notes:
        terminal = is_terminal(note)
        existing = open_rows.get(note.dedup_key)

        if existing is None:
            if not claim(conn, note, now=now):
                # Already closed, or out of retries. Either way it is not ours.
                continue
            attempts = slack.deliver([note], client=client)
            messages = [
                {
                    "channel": a["message_channel"],
                    "ts": a["message_ts"],
                    "route": a["channel"],
                }
                for a in attempts
                if a["delivered"] and a.get("message_ts")
            ]
            delivered = bool(messages)
            errors = "; ".join(
                f"{a['channel']}: {a['error']}" for a in attempts if not a["delivered"]
            )
            _open_message(
                conn, note,
                messages=messages,
                delivered=delivered,
                error=errors or (None if attempts else "no matching route"),
                # Only a message that exists can be edited later.
                live=delivered and not terminal,
            )
            (summary["posted"] if delivered else summary["failed"]).append(note.dedup_key)
            continue

        if existing["content_hash"] == _content_hash(note):
            summary["unchanged"].append(note.dedup_key)
            if terminal:
                # Identical content and finished: nothing to say, but stop
                # reconsidering it on every future sweep.
                _edit_message(conn, note, live=False, error=existing.get("error"))
                summary["closed"].append(note.dedup_key)
            continue

        attempts = slack.update(note, list(existing["messages"] or []), client=client)
        failed = [a for a in attempts if not a["delivered"]]
        _edit_message(
            conn, note,
            # A failed edit stays live so the next sweep tries again; the content
            # hash is still advanced, because re-sending the same failing edit
            # forever is the loop this design keeps refusing.
            live=not terminal,
            error="; ".join(f"{a['channel']}: {a['error']}" for a in failed) or None,
        )
        if failed:
            summary["failed"].append(note.dedup_key)
        else:
            summary["updated"].append(note.dedup_key)
        if terminal:
            summary["closed"].append(note.dedup_key)

    return summary


def sweep(
    conn: psycopg.Connection,
    *,
    since: datetime | None = None,
    now: datetime | None = None,
    grace_minutes: float = DEFAULT_GRACE_MINUTES,
    run_failures_on: bool = True,
    incidents_on: bool = True,
    data_tests_on: bool = True,
    client: Any = None,
) -> dict[str, Any]:
    """One pass over everything worth saying. Driven from cron, like `check`."""
    notes: list[Notification] = []
    if run_failures_on:
        notes += run_failures(conn, since=since, now=now, grace_minutes=grace_minutes)
    if incidents_on:
        try:
            notes += incident_notifications(conn, now=now)
        except Exception as exc:  # noqa: BLE001 - a graph problem must not lose failures
            log.warning("could not build incident notifications: %s", exc)
    if data_tests_on:
        notes += data_test_notifications(conn, since=since, now=now)
    return send(conn, notes, client=client, now=now)
