"""wishd CLI.

Boot a database, run the gateway, push events at it, read the run tree back out,
and reconcile and evaluate monitors. Everything the web UI does is reachable from
here, deliberately: the parts that belong in cron (`maintain`, `check`) and in CI
(`apply`) must not require a browser.
"""

from __future__ import annotations

import json
import os
from datetime import UTC
from pathlib import Path
from typing import Any
from uuid import UUID

import typer
from rich.console import Console
from rich.table import Table
from rich.tree import Tree

from . import __version__
from .config import env

app = typer.Typer(
    add_completion=False,
    help="wish:d (what is happening:data). Data observability for your own compute.",
)
console = Console()


def _show_version(value: bool) -> None:
    if value:
        console.print(f"wish:d {__version__}")
        raise typer.Exit()


@app.callback()
def options(
    version: bool = typer.Option(
        False, "--version", callback=_show_version, is_eager=True, help="Show the version."
    ),
) -> None:
    pass


STATE_STYLE = {
    "COMPLETED": "green",
    "RUNNING": "yellow",
    "FAILED": "bold red",
    "ABORTED": "red",
    "UNKNOWN": "dim",
}


def _fmt_duration(ms: float | None) -> str:
    if ms is None:
        return "–"
    s = ms / 1000
    if s < 60:
        return f"{s:.1f}s"
    m, s = divmod(int(s), 60)
    if m < 60:
        return f"{m}m{s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m"


# How much of a run id the listings print. Not 8: UUIDv7 spends its first 48
# bits on a millisecond timestamp, so every run that started within the same
# ~65 seconds shares the first 8 hex characters -- which is precisely the
# Airflow/dbt/Spark set of one pipeline, the set you are choosing between. 18
# characters clears the version nibble and reaches the random bits.
RUN_ID_DISPLAY = 18


def _resolve_run_id(conn: Any, value: str) -> UUID:
    """Resolve a run id or prefix, turning a lookup failure into a clean exit.

    The listings truncate, so the id a user has in hand is usually a prefix.
    Letting `UUID()` raise here would meet them with a traceback on the second
    command of the quickstart.
    """
    from . import queries

    try:
        return queries.resolve_run_id(conn, value)
    except queries.RunIdError as exc:
        console.print(f"[red]{exc}[/]")
        for candidate in exc.candidates:
            console.print(f"  [dim]{candidate}[/]")
        raise typer.Exit(1) from None


# --------------------------------------------------------------------- database


@app.command()
def migrate() -> None:
    """Apply pending SQL migrations."""
    from .db import migrate as run_migrations

    applied = run_migrations()
    if applied:
        console.print(f"[green]applied[/] {', '.join(applied)}")
    else:
        console.print("[dim]already up to date[/]")


@app.command("dev-db")
def dev_db(
    data_dir: Path = typer.Option(Path(".dev/pgdata"), help="Where to keep the dev cluster."),
    stop: bool = typer.Option(False, "--stop", help="Shut the dev cluster down."),
) -> None:
    """Run Postgres locally without Docker, using an embedded server binary.

    Docker Compose is the supported way to run wishd (`make up`). This
    command exists so you can develop before Docker is installed, and so CI can
    run the integration tests without a service container.
    """
    try:
        import pgserver
    except ImportError:
        console.print("[red]pgserver is not installed.[/] Run: uv pip install -e '.[dev]'")
        raise typer.Exit(1) from None

    data_dir = data_dir.resolve()
    if stop:
        pgserver.get_server(data_dir, cleanup_mode="stop")
        console.print("[green]stopped[/]")
        return

    data_dir.mkdir(parents=True, exist_ok=True)
    server = pgserver.get_server(data_dir, cleanup_mode=None)
    uri = server.get_uri()
    console.print(f"[green]postgres up[/]  {uri}")
    console.print(f'\n  export DATASPINE_DATABASE_URL="{uri}"\n')


@app.command()
def serve(
    host: str = "0.0.0.0",
    port: int = 8080,
    reload: bool = typer.Option(False, help="Auto-reload on code changes."),
) -> None:
    """Run the ingest gateway and web UI."""
    import uvicorn

    from . import auth

    # Refuses to start rather than exposing unauthenticated pipeline metadata on
    # a reachable interface. See auth.guard_bind_address.
    auth.guard_bind_address(host)
    if auth.auth_enabled():
        console.print(f"[green]auth enabled[/] ({len(auth.configured_tokens())} token(s))")
    uvicorn.run("dataspine.api:app", host=host, port=port, reload=reload)


@app.command()
def replay(
    window_start: str = typer.Option("", "--since", help="ISO timestamp; only with --no-truncate."),
    window_end: str = typer.Option("", "--until", help="ISO timestamp; only with --no-truncate."),
    truncate: bool = typer.Option(True, help="Rebuild everything from scratch."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip the confirmation."),
) -> None:
    """Rebuild runs, jobs and datasets from the raw event archive.

    Run this after changing correlator logic, or after fixing a producer that was
    misconfigured for a week. The `events` table is never touched — it is the
    source of truth, and this only re-derives the projection from it.
    """
    from datetime import datetime

    from .db import connection
    from .replay import replay as run_replay

    since = datetime.fromisoformat(window_start) if window_start else None
    until = datetime.fromisoformat(window_end) if window_end else None

    if truncate and not yes:
        console.print(
            "[yellow]This drops and rebuilds runs/jobs/datasets from the event archive.[/]\n"
            "[dim]`events` is not touched.[/]"
        )
        if not typer.confirm("Continue?"):
            raise typer.Abort()

    with connection() as conn:
        stats = run_replay(conn, since=since, until=until, truncate=truncate)

    console.print(
        f"[green]replayed[/] {stats['ingested']} of {stats['events']} archived events"
        + (f"  [yellow]({stats['invalid']} not valid RunEvents)[/]" if stats["invalid"] else "")
    )


@app.command()
def maintain(
    months_ahead: int = typer.Option(3, help="How many future month partitions to provision."),
    keep_months: int = typer.Option(
        0, help="Drop partitions older than this many months. 0 disables retention."
    ),
) -> None:
    """Provision partitions and apply retention.

    Run this from cron (daily is plenty). Provisioning ahead of time keeps live
    inserts out of the DEFAULT partition; retention drops whole partitions,
    which is a metadata operation rather than a DELETE over the biggest table in
    the system.

    Covers every partitioned table — `events` and, since migration 007,
    `metric_points`. One maintenance mechanism for both is most of why ADR-005
    chose a plain partitioned table for metric history.
    """
    from . import retention
    from .db import connection

    with connection() as conn:
        if not retention.is_partitioned(conn):
            console.print("[yellow]events is not partitioned[/] — run `wishd migrate` first")
            raise typer.Exit(1)
        for table in retention.PARTITIONED_TABLES:
            if not retention.is_partitioned(conn, table):
                continue
            created = retention.ensure_partitions(
                conn, table=table, months_ahead=months_ahead
            )
            dropped = (
                retention.apply_retention(conn, table=table, keep_months=keep_months)
                if keep_months
                else []
            )
            console.print(
                f"[dim]{table}[/]  [green]created[/] {len(created)}: "
                f"{', '.join(created) or '—'}"
                + (
                    f"   [green]dropped[/] {len(dropped)}: {', '.join(dropped) or '—'}"
                    if keep_months
                    else ""
                )
            )

    if not keep_months:
        console.print("[dim]retention disabled (--keep-months 0)[/]")


# ------------------------------------------------------------------- monitors


@app.command()
def apply(
    path: Path = typer.Argument(Path("monitors"), help="A monitor YAML file, or a directory."),
    prune: bool = typer.Option(
        False, help="Delete monitors removed from these files instead of disabling them."
    ),
    backfill: bool = typer.Option(
        True, help="Build metric history from the run archive so monitors arm immediately."
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Report what would change."),
) -> None:
    """Reconcile monitor definitions from YAML in your repo.

    Monitors live next to the dbt project they protect, so a threshold change goes
    through review like any other change. Removing one from a file disables it
    rather than deleting it — a monitor usually vanishes from a file because of a
    bad merge, and deleting would take its history with it. Use --prune when you
    mean it.

    With --backfill (the default) a new monitor is immediately evaluated against
    the run archive, so it arms on day one instead of after a week of collecting.
    """
    from . import catalog as catalog_mod
    from . import checks
    from . import monitors as monitors_mod
    from .db import connection

    try:
        specs = monitors_mod.load_specs(path)
    except monitors_mod.SpecError as exc:
        console.print(f"[red]{exc}[/]")
        raise typer.Exit(1) from None

    if dry_run:
        table = Table(box=None, pad_edge=False, header_style="dim")
        table.add_column("monitor")
        table.add_column("kind", style="cyan")
        table.add_column("target")
        table.add_column("schedule", style="dim")
        for spec in specs:
            table.add_row(spec.name, spec.kind, spec.target, spec.schedule)
        console.print(table)
        console.print(f"[dim]{len(specs)} monitor(s) — nothing written (--dry-run)[/]")
        return

    sources = sorted({s.source for s in specs if s.source})
    with connection() as conn:
        outcome = monitors_mod.apply_specs(conn, specs, sources=sources, prune=prune)
        # `datasets:` blocks live in the same files as `monitors:`, so ownership
        # and a threshold change arrive in one reviewed commit rather than two.
        annotations = _load_annotations(path)
        if annotations:
            catalog_mod.apply_annotations(conn, annotations, sources=sources)
        conn.commit()

        armed = []
        if backfill:
            # `since=None` collects the whole archive. Same code path the hourly
            # check uses with a narrow window -- see checks.collect.
            #
            # `record=False` matters: arming must not move `last_status`, or the
            # first real `check` sees no transition and never alerts on a breach
            # this arming run discovered.
            for name in outcome["created"] + outcome["updated"]:
                monitor = monitors_mod.get_monitor(conn, name)
                if monitor and monitor["enabled"]:
                    armed.append(
                        checks.evaluate(conn, monitor, since=None, record=False)
                    )
            conn.commit()

    for action in ("created", "updated", "disabled", "pruned"):
        if outcome[action]:
            style = "red" if action in ("disabled", "pruned") else "green"
            console.print(f"[{style}]{action}[/] {', '.join(outcome[action])}")
    if outcome["unchanged"]:
        console.print(f"[dim]unchanged[/] {len(outcome['unchanged'])}")

    if annotations:
        console.print(f"[green]annotated[/] {len(annotations)} dataset(s)")

    if armed:
        points = sum(a["collected"] for a in armed)
        console.print(f"[dim]backfilled {points} metric point(s) from the run archive[/]")
        _print_check_results(armed)


def _load_annotations(path: Path) -> list[Any]:
    """Read `datasets:` blocks from the same files monitors come from.

    Missing blocks are normal -- most monitor files have none -- so this is
    quiet, unlike monitor parsing, where silence would hide a monitor that never
    fires.
    """
    import yaml

    from . import catalog as catalog_mod

    files = sorted(path.rglob("*.y*ml")) if path.is_dir() else [path]
    annotations: list[Any] = []
    for file in files:
        if not file.is_file():
            continue
        try:
            raw = yaml.safe_load(file.read_text()) or {}
        except Exception:
            continue
        annotations += catalog_mod.parse_annotations(raw, source=str(file))
    return annotations


@app.command()
def check(
    monitor: str = typer.Option("", help="Evaluate one monitor by name."),
    schedule: str = typer.Option("", help="Only monitors on this schedule: hourly | daily."),
    since_days: int = typer.Option(
        7, help="How far back to collect observations. Judgement always uses full history."
    ),
    alert: bool = typer.Option(
        True, help="Deliver transitions to the configured channels."
    ),
) -> None:
    """Evaluate monitors, record the results, and alert on what changed.

    There is no daemon: run this from cron or an Airflow DAG, the same way
    `maintain` runs. A scheduler that only works while our process is up is a
    scheduler that silently stops watching your data when we get OOM-killed.

        wishd check --schedule hourly

    Alerts go to whichever of DATASPINE_SLACK_BOT_TOKEN (or DATASPINE_SLACK_WEBHOOK),
    DATASPINE_PAGERDUTY_ROUTING_KEY and DATASPINE_ALERT_WEBHOOK are set, and only
    on a status change. Which Slack channel each one lands in is decided by
    DATASPINE_SLACK_ROUTES — `wishd slack-check` shows you.

    Use --no-alert when experimenting with thresholds, so a re-evaluation of old
    history cannot page the on-call for incidents that are long over.

    This command covers monitors. `wishd notify` covers failed pipeline runs,
    which is the other half and belongs on the same cron.
    """
    from datetime import datetime, timedelta

    from . import alerts as alerts_mod
    from . import checks
    from . import slack as slack_mod
    from .db import connection

    since = datetime.now(UTC) - timedelta(days=since_days) if since_days else None
    with connection() as conn:
        results = checks.check_all(
            conn, schedule=schedule or None, name=monitor or None, since=since, alert=alert
        )
        conn.commit()

    if alert:
        changed = alerts_mod.transitions(results)
        channels = alerts_mod.configured_channels()
        if changed and not channels:
            # Worth saying once, when it actually matters: something transitioned
            # and there was nowhere to send it. Saying it on every quiet run
            # would be noise nobody reads.
            console.print(
                f"[yellow]{len(changed)} status change(s) and no alert channel configured.[/]\n"
                f"[dim]Set {slack_mod.BOT_TOKEN_ENV}, {alerts_mod.PAGERDUTY_ENV} "
                f"or {alerts_mod.WEBHOOK_ENV}.[/]"
            )

    if not results:
        console.print("[dim]no monitors to evaluate[/] — try `wishd apply monitors/`")
        return
    _print_check_results(results)

    breaches = [r for r in results if r["status"] == "breach"]
    if breaches:
        raise typer.Exit(1)


STATUS_STYLE = {
    "ok": "green",
    "breach": "bold red",
    "insufficient_data": "dim",
    "error": "yellow",
}


def _print_check_results(results: list[dict[str, Any]]) -> None:
    table = Table(box=None, pad_edge=False, header_style="dim")
    table.add_column("monitor")
    table.add_column("status")
    table.add_column("detail")
    for r in results:
        style = STATUS_STYLE.get(r["status"], "")
        marker = " [yellow]←changed[/]" if r.get("transitioned") else ""
        table.add_row(
            r["monitor"],
            f"[{style}]{r['status']}[/]",
            f"{r['message']}{marker}",
        )
    console.print(table)


@app.command()
def resolve() -> None:
    """Rebuild dataset identity, the lineage graph and the search index.

    Run it from cron next to `check`, or call `POST /api/v1/graph/resolve` at the
    end of a pipeline so the graph is current before anyone looks at an incident.

    Everything it writes is derived from `runs`, `datasets` and job SQL, so a
    wrong merge or a wrong edge is fixed by improving the resolver and running
    this again — never by editing rows.
    """
    from . import catalog as catalog_mod
    from . import identity, lineage, stitch
    from .db import connection

    with connection() as conn:
        # Stitching first: identity resolution merges on co-write evidence, which
        # needs the run tree that a shared Thrift Server splits apart. Resolving
        # before stitching would compute entities from a graph we are about to fix.
        stitched = stitch.stitch_query_comments(conn)
        entities = identity.resolve(conn)
        graph = lineage.resolve(conn)
        indexed = catalog_mod.reindex(conn)
        # What stitching could not fix. Reported every time, because a silent
        # split is the failure D6 found and unstitched_runs cannot see.
        split = stitch.disjoint_trees(conn)
        conn.commit()

    if stitched:
        console.print(
            f"[green]stitched[/] {stitched} Spark run(s) onto their dbt node "
            f"via dbt's query comment"
        )
    console.print(
        f"[green]entities[/] {entities['entities']} "
        f"({entities['merged']} identity merge(s))"
    )


    console.print(
        f"[green]edges[/] {graph['table_edges']} table, "
        f"{graph['column_edges_facet']} column from facets, "
        f"{graph['column_edges_sql']} column from SQL"
    )
    console.print(f"[dim]indexed {indexed} entities for search[/]")

    if split:
        console.print(
            f"\n[yellow]{len(split)} table(s) written by producers whose run trees "
            f"never meet[/]"
        )
        console.print(
            "[dim]either a correlation gap or two different tables sharing a name; "
            "`unstitched_runs` cannot detect this[/]"
        )
        for row in split[:8]:
            console.print(
                f"  {row['name']}  [dim]{row['trees']} trees · "
                f"{', '.join(row['integrations'])}[/]"
            )


@app.command()
def incidents() -> None:
    """Current incidents: the cause, and what it took down with it."""
    from . import incidents as incidents_mod
    from .db import connection

    with connection() as conn:
        current = incidents_mod.detect(conn, persist=True)
        conn.commit()

    if not current:
        console.print("[green]no open incidents[/]")
        return

    for incident in current:
        console.print(
            f"\n[bold red]{incident['cause_entity_name'] or incident['cause_monitor']}[/]"
        )
        console.print(f"  cause: [red]{incident['cause_monitor']}[/] — {incident['message']}")
        if incident["consequences"]:
            console.print(
                f"  [dim]{len(incident['consequences'])} downstream monitor(s) "
                f"also breaching (suppressed):[/]"
            )
            for consequence in incident["consequences"]:
                console.print(
                    f"    [dim]{consequence['distance']} hop(s)[/] "
                    f"{consequence['entity_name'] or '—'}  {consequence['monitor']}"
                )
        else:
            console.print("  [dim]nothing downstream affected[/]")


# ------------------------------------------------------------------ notifications


@app.command()
def notify(
    since_hours: float = typer.Option(
        24, help="How far back to look for failures. The ledger, not this, prevents repeats."
    ),
    grace_minutes: float = typer.Option(
        5, help="Ignore failures newer than this, so an Airflow retry has time to land."
    ),
    run_failures: bool = typer.Option(
        True, "--run-failures/--no-run-failures", help="Notify about failed pipeline runs."
    ),
    incidents: bool = typer.Option(
        True, "--incidents/--no-incidents", help="Notify about newly opened incidents."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Print what would be sent. Claims nothing, sends nothing."
    ),
) -> None:
    """Tell Slack about failed runs and new incidents.

    Run this from cron next to `wishd check`; there is no daemon, for the
    reason `check` states — a watcher that only works while our process is up
    silently stops watching when we get OOM-killed.

        wishd notify

    Each notification is sent once. A pipeline is one message however many of its
    tasks failed, a failure a retry then fixed is not a message at all, and the
    `notifications` table records every decision so "why did nobody get told?"
    stays answerable weeks later.
    """
    from datetime import datetime, timedelta

    from . import notify as notify_mod
    from .db import connection

    since = datetime.now(UTC) - timedelta(hours=since_hours)

    with connection() as conn:
        if dry_run:
            notes = []
            if run_failures:
                notes += notify_mod.run_failures(conn, since=since, grace_minutes=grace_minutes)
            if incidents:
                notes += notify_mod.incident_notifications(conn)
            conn.rollback()
            _print_notifications(notes)
            return

        summary = notify_mod.sweep(
            conn,
            since=since,
            grace_minutes=grace_minutes,
            run_failures_on=run_failures,
            incidents_on=incidents,
        )
        conn.commit()

    _print_delivery(summary)


@app.command()
def track(
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Print the feed as it stands. Posts and edits nothing."
    ),
) -> None:
    """Keep one Slack message per pipeline execution in step with the pipeline.

    A live feed, without the flood that usually means. A pipeline gets exactly
    one message: posted when the feed first sees it running, edited in place as
    steps complete, and edited once more to say how it ended. Eight steps is one
    message, not eight.

        wishd track        # from cron, every minute

    Needs DATASPINE_SLACK_BOT_TOKEN. An incoming webhook cannot edit a message it
    already sent, so the feed refuses that transport rather than degrading into
    the per-step flood it exists to avoid.

    Route it with `match: {event: pipeline}`. Once this is running, a separate
    `run_failure` route into the same channel is duplicate news — the feed
    already turns that pipeline's message red.
    """
    from . import notify as notify_mod
    from .db import connection

    with connection() as conn:
        if dry_run:
            notes = notify_mod.pipeline_feed(
                conn, tracking=set(notify_mod.open_messages(conn))
            )
            conn.rollback()
            _print_notifications(notes)
            return
        summary = notify_mod.track(conn)
        conn.commit()

    if summary["skipped"]:
        console.print(f"[yellow]{summary['skipped']}[/]")
        raise typer.Exit(1)

    parts = [
        f"[green]posted[/] {len(summary['posted'])}" if summary["posted"] else "",
        f"[green]updated[/] {len(summary['updated'])}" if summary["updated"] else "",
        f"[dim]unchanged {len(summary['unchanged'])}[/]" if summary["unchanged"] else "",
        f"[dim]closed {len(summary['closed'])}[/]" if summary["closed"] else "",
        f"[red]failed[/] {len(summary['failed'])}" if summary["failed"] else "",
    ]
    line = "  ".join(p for p in parts if p)
    console.print(line or "[dim]nothing in flight[/]")


@app.command()
def digest(
    hours: float = typer.Option(24, help="Window the digest covers."),
    force: bool = typer.Option(
        False, "--force", help="Send even if a digest for this hour already went out."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Print the digest. Claims nothing, sends nothing."
    ),
) -> None:
    """Post a summary of the window — including the quiet one.

    A channel that only ever speaks when something is wrong gives nobody a way to
    tell "quiet" from "broken and silent". This is the message that makes the
    silence mean something.

        wishd digest --hours 24     # from cron, once a morning
    """
    from . import notify as notify_mod
    from .db import connection

    with connection() as conn:
        note = notify_mod.digest(conn, hours=hours)
        if dry_run:
            conn.rollback()
            _print_notifications([note])
            return
        summary = notify_mod.send(conn, [note], force=force)
        conn.commit()

    _print_delivery(summary)


@app.command("slack-check")
def slack_check(
    send: bool = typer.Option(
        False, "--send", help="Also post a test message to every configured channel."
    ),
) -> None:
    """Show how Slack is configured, and what each route would catch.

    The failure this exists to catch is the silent one: a routes file naming four
    channels, and an incoming webhook that can only ever reach one of them. That
    setup delivers messages, so it looks like it works, and the three teams who
    never hear anything have no reason to suspect why.
    """
    from . import notify as notify_mod
    from . import slack as slack_mod

    state = slack_mod.describe()

    transport = state["transport"]
    label = {
        "bot": f"bot token ([green]{slack_mod.BOT_TOKEN_ENV}[/]) — any channel",
        "webhook": f"incoming webhook ([green]{slack_mod.WEBHOOK_ENV}[/]) — one channel only",
    }.get(transport, "[red]not configured[/]")
    console.print(f"transport  {label}")
    console.print(f"channel    {state['default_channel'] or '[dim]— (from routes)[/]'}")
    console.print(
        f"base url   {notify_mod._base_url() or '[dim]— (messages will carry no links)[/]'}"
    )

    if state["routes"]:
        table = Table(box=None, pad_edge=False)
        table.add_column("#", style="dim")
        table.add_column("matches")
        table.add_column("channel")
        for index, route in enumerate(state["routes"], start=1):
            match = (
                ", ".join(f"{k}={'|'.join(v)}" for k, v in route.match.items())
                or "[dim]everything else[/]"
            )
            table.add_row(str(index), match, ", ".join(route.channels))
        console.print()
        console.print("[bold]routes[/] [dim](first match wins)[/]")
        console.print(table)
    elif not state["route_error"]:
        console.print()
        console.print(f"[dim]no routes file — set {slack_mod.ROUTES_ENV} to route by channel[/]")

    for problem in state["problems"]:
        console.print()
        console.print(f"[yellow]![/] {problem}")

    if not send:
        if not state["problems"]:
            console.print()
            console.print("[green]Slack is configured.[/] Use --send to post a test message.")
        raise typer.Exit(1 if state["problems"] else 0)

    if transport is None:
        raise typer.Exit(1)

    probe = notify_mod.Notification(
        event="digest",
        status="ok",
        title="Test message",
        summary="If you can read this, wishd can reach this channel.",
        dedup_key="slack-check",
        fields=(("sent by", "wishd slack-check"),),
    )
    # Deliberately bypasses the ledger: a test you can only run once is not a test.
    attempts = slack_mod.deliver([probe])
    console.print()
    if not attempts:
        console.print("[yellow]nothing was sent[/] — no route matched the test message")
        raise typer.Exit(1)
    for attempt in attempts:
        if attempt["delivered"]:
            console.print(f"[green]delivered[/] to {attempt['channel']}")
        else:
            console.print(f"[red]failed[/] to {attempt['channel']}: {attempt['error']}")
    if any(not a["delivered"] for a in attempts):
        raise typer.Exit(1)


def _print_notifications(notes: list[Any]) -> None:
    if not notes:
        console.print("[dim]nothing to send[/]")
        return
    for note in notes:
        console.print()
        console.print(f"[bold]{note.title}[/] [dim]({note.event})[/]")
        for line in note.summary.splitlines():
            console.print(f"  {line}")
        if note.fields:
            console.print(
                "  [dim]" + "  ·  ".join(f"{k}: {v}" for k, v in note.fields if v) + "[/]"
            )


def _print_delivery(summary: dict[str, Any]) -> None:
    sent, failed, skipped = summary["sent"], summary["failed"], summary["skipped"]
    if not (sent or failed or skipped):
        console.print("[dim]nothing to send[/]")
        return
    if sent:
        console.print(f"[green]sent[/] {len(sent)}")
    if failed:
        console.print(f"[red]not delivered[/] {len(failed)} — see `select * from notifications`")
    if skipped:
        console.print(f"[dim]already sent, or nowhere to send: {len(skipped)}[/]")


@app.command("lineage")
def lineage_command(
    table: str = typer.Argument(..., help="Table name to inspect."),
    depth: int = typer.Option(2, help="How many hops each way."),
    columns: bool = typer.Option(False, "--columns", help="Show column-level lineage."),
) -> None:
    """Show what a table depends on and what depends on it."""
    from . import identity
    from . import lineage as lineage_mod
    from .db import connection

    with connection() as conn:
        matches = identity.find(conn, table)
        if not matches:
            console.print(f"[red]no table matching[/] {table}")
            raise typer.Exit(1)
        entity_id = matches[0]["id"]
        name = matches[0]["name"]
        up = lineage_mod.upstream(conn, entity_id, depth=depth)
        down = lineage_mod.downstream(conn, entity_id, depth=depth)
        column_rows = (
            lineage_mod.column_edges(conn, entity_id=entity_id) if columns else []
        )

    console.print(f"\n[bold]{name}[/]")
    console.print("\n[dim]upstream[/]")
    for node in sorted(up, key=lambda n: n["distance"]):
        console.print(f"  {'←' * node['distance']} {node['name']}")
    if not up:
        console.print("  [dim]nothing — this is a source table[/]")

    console.print("\n[dim]downstream (blast radius)[/]")
    for node in sorted(down, key=lambda n: n["distance"]):
        console.print(f"  {'→' * node['distance']} {node['name']}")
    if not down:
        console.print("  [dim]nothing depends on this[/]")

    if columns and column_rows:
        table_out = Table(box=None, pad_edge=False, header_style="dim")
        table_out.add_column("column")
        table_out.add_column("from")
        table_out.add_column("via", style="dim")
        table_out.add_column("src", style="cyan")
        for row in column_rows:
            table_out.add_row(
                row["downstream_column"],
                f"{row['upstream_name']}.{row['upstream_column']}",
                row["transformation_subtype"] or row["transformation_type"] or "—",
                row["source"],
            )
        console.print()
        console.print(table_out)


@app.command("import-cost")
def import_cost(
    path: Path = typer.Argument(..., help="A CUR CSV (optionally .gz)."),
    tag_key: str = typer.Option(
        "cluster", help="Tag whose value identifies the cluster in your CUR."
    ),
    attribute: bool = typer.Option(True, help="Re-run attribution afterwards."),
) -> None:
    """Import an AWS Cost and Usage Report and attribute it to pipelines.

    CUR rows carry `resourceTags/...`, never cluster ids, so --tag-key names the
    tag your clusters are labelled with. Rows matching no cluster are kept and
    reported as unattributed rather than dropped: totals that disagree with the
    AWS console are worse than useless.
    """
    from . import cost as cost_mod
    from .db import connection

    with connection() as conn:
        imported = cost_mod.import_cur_file(conn, path, tag_key=tag_key)
        priced = cost_mod.attribute(conn) if attribute else 0
        gap = cost_mod.unattributed(conn)
        conn.commit()

    console.print(f"[green]imported[/] {imported} line item(s)")
    if attribute:
        console.print(f"[green]priced[/] {priced} Spark application(s)")
    if gap["line_items"]:
        console.print(
            f"[yellow]${float(gap['cost_usd']):,.2f} across {gap['line_items']} line "
            f"item(s) matched no cluster[/] — check --tag-key and the cluster sync"
        )


@app.command("costs")
def costs_command(
    limit: int = typer.Option(20, help="How many jobs to show."),
) -> None:
    """What each pipeline costs to run."""
    from . import cost as cost_mod
    from .db import connection

    with connection() as conn:
        rows = cost_mod.by_job(conn, limit=limit)
        clusters = cost_mod.unattributed(conn)

    if not rows:
        console.print(
            "[dim]nothing priced yet[/] — "
            "try `wishd import-cost <cur.csv> --tag-key team`"
        )
        return

    table = Table(box=None, pad_edge=False, header_style="dim")
    table.add_column("job")
    table.add_column("cost", justify="right")
    table.add_column("runs", justify="right")
    table.add_column("last run", style="dim")
    for row in rows:
        table.add_row(
            row["job_name"],
            f"${float(row['cost_usd']):,.2f}",
            str(row["runs"]),
            row["last_run_at"].strftime("%m-%d %H:%M") if row["last_run_at"] else "–",
        )
    console.print(table)
    if clusters["line_items"]:
        console.print(
            f"[dim]${float(clusters['cost_usd']):,.2f} unattributed "
            f"({clusters['line_items']} line items)[/]"
        )


@app.command("lineage-coverage")
def lineage_coverage() -> None:
    """How much column lineage we actually have, and why the rest is missing.

    Open question 3. Every SQLGlot decline is a missing edge, and a graph quietly
    missing a third of its edges while looking complete is worse than one that
    says so. This makes coverage a stated figure rather than an assumption.

    The reasons matter more than the percentage: "declined because they were all
    `select *`" is a catalog problem, "declined as unparseable" is a dialect
    problem, and they have completely different fixes.
    """
    from . import lineage as lineage_mod
    from .db import connection

    with connection() as conn:
        report = lineage_mod.coverage_from_db(conn)

    if not report["statements"]:
        console.print("[dim]no SQL collected yet[/] — nothing to measure")
        return

    rate = report["coverage"]
    console.print(
        f"[bold]{report['statements']}[/] statement(s), "
        f"{report['with_projection']} projecting columns"
    )
    if rate is None:
        # Nought out of nought is not nought per cent.
        console.print("[dim]no column-projecting SQL — coverage not applicable[/]")
    else:
        style = "green" if rate >= 0.8 else "yellow" if rate >= 0.5 else "red"
        console.print(
            f"[{style}]{rate:.0%}[/] of {report['output_columns']} output column(s) "
            f"resolved from SQL ({report['resolved_columns']} edges)"
        )

    if report["reasons"]:
        console.print("\n[dim]why the rest declined[/]")
        for reason, count in report["reasons"].items():
            console.print(f"  {count:>4}  {reason}")

    if report.get("edges_by_source"):
        console.print("\n[dim]stored column edges by source[/]")
        for source, count in sorted(report["edges_by_source"].items()):
            console.print(f"  {count:>4}  {source}")
        console.print(
            "[dim]facet edges came from the producer and never went through the "
            "parser — see ADR-007[/]"
        )


@app.command("pr-impact")
def pr_impact(
    paths: list[str] = typer.Argument(..., help="Changed file paths."),
    depth: int = typer.Option(5, help="How far downstream to look."),
) -> None:
    """Print the blast radius of changed dbt models, as Markdown.

    Built for CI: pipe it into a PR comment. Impact is computed from lineage we
    have actually observed, not from parsing the dbt DAG — so it reflects what
    really runs, including the parts nobody remembered to document.

        wishd pr-impact $(git diff --name-only origin/main...HEAD)
    """
    from . import pr as pr_mod
    from .db import connection

    with connection() as conn:
        body = pr_mod.comment(conn, list(paths), depth=depth)

    if not body:
        console.print("[dim]no dbt models changed[/]")
        return
    # Printed raw: the caller pipes it into `gh pr comment --body-file -`.
    print(body)


@app.command()
def poll(
    path: Path = typer.Argument(Path("monitors"), help="A YAML file or directory with `sources:`."),
    source: str = typer.Option("", help="Poll only this source by name."),
) -> None:
    """Collect catalog metadata from systems that do not emit OpenLineage.

    Reads only metadata — INFORMATION_SCHEMA, system tables, Iceberg snapshot
    summaries, the Delta transaction log — never table data. Those catalogs
    already hold row counts and modification times because the engine maintains
    them for its own planner, so this costs the warehouse essentially nothing.

    Run it from cron next to `wishd check`. What it stores is read by exactly
    the same freshness, volume and schema monitors that watch your own pipelines,
    so a Fivetran-loaded source table is monitored the same way a dbt model is.
    """
    import yaml

    from . import sources as sources_mod
    from .db import connection

    files = sorted(path.rglob("*.y*ml")) if path.is_dir() else [path]
    specs: list[Any] = []
    for file in files:
        if not file.is_file():
            continue
        try:
            specs += sources_mod.parse_sources(yaml.safe_load(file.read_text()) or {})
        except sources_mod.SourceError as exc:
            console.print(f"[red]{file}: {exc}[/]")
            raise typer.Exit(1) from None

    if source:
        specs = [s for s in specs if s.name == source]
    if not specs:
        console.print(f"[dim]no sources declared in {path}[/]")
        return

    table = Table(box=None, pad_edge=False, header_style="dim")
    table.add_column("source")
    table.add_column("type", style="cyan")
    table.add_column("tables", justify="right")
    with connection() as conn:
        for spec in specs:
            try:
                # A SQL source needs its own driver connection; only Postgres can
                # reuse ours, and only when it is genuinely the same server.
                written = sources_mod.poll(conn, spec)
                table.add_row(spec.name, spec.type, str(written))
            except Exception as exc:  # noqa: BLE001 - one bad source must not end the sweep
                table.add_row(spec.name, spec.type, f"[red]{type(exc).__name__}[/]")
        conn.commit()
    console.print(table)


@app.command("profile")
def profile_command(
    table_name: str = typer.Argument(..., help="Table to profile."),
    dataset: str = typer.Option("", help="Dataset name to attach profiles to."),
    namespace: str = typer.Option("local", help="Dataset namespace."),
    max_rows: int = typer.Option(
        1_000_000, help="Scan budget. Above this it samples; 0 refuses to scan."
    ),
) -> None:
    """Profile a table's columns. Opt-in, budgeted, and it reads real data.

    This is the only command in wishd that reads table *contents* rather than
    metadata, so it never runs on its own — you ask for it, and the budget is
    enforced. Above --max-rows it samples and records that it did, because a null
    rate from a 1% sample is not the same claim as one from a full table.
    """
    from datetime import datetime

    from . import profile as profile_mod
    from .db import connection

    with connection() as conn:
        profiles = profile_mod.profile_table(conn, table_name, max_rows=max_rows)
        if not profiles:
            console.print(
                "[yellow]nothing profiled[/] — "
                + ("budget is zero" if max_rows <= 0 else f"no columns found on {table_name}")
            )
            raise typer.Exit(1)

        dataset_id = conn.execute(
            "insert into datasets (namespace, name) values (%s, %s) "
            "on conflict (namespace, name) do update set updated_at = now() returning id",
            (namespace, dataset or table_name),
        ).fetchone()["id"]
        profile_mod.store_profiles(
            conn, dataset_id, profiles, observed_at=datetime.now(UTC)
        )
        conn.commit()

    out = Table(box=None, pad_edge=False, header_style="dim")
    out.add_column("column")
    out.add_column("nulls", justify="right")
    out.add_column("distinct", justify="right")
    out.add_column("min", justify="right")
    out.add_column("max", justify="right")
    for entry in profiles:
        out.add_row(
            entry.column,
            f"{entry.null_rate:.1%}" if entry.null_rate is not None else "–",
            f"{entry.distinct_count:,}" if entry.distinct_count is not None else "–",
            f"{entry.min:,.2f}" if entry.min is not None else "–",
            f"{entry.max:,.2f}" if entry.max is not None else "–",
        )
    console.print(out)
    if profiles[0].sampled:
        console.print(
            f"[yellow]sampled[/] {profiles[0].scanned_rows:,} rows — "
            f"statistics are estimates, and are stored as such"
        )


@app.command()
def monitors(
    resolve: bool = typer.Option(
        False, "--resolve", help="Show which datasets/jobs each target actually matches."
    ),
) -> None:
    """List monitors and their current status.

    --resolve is worth running once after `apply`. A dataset target matches on the
    final name segment, because one logical table shows up under several
    identities (dbt's `postgres://…/fct_orders` and Spark's `/warehouse/fct_orders`
    are the same table). That match is loose by necessity, so this prints exactly
    what it resolved to rather than leaving you to trust it.
    """
    from . import checks
    from . import monitors as monitors_mod
    from .db import connection

    with connection() as conn:
        rows = monitors_mod.list_monitors(conn)
        resolution: dict[str, list[str]] = {}
        if resolve:
            for row in rows:
                matches = (
                    checks.resolve_datasets(
                        conn, row["target"], namespace=(row["config"] or {}).get("namespace")
                    )
                    if row["target_kind"] == "dataset"
                    else checks.resolve_jobs(conn, row["target"])
                )
                resolution[row["name"]] = [
                    f"{m['name']}  [dim]({m['namespace']})[/]" for m in matches
                ]

    if not rows:
        console.print("[dim]no monitors defined[/] — try `wishd apply monitors/`")
        return

    table = Table(box=None, pad_edge=False, header_style="dim")
    table.add_column("monitor")
    table.add_column("kind", style="cyan")
    table.add_column("target")
    table.add_column("status")
    table.add_column("last checked", style="dim")
    for row in rows:
        status = row["last_status"] or "—"
        style = STATUS_STYLE.get(status, "dim")
        name = row["name"] if row["enabled"] else f"[dim]{row['name']} (disabled)[/]"
        table.add_row(
            name,
            row["kind"],
            row["target"],
            f"[{style}]{status}[/]",
            row["last_evaluated_at"].strftime("%m-%d %H:%M") if row["last_evaluated_at"] else "–",
        )
    console.print(table)

    if resolve:
        console.print("\n[dim]target resolution[/]")
        for name, matches in resolution.items():
            if not matches:
                console.print(f"  {name}: [yellow]nothing matched[/]")
            else:
                console.print(f"  [bold]{name}[/]")
                for match in matches:
                    console.print(f"    {match}")


# The subset of dbt's target/ worth keeping per run. The rest (compiled SQL for
# every model, partial_parse.msgpack) is either derivable or noise, and storing
# it per run is how an artifact store becomes the thing people disable.
DBT_ARTIFACTS = ("manifest.json", "run_results.json", "catalog.json", "sources.json")


def push_artifacts(directory: Path, run_id: str, *, url: str = "", token: str = "",
                   client: Any = None) -> list[str]:
    """Upload dbt artifacts for a run. Returns the names actually pushed.

    Returns quietly when there is nothing to send: a dbt run that died before
    writing `target/` must not also fail the task cleaning up after it.
    """
    import httpx

    owns_client = client is None
    if owns_client:
        client = httpx.Client(timeout=60, headers=_auth_headers(token))
    base = url.rstrip("/") if url else ""

    pushed: list[str] = []
    try:
        for name in DBT_ARTIFACTS:
            path = directory / name
            if not path.is_file():
                continue
            resp = client.post(
                f"{base}/api/v1/runs/{run_id}/artifacts",
                files={"file": (name, path.read_bytes(), "application/json")},
            )
            _abort_on_auth_error(resp)
            if resp.status_code == 201:
                pushed.append(name)
            else:
                console.print(f"[yellow]{name}: {resp.status_code}[/] {resp.text[:120]}")
    finally:
        if owns_client:
            client.close()
    return pushed


def ingest_dbt_run(directory: Path, *, job_name: str = "", dbt_cloud_url: str = "",
                   url: str = "", token: str = "", client: Any = None) -> dict[str, Any]:
    """Record a local `target/` as a run tree, for dbt invocations no one reports.

    The gap this closes is the dbt Cloud CLI. It executes on dbt Cloud's
    infrastructure, so `dbt-ol` never sees it, and it is *not* a job run either --
    the Admin API's run list holds scheduled and API-triggered runs only, so
    `pull-dbt-cloud` cannot find it however long it looks. The invocation is
    real, it touched the warehouse, and until now nothing could record it.

    What it does have is `target/`, which the Cloud CLI downloads when the
    invocation finishes. That is the same `run_results.json` dbt Cloud's API
    serves, so the artifacts parser already handles it and the run tree comes out
    named the way `dbt-ol` names things.

    Not for a stack already running `dbt-ol`: that stack reports its own tree and
    this would synthesise a second copy of every run beside it. `push-artifacts`
    is the command for that case -- it keeps the artifacts and takes only the
    tests, precisely because the runs are already accounted for.

    `dbt_cloud_url` is stated by the caller because nothing else can state it. A
    Cloud CLI invocation has no address of its own: it is absent from the run
    list, no invocations endpoint exists to ask, and the artifacts name neither
    the account nor the project. So whoever runs this is the only party that
    knows where the button should point -- usually the project in dbt Cloud --
    and an unset value produces no link rather than a guessed one that 404s.
    """
    import httpx

    from . import dbt_artifacts

    results_path = directory / dbt_artifacts.RUN_RESULTS
    if not results_path.is_file():
        return {"events": 0, "run_id": None, "pushed": []}

    manifest_path = directory / dbt_artifacts.MANIFEST
    invocation = dbt_artifacts.parse(
        json.loads(results_path.read_text()),
        json.loads(manifest_path.read_text()) if manifest_path.is_file() else None,
    )
    run_facets = None
    if dbt_cloud_url.startswith("http"):
        run_facets = {"dbt_cloud": {"href": dbt_cloud_url}}
    events = dbt_artifacts.events(
        invocation, job_name=job_name or None, run_facets=run_facets
    )
    run_id = str(dbt_artifacts.run_id_for(invocation.invocation_id))

    owns_client = client is None
    if owns_client:
        client = httpx.Client(timeout=60, headers=_auth_headers(token))
    base = url.rstrip("/") if url else ""
    try:
        resp = client.post(f"{base}/api/v1/lineage/batch", json=events)
        _abort_on_auth_error(resp)
        if resp.status_code >= 400:
            console.print(f"[red]{resp.status_code}[/] {resp.text[:160]}")
            return {"events": 0, "run_id": run_id, "pushed": []}
        # The upload is what records the tests: the gateway reads the artifacts
        # it is handed, so there is no second endpoint to keep in step with it.
        pushed = push_artifacts(directory, run_id, url=url, token=token, client=client)
    finally:
        if owns_client:
            client.close()

    return {
        "events": len(events),
        "run_id": run_id,
        "job": dbt_artifacts.root_job_name(invocation, job_name or None),
        "pushed": pushed,
    }


@app.command("dbt-cloud-check")
def dbt_cloud_check() -> None:
    """Show how dbt Cloud is configured, and what it can actually see.

    The failure this exists to catch is a 401 or a 404 that says nothing about
    which of four things is wrong. The most likely is the host: dbt Cloud is
    multi-cell, so a newer account lives at something like `abc123.us1.dbt.com`
    and the documented default of `cloud.getdbt.com` simply rejects it — with no
    hint that the host is the problem.

    Also reports whether webhooks are reachable, because they need a service
    token and are unavailable on some plans. When they are not, `pull-dbt-cloud`
    on a cron is the supported alternative and this says so rather than leaving
    you to infer it from a 404.
    """
    from . import dbt_cloud
    from . import notify as notify_mod

    if not dbt_cloud.configured():
        console.print(
            f"[red]not configured[/]\n"
            f"[dim]Set {dbt_cloud.TOKEN_ENV} and {dbt_cloud.ACCOUNT_ENV}"
            f", and {dbt_cloud.HOST_ENV} if your account is not on"
            f" {dbt_cloud.DEFAULT_HOST}.[/]"
        )
        raise typer.Exit(1)

    import httpx

    token, account, host = dbt_cloud._config()
    console.print(f"host       {host}")
    console.print(f"account    {account}")
    console.print(
        f"namespace  {dbt_cloud.namespace() or '[dim]— (derived from the adapter)[/]'}"
    )
    console.print(
        f"base url   {notify_mod._base_url() or '[dim]— (messages carry no links)[/]'}"
    )

    problems: list[str] = []
    with httpx.Client(
        base_url=f"https://{host}",
        headers={"Authorization": f"Token {token}", "Accept": "application/json"},
        timeout=30,
    ) as client:
        response = client.get(f"/api/v2/accounts/{account}/")
        if response.status_code == 401:
            # A 401 cannot tell these apart from outside, and guessing "bad
            # token" sends people to re-issue a token that was fine. The host is
            # named first because it is the less obvious of the two and the one
            # that costs an afternoon.
            problems.append(
                f"rejected on {host}. Either the account is on a different host "
                f"— dbt Cloud is multi-cell, so check the hostname in the URL "
                f"you sign in with and set {dbt_cloud.HOST_ENV} to it — or "
                f"{dbt_cloud.TOKEN_ENV} is wrong. The host is the more common "
                f"cause and the cheaper one to rule out."
            )
        elif response.status_code == 404:
            problems.append(
                f"account {account} was not found on {host}. dbt Cloud is "
                f"multi-cell: check the host in the URL you use to sign in, and "
                f"set {dbt_cloud.HOST_ENV} to it"
            )
        elif response.status_code != 200:
            problems.append(f"the account endpoint returned HTTP {response.status_code}")
        else:
            data = response.json().get("data") or {}
            console.print(f"           [green]reachable[/] — {data.get('name')!r}")

        if not problems:
            jobs = client.get(f"/api/v2/accounts/{account}/jobs/")
            if jobs.status_code == 200:
                rows = jobs.json().get("data") or []
                console.print(f"\n[bold]jobs visible[/] ({len(rows)})")
                if not rows:
                    console.print("  [yellow]none[/] — nothing to poll for yet")
                for job in rows:
                    console.print(
                        f"  {job['id']}  {job['name']}  "
                        f"[dim]{' · '.join(job.get('execute_steps') or [])}[/]"
                    )
            else:
                problems.append(f"listing jobs returned HTTP {jobs.status_code}")

            hooks = client.get(f"/api/v3/accounts/{account}/webhooks/subscriptions")
            console.print()
            if hooks.status_code == 200:
                subs = hooks.json().get("data") or []
                console.print(f"[bold]webhooks[/] available — {len(subs)} subscription(s)")
                for s in subs:
                    state = "[green]active[/]" if s.get("active") else "[dim]inactive[/]"
                    console.print(f"  {s.get('id')}  {s.get('client_url')}  {state}")
                if not env.get(dbt_cloud.SECRET_ENV):
                    problems.append(
                        f"{dbt_cloud.SECRET_ENV} is unset, so the webhook endpoint "
                        f"refuses every request — set it to the secret dbt Cloud "
                        f"returned when the subscription was created"
                    )
            else:
                # Not an error. It is a plan and token-type limit, and the
                # alternative is supported rather than a workaround.
                console.print(
                    "[yellow]webhooks unavailable[/] on this account "
                    f"[dim](HTTP {hooks.status_code})[/]\n"
                    "[dim]They need a service token, which some plans do not offer.\n"
                    "Run `wishd pull-dbt-cloud` from cron instead — every\n"
                    "minute keeps the live feed close to real time.[/]"
                )

    for problem in problems:
        console.print(f"\n[yellow]![/] {problem}")
    raise typer.Exit(1 if problems else 0)


@app.command("pull-dbt-cloud")
def pull_dbt_cloud(
    since_hours: float = typer.Option(24, help="How far back to look for finished runs."),
    limit: int = typer.Option(50, help="Most runs to consider in one pass."),
) -> None:
    """Fetch finished dbt Cloud runs and record them as ordinary runs.

    dbt Cloud emits no OpenLineage, but it serves the same run_results.json and
    manifest.json that dbt-core writes to target/. This collects them and builds
    the run tree from them, so dbt Cloud gets the timeline, the live feed, run
    failures, lineage and tests with nothing downstream special-cased.

        wishd pull-dbt-cloud        # from cron, every few minutes

    A webhook (POST /api/v1/dbt-cloud/webhook) does the same thing seconds after
    a job finishes. Run both: this is the backstop for whatever the webhook
    dropped, and ingest is idempotent so the overlap costs nothing.

        DATASPINE_DBT_CLOUD_TOKEN=dbtc_...
        DATASPINE_DBT_CLOUD_ACCOUNT=12345
    """
    from datetime import datetime, timedelta

    from . import dbt_cloud
    from .db import connection

    if not dbt_cloud.configured():
        console.print(
            f"[yellow]dbt Cloud is not configured.[/]\n"
            f"[dim]Set {dbt_cloud.TOKEN_ENV} and {dbt_cloud.ACCOUNT_ENV}.[/]"
        )
        raise typer.Exit(1)

    since = datetime.now(UTC) - timedelta(hours=since_hours)
    try:
        with connection() as conn:
            results = dbt_cloud.pull(conn, since=since, limit=limit)
            conn.commit()
    except dbt_cloud.DbtCloudError as exc:
        console.print(f"[red]{exc}[/]")
        raise typer.Exit(1) from exc

    if not results:
        console.print("[dim]no finished dbt Cloud runs in the window[/]")
        return

    table = Table(box=None, pad_edge=False, header_style="dim")
    table.add_column("run")
    table.add_column("events", justify="right")
    table.add_column("checks", justify="right")
    table.add_column("note", style="dim")
    for row in results:
        note = row.get("error") or row.get("skipped") or ""
        table.add_row(
            row["run_id"],
            str(row.get("events", "–")),
            str(row.get("checks", "–")),
            f"[red]{note}[/]" if row.get("error") else note,
        )
    console.print(table)

    failed = sum(1 for r in results if r.get("error"))
    if failed:
        console.print(f"\n[red]{failed}[/] run(s) could not be ingested")
        raise typer.Exit(1)


@app.command("push-artifacts")
def push_artifacts_command(
    run_id: str = typer.Argument(..., help="The OpenLineage run id to attach these to."),
    directory: Path = typer.Option(Path("target"), help="dbt target/ directory."),
    url: str = typer.Option("http://localhost:8080", help="Gateway URL."),
    token: str = typer.Option("", help="Bearer token, if the gateway has auth enabled."),
) -> None:
    """Push dbt artifacts for a run, so they outlive the node that made them.

    Run this as the last step of the task that invoked dbt:

        wishd push-artifacts "$OPENLINEAGE_RUN_ID" --directory target/
    """
    pushed = push_artifacts(directory, run_id, url=url, token=token)
    if pushed:
        console.print(f"[green]pushed[/] {', '.join(pushed)}")
    else:
        console.print(f"[dim]nothing to push from {directory}[/]")


@app.command("ingest-dbt-run")
def ingest_dbt_run_command(
    directory: Path = typer.Option(Path("target"), help="dbt target/ directory."),
    job_name: str = typer.Option("", help="Label the root run, as dbt Cloud's job name does."),
    dbt_cloud_url: str = typer.Option(
        "", help="Where the run's `dbt Cloud` link should point; usually the project."
    ),
    url: str = typer.Option("http://localhost:8080", help="Gateway URL."),
    token: str = typer.Option("", help="Bearer token, if the gateway has auth enabled."),
) -> None:
    """Record a finished dbt invocation from its artifacts, tree and all.

    For dbt runs nothing else reports -- the dbt Cloud CLI above all, which runs
    on dbt Cloud but never appears in the Admin API's run list, so
    `pull-dbt-cloud` cannot see it:

        dbt run && dbt test          # dbt Cloud CLI, artifacts land in target/
        wishd ingest-dbt-run --directory target/

    Use `push-artifacts` instead when the stack already emits OpenLineage
    through `dbt-ol`. This synthesises the run tree; running both would record
    every run twice.
    """
    result = ingest_dbt_run(
        directory, job_name=job_name, dbt_cloud_url=dbt_cloud_url, url=url, token=token
    )
    if not result["events"]:
        console.print(f"[dim]no run_results.json in {directory}[/]")
        return
    console.print(
        f"[green]recorded[/] {result['job']} as {result['run_id']} ({result['events']} events)"
    )
    if result["pushed"]:
        console.print(f"[dim]artifacts: {', '.join(result['pushed'])}[/]")


@app.command("ingest-eventlog")
def ingest_eventlog(
    path: str = typer.Argument(
        ...,
        help="An event log, a directory of them, or an s3:// object or prefix.",
    ),
    relink: bool = typer.Option(True, help="Attach metrics whose run arrived later."),
) -> None:
    """Ingest Spark event logs and join them to runs by application id.

    Works retroactively: point it at the S3 prefix EMR has been writing event
    logs to and it will import history that predates wishd entirely.
    """
    from . import s3
    from .backfill import backfill_directory
    from .db import connection
    from .spark_metrics import store
    from .sparklog import parse_event_log

    if s3.is_s3_uri(path):
        is_many = not s3.is_object(path)
    else:
        path = Path(path)  # type: ignore[assignment]
        is_many = path.is_dir()

    with connection() as conn:
        if is_many:
            stats = backfill_directory(conn, path, relink=relink)
            console.print(
                f"[green]ingested[/] {stats['ingested']} application(s), "
                f"linked {stats['linked']}"
                + (f", relinked {stats.get('relinked', 0)}" if relink else "")
                + (f"  [yellow]{stats['failed']} failed[/]" if stats["failed"] else "")
            )
            return

        summary = parse_event_log(path)
        if not summary.app_id:
            console.print(f"[yellow]no application id in {path}[/] — not an event log?")
            raise typer.Exit(1)
        run_id = store(conn, summary, source_uri=str(path))
        console.print(
            f"[green]ingested[/] {summary.app_id} "
            f"({summary.task_count} tasks, {len(summary.stages)} stages)"
            + (f" → run {str(run_id)[:8]}" if run_id else "  [yellow]no matching run yet[/]")
        )
        if summary.truncated:
            console.print("[yellow]log was truncated[/] — totals are partial")


# ----------------------------------------------------------------------- seed


@app.command()
def seed(
    pipelines: int = typer.Option(3, help="How many DAG runs to generate."),
    fail: bool = typer.Option(True, help="Make the last pipeline fail on a Spark stage."),
    shuffle: bool = typer.Option(True, help="Deliver events out of causal order."),
    url: str = typer.Option("", help="POST to a running gateway instead of writing directly."),
    token: str = typer.Option("", help="Bearer token, if the gateway has auth enabled."),
    live: int = typer.Option(
        0, help="Also leave this many pipelines mid-execution, overlapping in time."
    ),
    batch: bool = typer.Option(True, help="Use the batch endpoint instead of one POST per event."),
) -> None:
    """Generate a realistic Airflow → dbt → Spark/EMR pipeline and ingest it.

    No AWS account required. The events are the same shapes the real
    integrations emit, so anything that works here works against real EMR.

    `--live` leaves pipelines in flight, which is the only way to see the
    overview board and the running lane do anything. They are produced by
    truncating a real pipeline's event sequence at a cutoff rather than by
    building a separate "running" scenario -- see `simulate.truncate_at`.
    """
    from datetime import datetime, timedelta

    from .simulate import build_pipeline, shuffle_events, truncate_at

    now = datetime.now(UTC)
    events: list[dict[str, Any]] = []
    for i in range(pipelines):
        is_last = i == pipelines - 1
        events += build_pipeline(
            fail_model="fct_order_items" if (fail and is_last) else None,
            start=now - timedelta(days=pipelines - i - 1, minutes=45),
        )

    if live:
        # Stagger the starts across one pipeline's own span, so each is caught at
        # a different point of progress and they overlap rather than stack --
        # concurrency is the thing the overview page exists to show.
        #
        # The span is measured from a built pipeline rather than hardcoded. A
        # fixed offset silently stops working the moment the scenario grows a
        # step: start a pipeline longer ago than it takes to run and it is not
        # in flight at all, it is simply finished, which is the bug this
        # replaces.
        probe = build_pipeline(start=now)
        moments = [datetime.fromisoformat(e["eventTime"]) for e in probe]
        span = max(moments) - min(moments)
        for i in range(live):
            started = now - span * ((i + 1) / (live + 1))
            events += truncate_at(
                build_pipeline(dag_id=f"live_pipeline_{i + 1}", start=started), now
            )
    if shuffle:
        events = shuffle_events(events, seed=1337)

    if url:
        import httpx

        headers = _auth_headers(token)
        base = url.rstrip("/")
        with httpx.Client(timeout=30, headers=headers) as client:
            if batch:
                resp = client.post(f"{base}/api/v1/lineage/batch", json=events)
                _abort_on_auth_error(resp)
                body = resp.json()
                accepted = body.get("accepted", 0)
                if body.get("failures"):
                    console.print(f"[yellow]{body['rejected']} rejected[/]: {body['failures'][:3]}")
            else:
                accepted = 0
                for event in events:
                    resp = client.post(f"{base}/api/v1/lineage", json=event)
                    _abort_on_auth_error(resp)
                    if resp.status_code in (200, 201) and resp.json().get("accepted"):
                        accepted += 1
        console.print(f"[green]{accepted}/{len(events)}[/] events accepted by {base}")
    else:
        from .db import connection
        from .events import RunEvent
        from .ingest import ingest_run_event

        with connection() as conn:
            for event in events:
                ingest_run_event(conn, RunEvent.model_validate(event))
        console.print(f"[green]{len(events)}[/] events ingested directly")

    console.print("[dim]try:[/] wishd runs --roots")


# ----------------------------------------------------------------------- reads


@app.command()
def runs(
    integration: str = typer.Option("", help="AIRFLOW | DBT | SPARK"),
    state: str = typer.Option("", help="RUNNING | COMPLETED | FAILED | ABORTED"),
    job: str = typer.Option("", help="Substring match on job name."),
    roots: bool = typer.Option(False, "--roots", help="Only top-level runs."),
    limit: int = 25,
) -> None:
    """List runs."""
    from . import queries
    from .db import connection

    with connection() as conn:
        rows = queries.list_runs(
            conn,
            integration=integration or None,
            state=state or None,
            job_name=job or None,
            roots_only=roots,
            limit=limit,
        )

    table = Table(box=None, pad_edge=False, header_style="dim")
    table.add_column("run id", style="dim", no_wrap=True)
    table.add_column("job")
    table.add_column("intg", style="cyan")
    table.add_column("state")
    table.add_column("dur", justify="right")
    table.add_column("started", style="dim")
    for r in rows:
        table.add_row(
            str(r["run_id"])[:RUN_ID_DISPLAY],
            r["job_name"],
            r["integration"] or "–",
            f"[{STATE_STYLE.get(r['state'], '')}]{r['state']}[/]",
            _fmt_duration(r["duration_ms"]),
            r["started_at"].strftime("%m-%d %H:%M:%S") if r["started_at"] else "–",
        )
    console.print(table)
    console.print(f"[dim]{len(rows)} run(s)[/]")


@app.command()
def tree(
    run_id: str,
    here: bool = typer.Option(False, "--here", help="Start at this run, not the root."),
) -> None:
    """Render the full run tree containing a run.

    This is the Phase 00 payoff: one command that takes any run id — an Airflow
    task, a dbt model, a Spark SQL execution — and shows the entire pipeline
    execution it belongs to, including which datasets it touched.
    """
    from . import queries
    from .db import connection

    with connection() as conn:
        rid = _resolve_run_id(conn, run_id)
        start = rid if here else (queries.root_of(conn, rid) or rid)
        nodes = queries.run_tree(conn, start)
        if not nodes:
            console.print("[red]run not found[/]")
            raise typer.Exit(1)
        datasets = queries.tree_datasets(conn, start)

    by_parent: dict[Any, list[dict]] = {}
    for n in nodes:
        by_parent.setdefault(n["parent_run_id"], []).append(n)

    def label(n: dict) -> str:
        style = STATE_STYLE.get(n["state"], "")
        bits = [
            f"[bold]{n['job_name']}[/]",
            f"[cyan]{n['integration'] or '?'}[/]",
            f"[{style}]{n['state']}[/]",
            f"[dim]{_fmt_duration(n['duration_ms'])}[/]",
        ]
        if n["is_placeholder"]:
            bits.append("[yellow](placeholder — no events received)[/]")
        return "  ".join(bits)

    root_node = nodes[0]
    rendered = Tree(label(root_node))

    def attach(parent_tree: Tree, parent_id: Any) -> None:
        for child in by_parent.get(parent_id, []):
            branch = parent_tree.add(label(child))
            if child["error_message"]:
                first_line = child["error_message"].splitlines()[0]
                branch.add(f"[red]{first_line[:140]}[/]")
            attach(branch, child["run_id"])

    attach(rendered, root_node["run_id"])
    console.print()
    console.print(rendered)

    if datasets:
        console.print()
        dt = Table(title="datasets touched", box=None, title_justify="left",
                   header_style="dim", title_style="dim")
        dt.add_column("dir", style="dim")
        dt.add_column("dataset")
        dt.add_column("rows", justify="right")
        for d in datasets:
            dt.add_row(
                d["direction"].lower(),
                f"{d['namespace']}/{d['name']}",
                f"{d['row_count']:,}" if d["row_count"] else "–",
            )
        console.print(dt)


@app.command()
def health() -> None:
    """Ingest health. Watch `unstitched_runs` — it is the correlation alarm."""
    from . import queries
    from .db import connection

    with connection() as conn:
        stats = queries.ingest_health(conn)

    by_integration = stats.pop("runs_by_integration", {})
    table = Table(box=None, show_header=False, pad_edge=False)
    for key, value in stats.items():
        style = "red" if key == "unstitched_runs" and value else ""
        table.add_row(f"[dim]{key}[/]", f"[{style}]{value}[/]" if style else str(value))
    console.print(table)
    if by_integration:
        console.print("\n[dim]runs by integration[/]")
        for k, v in by_integration.items():
            console.print(f"  {k:<12} {v}")


@app.command("emit")
def emit(
    file: Path,
    url: str = "http://localhost:8080",
    token: str = typer.Option("", help="Bearer token, if the gateway has auth enabled."),
) -> None:
    """POST a JSON file of OpenLineage events (one object, or an array) to a gateway."""
    import httpx

    payload = json.loads(file.read_text())
    events = payload if isinstance(payload, list) else [payload]
    with httpx.Client(timeout=30, headers=_auth_headers(token)) as client:
        resp = client.post(url.rstrip("/") + "/api/v1/lineage/batch", json=events)
        _abort_on_auth_error(resp)
        console.print(resp.status_code, resp.json())


def _auth_headers(token: str) -> dict[str, str]:
    """Explicit --token wins; otherwise fall back to the environment, so the
    token generated by `make up` into .env just works."""
    token = token or env.get("DATASPINE_API_TOKENS", "")
    if not token:
        return {}
    # .env may hold several labelled tokens; any one of them authenticates.
    first = token.split(",")[0].strip()
    return {"Authorization": f"Bearer {first.split(':', 1)[-1]}"}


def _abort_on_auth_error(resp: Any) -> None:
    if resp.status_code == 401:
        console.print(
            "[red]401 unauthorized.[/] The gateway has auth enabled.\n"
            "[dim]Pass --token, or set DATASPINE_API_TOKENS (make up writes one to .env).[/]"
        )
        raise typer.Exit(1)


def load_dotenv(path: Path = Path(".env")) -> dict[str, str]:
    """Load product settings; exported values beat either spelling in the file."""
    loaded: dict[str, str] = {}
    if not path.exists():
        return loaded
    settings: dict[str, str] = {}
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key, value = key.strip(), value.strip().strip('"').strip("'")
        if key.startswith(("WISHD_", "DATASPINE_")):
            settings[key] = value
    exported = set(os.environ)
    for key, value in settings.items():
        suffix = key.split("_", 1)[1]
        if {f"WISHD_{suffix}", f"DATASPINE_{suffix}"} & exported:
            continue
        os.environ[key] = value
        loaded[key] = value
    return loaded


def main() -> None:  # pragma: no cover
    load_dotenv()
    app()


if __name__ == "__main__":  # pragma: no cover
    main()
