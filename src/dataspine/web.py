"""Web UI.

Server-rendered pages, no build step, no framework (ADR-002). Four views, which
is the whole of what Phase 01 needs:

    /              run list, filterable
    /runs/{id}     one run: tree, error, SQL, datasets, history of the same job
    /jobs          every job with its latest state
    /login         set the auth cookie

The job of this UI is narrow and worth stating: make a failed nightly dbt run
debuggable without SSH-ing anywhere or reading an S3 log. Everything on the run
page is there because it answers a question you would otherwise answer by hand.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import (
    artifacts,
    auth,
    catalog,
    checks,
    cost,
    identity,
    incidents,
    lineage,
    links,
    platforms,
    queries,
    resources,
    spark_metrics,
    timing,
)
from . import heuristics as heuristics_mod
from . import monitors as monitors_mod
from .config import env
from .db import connection

HERE = Path(__file__).parent
templates = Jinja2Templates(directory=str(HERE / "templates"))
web = APIRouter()


# ------------------------------------------------------------ template filters


def _duration(ms: float | None) -> str:
    if ms is None:
        return "–"
    seconds = ms / 1000
    if seconds < 1:
        return f"{ms:.0f}ms"
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, seconds = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}m{seconds:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m"


def _rows(value: int | None) -> str:
    return f"{value:,}" if value else "–"


def _short(value: Any) -> str:
    return str(value)[:8] if value else ""


def _filesize(value: int | None) -> str:
    if not value:
        return "–"
    size = float(value)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:.0f}{unit}" if unit == "B" else f"{size:.1f}{unit}"
        size /= 1024
    return f"{size:.1f}GB"


templates.env.filters["filesize"] = _filesize
templates.env.filters["duration"] = _duration
templates.env.filters["rows"] = _rows
templates.env.filters["short"] = _short

# Exposed as globals rather than filters because they return a Platform tuple the
# template destructures, not a formatted string.
templates.env.globals["infrastructure"] = platforms.infrastructure
templates.env.globals["tool"] = platforms.tool
templates.env.globals["activity"] = platforms.activity


THEME_COOKIE = "dataspine_theme"
THEMES = ("light", "dark")


def _theme(request: Request) -> str:
    """The stored theme choice, or "" meaning follow the OS.

    Empty is a real third state, not a default standing in for one. With nothing
    stamped on <html>, the stylesheet's media query decides -- which is what
    someone who has never touched the control should get, and what they get back
    if they pick "Auto".
    """
    value = request.cookies.get(THEME_COOKIE, "")
    return value if value in THEMES else ""


def _render(request: Request, name: str, **context: Any) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        name,
        {
            "auth_enabled": auth.auth_enabled(),
            # The nav rail is chrome for someone already inside the tool. The
            # login page opts out with `{% set chrome = false %}`; everything
            # else gets it.
            "chrome": True,
            "breach_count": _breach_count(),
            "incident_count": _incident_count(),
            "theme": _theme(request),
            "here": request.url.path,
            **context,
        },
    )


def _breach_count() -> int:
    """How many monitors are in breach, for the nav badge.

    On every page on purpose: someone looking at a run list should find out that
    a table is stale without navigating to a second page to ask. Counted from the
    denormalised `monitors.last_status` so it is one index-free scan of a table
    with tens of rows, not a join over the results history.

    Never raises. A failed badge query must not 500 the page it decorates -- and
    this runs before migration 007 has been applied on an upgrading install.
    """
    try:
        with connection() as conn:
            row = conn.execute(
                "select count(*) as n from monitors where enabled and last_status = 'breach'"
            ).fetchone()
        return row["n"] if row else 0
    except Exception:  # noqa: BLE001 - a nav badge is never worth an error page
        return 0


def _incident_count() -> int:
    """How many incidents are open, for the nav badge.

    Read from the `incidents` table rather than recomputed with
    `incidents.detect`: detection walks lineage and writes, and a nav badge that
    ran on every page render would make every page pay for it. The stored rows
    are written by the incidents page and by the alerting sweep, so the badge
    trails those by at most one sweep -- which is the right trade for a number
    whose job is to make someone click through.

    Never raises, for the same reason `_breach_count` does not.
    """
    try:
        with connection() as conn:
            row = conn.execute(
                "select count(*) as n from incidents where resolved_at is null"
            ).fetchone()
        return row["n"] if row else 0
    except Exception:  # noqa: BLE001 - a nav badge is never worth an error page
        return 0


SINCE_PRESETS = {
    "1h": timedelta(hours=1),
    "24h": timedelta(hours=24),
    "7d": timedelta(days=7),
    "30d": timedelta(days=30),
}


def _parse_since(value: str) -> datetime | None:
    """Accept a preset (`24h`) or an ISO timestamp.

    Presets cover what people actually want from a run list; the ISO form keeps
    the URL useful for linking to a specific incident window. Anything
    unparseable narrows nothing rather than erroring -- a hand-edited URL should
    not 500 the landing page.
    """
    if not value:
        return None
    if value in SINCE_PRESETS:
        return datetime.now(UTC) - SINCE_PRESETS[value]
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed


def _guard(request: Request) -> RedirectResponse | None:
    """Send unauthenticated browsers to the login form rather than a bare 401.

    The API returns 401 because machines handle status codes; a human who
    bookmarked a run URL should get a form.
    """
    if auth.check(request):
        return None
    return RedirectResponse(f"/login?next={request.url.path}", status_code=303)


# ------------------------------------------------------------------- run list


@web.get("/", response_class=HTMLResponse)
def index(
    request: Request,
    integration: str = "",
    state: str = "",
    job: str = "",
    roots: bool = True,
    since: str = "",
    page: int = 1,
) -> Any:
    if redirect := _guard(request):
        return redirect

    page = max(page, 1)
    per_page = 20  # pipelines, not runs -- each card carries its whole tree
    with connection() as conn:
        pipelines = queries.list_pipelines(
            conn,
            integration=integration or None,
            state=state or None,
            job_name=job or None,
            since=_parse_since(since),
            limit=per_page + 1,  # one extra tells us whether a next page exists
            offset=(page - 1) * per_page,
        )
        health = queries.ingest_health(conn)

    has_next = len(pipelines) > per_page
    pipelines = pipelines[:per_page]

    # Three lanes, decided by the root run. Sorting into lanes here rather than
    # in SQL keeps it one query and one ordering; the page is 20 rows, so the
    # cost is nil and the rule stays readable next to `platforms.lane`.
    lanes: dict[str, list[Any]] = {"running": [], "queued": [], "finished": []}
    for pipeline in pipelines:
        root = pipeline["root"]
        key = platforms.lane(root["state"], root["started_at"])
        if key == "finished" and pipeline["any_running"]:
            # The root said COMPLETE but something beneath it is still going --
            # which happens whenever a producer reports its own terminal state
            # without waiting for children. Live work belongs in the live lane.
            key = "running"
        lanes[key].append(pipeline)

    return _render(
        request,
        "runs.html",
        lanes=lanes,
        pipeline_count=len(pipelines),
        health=health,
        page=page,
        has_next=has_next,
        filters={
            "integration": integration,
            "state": state,
            "job": job,
            "roots": roots,
            "since": since,
        },
    )


# --------------------------------------------------------------------- overview


# How much slower than its own median a live pipeline must be before the board
# says so. 1.5x rather than something tighter because the baseline is a median
# over ~20 runs and pipeline durations are genuinely noisy -- a threshold that
# fires on ordinary variance trains people to ignore the badge, which costs more
# than the warning was worth.
SLOW_FACTOR = 1.5

# How many executions on the timeline carry an inline tree. `<details>` renders
# its contents whether open or shut, so every expandable row is paid for on
# every page load -- which is the cost ADR-002 accepts in exchange for no build
# step and no partial fetch. A 7-day window can hold hundreds of executions and
# nobody opens hundreds, so past this they link to the run page instead.
INLINE_TREE_LIMIT = 40

# How long a RUNNING pipeline may go without emitting any event before the board
# stops calling it live. OpenLineage has no heartbeat: a producer speaks when
# work starts and when it ends, and if the cluster dies in between it never
# speaks again. Those runs stay RUNNING in the database forever, and any
# "elapsed" computed from `now` grows without bound -- a run abandoned two weeks
# ago renders as the busiest thing on the page.
#
# 45 minutes because the gap between events within one pipeline is minutes, not
# hours: a dbt model completing, a Spark SQL execution starting. Silence past
# that is much more likely to be a dead producer than a slow query, and the
# board says "silent for", which is what we actually know, rather than "dead",
# which we are inferring.
STALE_AFTER = timedelta(minutes=45)

TIMELINE_WINDOWS = {
    "1h": timedelta(hours=1),
    "6h": timedelta(hours=6),
    "24h": timedelta(hours=24),
    "7d": timedelta(days=7),
}


@web.get("/overview", response_class=HTMLResponse)
def overview(request: Request, window: str = "6h") -> Any:
    """Everything at once: what is live now, and how executions overlap.

    Two halves answering two different questions, on one page because at 02:00
    they are asked together. The board answers "what is happening and is any of
    it wrong". The timeline answers "what is happening *at the same time*",
    which is the question the run list structurally cannot answer -- it is
    ordered by start time, so concurrent work is interleaved rows rather than
    parallel lanes, and the 02:00 pile-up is invisible in it.

    No JavaScript and no polling loop (ADR-002). The bars are server-computed
    percentages; a browser refresh is the update mechanism, which is honest
    about staleness in a way a silently-failing fetch is not.
    """
    if redirect := _guard(request):
        return redirect

    span = TIMELINE_WINDOWS.get(window) or TIMELINE_WINDOWS["6h"]
    window = window if window in TIMELINE_WINDOWS else "6h"
    now = datetime.now(UTC)
    since = now - span

    with connection() as conn:
        live = queries.live_pipelines(conn)
        baselines = queries.job_baselines(conn, [p["job_name"] for p in live])
        executions = queries.concurrency_timeline(conn, since=since)
        health = queries.ingest_health(conn)
        breaching = monitors_mod.open_breaches(conn)

        # Every tree the page can expand, in one query rather than one per
        # disclosure. `<details>` renders its contents whether or not it is
        # open, so this is fetched up front -- which is the trade ADR-002 buys:
        # no partial fetch, so the page carries what it might need. Capped,
        # because a 7-day window can hold far more executions than anyone will
        # open, and an unbounded page is a worse failure than a truncated one.
        expandable = [p["run_id"] for p in live]
        chosen = set(expandable)
        for execution in _worth_expanding(executions):
            if len(expandable) >= INLINE_TREE_LIMIT + len(live):
                break
            if execution["run_id"] not in chosen:
                chosen.add(execution["run_id"])
                expandable.append(execution["run_id"])
        trees = queries.trees_for(conn, expandable)

    cards = [_live_card(pipeline, baselines, now) for pipeline in live]
    for card in cards:
        card["tree"] = trees.get(card["run_id"], [])
    # Stale pipelines are separated rather than filtered out. Dropping them would
    # hide a real failure -- work that died without telling anyone is exactly
    # what this project is for -- but leaving them mixed in makes every count on
    # the page wrong, starting with "how many are live".
    board = [c for c in cards if not c["stale"]]
    stale = [c for c in cards if c["stale"]]
    lanes = _timeline_lanes(executions, since=since, now=now, trees=trees)
    peak = queries.peak_concurrency(executions, now=now)

    return _render(
        request,
        "overview.html",
        board=board,
        stale=stale,
        lanes=lanes,
        peak=peak,
        infra_load=_infra_load(board),
        breaching=breaching,
        health=health,
        window=window,
        stale_after_minutes=int(STALE_AFTER.total_seconds() // 60),
        windows=list(TIMELINE_WINDOWS),
        since=since,
        now=now,
        late_count=sum(1 for card in board if card["slow"]),
    )


def _worth_expanding(executions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Executions in the order they deserve an inline tree, best first.

    The cap has to fall somewhere, so it should fall on the rows nobody opens.
    You expand a lane to investigate something, and the things worth
    investigating are what broke and what is still going -- a fortnight of green
    runs is the part you scroll past. Failed first, then live, then most recent.
    """
    def rank(run: dict[str, Any]) -> tuple[int, Any]:
        if run["state"] == "FAILED":
            priority = 0
        elif run["ended_at"] is None:
            priority = 1
        else:
            priority = 2
        return (priority, -(run["started_at"].timestamp() if run["started_at"] else 0))

    return sorted(executions, key=rank)


def _live_card(pipeline: dict[str, Any], baselines: dict[str, float], now: datetime) -> dict:
    """One row of the live board: progress, where it runs, and whether it is slow."""
    started = pipeline["started_at"]
    elapsed_ms = (now - started).total_seconds() * 1000 if started else None
    baseline = baselines.get(pipeline["job_name"])

    # Only claim "slow" with a baseline to compare against. Without history the
    # honest statement is the elapsed time itself, not a judgement about it.
    slow = bool(baseline and elapsed_ms and elapsed_ms > baseline * SLOW_FACTOR)

    last_event = pipeline["last_event_at"]
    silent_ms = (now - last_event).total_seconds() * 1000 if last_event else None

    total = pipeline["total"] or 1
    infra = [
        platforms.infrastructure(namespace, "SPARK")
        for namespace in (pipeline["spark_namespaces"] or "").split("|")
        if namespace
    ]
    return {
        **pipeline,
        "elapsed_ms": elapsed_ms,
        "baseline_ms": baseline,
        "slow": slow,
        "slow_factor": (elapsed_ms / baseline) if slow and baseline else None,
        "silent_ms": silent_ms,
        "stale": bool(last_event and now - last_event > STALE_AFTER),
        "percent": round(100 * (pipeline["done"] or 0) / total),
        "chain": [i for i in (pipeline["integrations"] or "").split(",") if i],
        "infra": infra,
    }


def _infra_load(board: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Which infrastructure is carrying how many live pipelines.

    The contention signal this project exists to surface: two pipelines on one
    EMR cluster is the ordinary explanation for both being slow, and it is
    invisible on any per-pipeline view.
    """
    load: dict[tuple[str, str], dict[str, Any]] = {}
    for card in board:
        for infra in card["infra"]:
            key = (infra.key, infra.detail)
            entry = load.setdefault(
                key, {"platform": infra, "pipelines": [], "count": 0}
            )
            if card["job_name"] not in entry["pipelines"]:
                entry["pipelines"].append(card["job_name"])
                entry["count"] += 1
    return sorted(load.values(), key=lambda e: -e["count"])


def _timeline_lanes(
    executions: list[dict[str, Any]],
    *,
    since: datetime,
    now: datetime,
    trees: dict[Any, list[dict[str, Any]]] | None = None,
) -> list[dict[str, Any]]:
    """Group executions into one lane per pipeline, with bar geometry.

    Offsets are percentages of the window so the bars need no JavaScript and no
    fixed pixel width -- the lane is a flex row and the browser does the layout.
    A bar that starts before the window is clamped to 0 and marked, rather than
    given a negative offset that would render off-screen: a long-running
    pipeline that began before the window is the most important row on the page.
    """
    span_seconds = max((now - since).total_seconds(), 1)
    lanes: dict[str, dict[str, Any]] = {}

    for run in executions:
        start = run["started_at"]
        end = run["ended_at"] or now
        if start is None:
            continue
        offset = (start - since).total_seconds() / span_seconds
        width = (end - start).total_seconds() / span_seconds
        clipped = offset < 0
        if clipped:
            width += offset  # the visible remainder only
            offset = 0.0

        lane = lanes.setdefault(
            run["job_name"], {"job_name": run["job_name"], "bars": [], "failures": 0}
        )
        if run["state"] == "FAILED":
            lane["failures"] += 1
        lane["bars"].append(
            {
                "run_id": run["run_id"],
                "state": run["state"],
                "started_at": start,
                "duration_ms": run["duration_ms"],
                "live": run["ended_at"] is None,
                "clipped": clipped,
                # A floor of 0.6% so a 3-second run is still a clickable target
                # rather than a hairline nobody can hit.
                "left": round(100 * max(offset, 0.0), 3),
                "width": round(100 * max(width, 0.006), 3),
                # Absent past the cap: the row then links out instead of
                # expanding, which is a smaller loss than an unbounded page.
                "tree": (trees or {}).get(run["run_id"], []),
            }
        )

    return sorted(
        lanes.values(),
        key=lambda lane: (-lane["failures"], lane["job_name"]),
    )


# ------------------------------------------------------------------ run detail


@web.get("/runs/{run_id}", response_class=HTMLResponse)
def run_detail(request: Request, run_id: UUID) -> Any:
    if redirect := _guard(request):
        return redirect

    with connection() as conn:
        run = queries.get_run(conn, run_id)
        if run is None:
            return _render(request, "not_found.html", run_id=run_id)

        root = run["root_run_id"] or run_id
        nodes = queries.run_tree(conn, root)
        tree_datasets = queries.tree_datasets(conn, root)
        own_datasets = queries.run_datasets(conn, run_id)
        history = queries.job_history(conn, run["job_id"], limit=20)
        run_artifacts = artifacts.list_artifacts(conn, run_id)
        spark = spark_metrics.get_for_run(conn, run_id)
        facets = queries.run_facets(conn, run_id)
        # Fall back to the nearest ancestor's SQL when this run has none of its
        # own, labelled so nobody mistakes it for this run's query.
        sql = {"query": facets.get("sql"), "job_name": None, "hops": 0}
        if not sql["query"]:
            sql = queries.inherited_sql(conn, run_id) or sql
        actions = run_actions(conn, run, facets, base_url=_web_base_url(request))
        # A dbt Cloud tree states its URL once, on the root. Every node in it
        # belongs to that run, so the link is inherited rather than absent on
        # each of the children -- which is where a reader actually is when they
        # want it.
        inherited_links = []
        if not facets.get("run_facets", {}).get("dbt_cloud"):
            root_facets = queries.run_facets(conn, root).get("run_facets")
            inherited_links = links.run_links(run["integration"], root_facets)

    # Derived outside the connection block: pure functions over facets.
    run_links = links.run_links(run["integration"], facets.get("run_facets"))
    if not run_links:
        run_links = [item for item in inherited_links if item.get("url")]
    run_timing = timing.run_timing(facets.get("run_facets"), run["started_at"])

    # The tree is returned flat with a level; nest it for rendering.
    by_parent: dict[Any, list[dict]] = {}
    for node in nodes:
        by_parent.setdefault(node["parent_run_id"], []).append(node)

    def nest(node: dict) -> dict:
        return {
            "run": node,
            "children": [nest(child) for child in by_parent.get(node["run_id"], [])],
        }

    tree = nest(nodes[0]) if nodes else None

    return _render(
        request,
        "run_detail.html",
        run=run,
        tree=tree,
        current_run_id=str(run_id),
        tree_datasets=tree_datasets,
        own_datasets=own_datasets,
        history=history,
        sql=sql,
        run_links=run_links,
        actions=actions,
        artifacts=run_artifacts,
        spark=spark,
        findings=heuristics_mod.analyse(spark["metrics"]) if spark else [],
        timing=run_timing,
        humanize=timing.humanize,
        run_facets=facets.get("run_facets", {}),
    )


# ------------------------------------------------------ actions on a run page


def _web_base_url(request: Request) -> str | None:
    """Where a handoff link should point back to.

    `WISHD_BASE_URL` wins when it is set, because it is the address that works
    from wherever alerts are read. Failing that the request's own origin is a
    safe answer here in a way it is not in `notify`: the reader is holding that
    URL already, so it cannot be the guess that 404s.
    """
    from .notify import BASE_URL_ENV

    configured = env.get(BASE_URL_ENV, "").strip().rstrip("/")
    return configured or str(request.base_url).rstrip("/") or None


def _dbt_unique_id(job_name: str | None) -> str | None:
    """dbt's own `unique_id` back out of the job name we gave it.

    `dbt_artifacts.root_job_name` prefixes every node with its project, so
    `analytics.test.analytics.not_null_x.e887a2de02` is project `analytics`
    carrying `test.analytics.not_null_x.e887a2de02`. Recovering it is what lets
    the page find the check row -- and therefore the warehouse query and the
    briefing -- without storing a second copy of the id on the run.
    """
    if not job_name:
        return None
    _, _, rest = job_name.partition(".")
    kind, _, _ = rest.partition(".")
    return rest if kind in ("test", "model", "snapshot", "seed") else None


def _failing_child_unique_id(conn: Any, run: dict[str, Any]) -> str | None:
    """The node that took this invocation down with it.

    A dbt invocation's own run carries no check row -- it is a container, and
    what failed is one assertion inside it. Reading up from the child is what the
    Slack summary does when it says "1 failure" and puts the buttons in the
    thread. The page has no thread, so the invocation offers the actions of the
    failure it is reporting rather than offering nothing at all.
    """
    rows = conn.execute(
        """
        select j.name
        from runs r join jobs j on j.id = r.job_id
        where r.root_run_id = %(root)s and r.state = 'FAILED' and r.run_id <> %(root)s
        order by r.started_at desc
        """,
        {"root": run["run_id"]},
    ).fetchall()
    for row in rows:
        if unique_id := _dbt_unique_id(row["name"]):
            return unique_id
    return None


def run_actions(
    conn: Any, run: dict[str, Any], facets: dict[str, Any], *, base_url: str | None
) -> list[dict[str, Any]]:
    """The buttons a failed dbt node offers: warehouse, and hand it to an agent.

    The same three things the Slack thread reply offers, for the reader who
    arrived at the page instead of the alert. Alerts are not the only way in --
    someone following a run tree down to the failure has exactly the question the
    alert's buttons answer, and making them go find the Slack message first is
    the kind of gap that teaches people the page is the lesser surface.

    Built from the check row rather than the run, because that is where the
    warehouse query id lives: OpenLineage has nowhere to put it, so dbt's
    `run_results.json` is the only thing that ever knew it.
    """
    from . import agents

    if run.get("state") != "FAILED":
        return []
    unique_id = _dbt_unique_id(run.get("job_name")) or _failing_child_unique_id(conn, run)
    if not unique_id:
        return []
    row = conn.execute(
        """
        select source, table_name, check_name, status, value, measured_at, details
        from external_checks
        where details->>'unique_id' = %(unique_id)s
        order by measured_at desc
        limit 1
        """,
        {"unique_id": unique_id},
    ).fetchone()

    out: list[dict[str, Any]] = []
    query_link = links.snowflake_query((row.get("details") or {}).get("query_id")) if row else None
    if query_link:
        out.append({**query_link, "kind": "warehouse"})

    # Keyed the way `notify` keys the same node, so the button on the page and
    # the button in Slack open the same briefing rather than two of them.
    dedup_key = f"{run.get('root_run_id') or run['run_id']}/{unique_id}"
    briefing = agents.briefing_from_check_row(conn, row) if row else None
    for label, url in agents.handoff_urls("dbt_job", dedup_key, base_url, briefing):
        out.append({"label": label, "url": url, "kind": "agent"})
    return out


# ---------------------------------------------------------------------- jobs


@web.get("/jobs", response_class=HTMLResponse)
def jobs(request: Request) -> Any:
    if redirect := _guard(request):
        return redirect
    with connection() as conn:
        rows = queries.list_jobs(conn)
    return _render(request, "jobs.html", jobs=rows)


# ------------------------------------------------------------------ monitors


@web.get("/monitors", response_class=HTMLResponse)
def monitor_list(request: Request) -> Any:
    if redirect := _guard(request):
        return redirect
    with connection() as conn:
        rows = monitors_mod.list_monitors(conn)
        breaches = monitors_mod.open_breaches(conn)
        # The newest evaluation's message per monitor. A status word alone says
        # something is wrong without saying what, which is the least useful thing
        # a monitoring page can do.
        for row in rows:
            latest = monitors_mod.recent_results(conn, row["id"], limit=1)
            row["latest_message"] = latest[0]["message"] if latest else None
    return _render(request, "monitors.html", monitors=rows, breaches=breaches)


@web.get("/monitors/{name}", response_class=HTMLResponse)
def monitor_detail(request: Request, name: str) -> Any:
    if redirect := _guard(request):
        return redirect

    with connection() as conn:
        monitor = monitors_mod.get_monitor(conn, name)
        if monitor is None:
            return _render(request, "not_found.html", run_id=name)
        points = monitors_mod.recent_points(conn, monitor["id"], limit=60)
        results = monitors_mod.recent_results(conn, monitor["id"], limit=25)
        matches = (
            checks.resolve_datasets(
                conn, monitor["target"], namespace=(monitor["config"] or {}).get("namespace")
            )
            if monitor["target_kind"] == "dataset"
            else checks.resolve_jobs(conn, monitor["target"])
        )

    return _render(
        request,
        "monitor_detail.html",
        monitor=monitor,
        points=_scale(list(reversed(points))),  # oldest first, for the bar chart
        results=results,
        latest=results[0] if results else None,
        resolution=[f"{m['namespace']} / {m['name']}" for m in matches],
    )


@web.post("/monitors/{name}/feedback")
def monitor_feedback(
    request: Request,
    name: str,
    subject: str = Form(...),
    feedback: str = Form(""),
) -> Any:
    """Label one observation as expected or as a confirmed anomaly.

    A form post and a redirect, not JSON over fetch: ADR-002 rules out the
    JavaScript, and this is the one interaction in the UI that writes. An empty
    value clears the label, so a misclick is undoable.
    """
    if redirect := _guard(request):
        return redirect

    value = feedback if feedback in monitors_mod.FEEDBACK_VALUES else None
    with connection() as conn:
        monitor = monitors_mod.get_monitor(conn, name)
        if monitor is None:
            return _render(request, "not_found.html", run_id=name)
        monitors_mod.set_feedback(conn, monitor["id"], subject, value)
        conn.commit()
    return RedirectResponse(f"/monitors/{name}", status_code=303)


def _scale(points: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Attach a 0-100 bar height to each point.

    Scaled against the maximum rather than against zero-to-threshold: the shape of
    the series is what someone reads a history for, and a threshold-relative scale
    flattens every normal day into the same stub.
    """
    values = [p["value"] for p in points if p["value"] is not None]
    peak = max(values) if values else 0
    for point in points:
        value = point["value"]
        point["pct"] = round(100 * value / peak) if (peak and value is not None) else 0
    return points


# ------------------------------------------------- catalog, lineage, incidents


@web.get("/catalog", response_class=HTMLResponse)
def catalog_page(request: Request, q: str = "") -> Any:
    if redirect := _guard(request):
        return redirect
    with connection() as conn:
        if q.strip():
            matches = catalog.search(conn, q)
            ids = {m["id"] for m in matches}
            entries = [e for e in catalog.list_entries(conn) if e["id"] in ids]
            # Preserve search ranking rather than falling back to alphabetical:
            # the ordering is most of what a search result is.
            rank = {m["id"]: index for index, m in enumerate(matches)}
            entries.sort(key=lambda e: rank.get(e["id"], 999))
        else:
            entries = catalog.list_entries(conn)
    return _render(request, "catalog.html", entries=entries, query=q)


@web.get("/catalog/{entity_id}", response_class=HTMLResponse)
def catalog_entry(request: Request, entity_id: int) -> Any:
    if redirect := _guard(request):
        return redirect
    with connection() as conn:
        entry = catalog.get_entry(conn, entity_id)
        if entry is None:
            return _render(request, "not_found.html", run_id=f"entity {entity_id}")
        columns = lineage.column_edges(conn, entity_id=entity_id)
        # Where identity resolution declined to merge, say so. A graph showing no
        # downstream consumers is exactly the thing someone acts on.
        candidates = identity.unmerged_candidates(conn, entity_id)
    return _render(
        request, "catalog_entry.html", entry=entry, columns=columns, candidates=candidates
    )


@web.get("/lineage/{entity_id}", response_class=HTMLResponse)
def lineage_page(request: Request, entity_id: int, depth: int = 2) -> Any:
    if redirect := _guard(request):
        return redirect
    depth = max(1, min(depth, lineage.MAX_DEPTH))
    with connection() as conn:
        graph = lineage.graph(conn, entity_id, depth=depth)
        if graph["focus"] is None:
            return _render(request, "not_found.html", run_id=f"entity {entity_id}")
        columns = lineage.column_edges(conn, entity_id=entity_id)
    return _render(
        request, "lineage.html", graph=lineage.layout(graph), columns=columns, depth=depth
    )


@web.get("/incidents", response_class=HTMLResponse)
def incidents_page(request: Request) -> Any:
    if redirect := _guard(request):
        return redirect
    with connection() as conn:
        current = incidents.detect(conn, persist=True)
        conn.commit()
    return _render(request, "incidents.html", incidents=current)


@web.get("/costs", response_class=HTMLResponse)
def costs_page(request: Request) -> Any:
    if redirect := _guard(request):
        return redirect
    with connection() as conn:
        by_job = cost.by_job(conn)
        clusters = resources.list_clusters(conn)
        gap = cost.unattributed(conn)
        summaries = [cost.cluster_summary(conn, c["cluster_id"]) | dict(c) for c in clusters]
    return _render(
        request, "costs.html", by_job=by_job, clusters=summaries, unattributed=gap
    )


# --------------------------------------------------------------------- login


@web.get("/login", response_class=HTMLResponse)
def login_form(request: Request, next: str = "/", error: str = "") -> Any:
    return _render(request, "login.html", next=next, error=error)


@web.post("/login")
def login_submit(request: Request, token: str = Form(...), next: str = Form("/")) -> Any:
    from urllib.parse import urlencode

    next = auth.local_redirect(next)
    if auth.auth_enabled() and not auth.valid_token(token):
        query = urlencode({"next": next, "error": "1"})
        return RedirectResponse(f"/login?{query}", status_code=303)

    response = RedirectResponse(next or "/", status_code=303)
    response.set_cookie(
        auth.COOKIE_NAME,
        token,
        httponly=True,
        samesite="lax",
        max_age=60 * 60 * 24 * 30,
        secure=request.url.scheme == "https",
    )
    return response


@web.post("/theme")
def set_theme(theme: str = Form(""), next: str = Form("/")) -> Any:
    """Store a theme choice, or clear it to follow the OS again.

    A cookie and a form rather than a script, because ADR-002 means there is no
    JavaScript to persist a choice in localStorage and no client to re-apply it
    on the next page load. The server already stamps every page, so it can stamp
    this too -- which also means the theme is correct in the very first painted
    frame, with none of the flash a script-applied theme has to work around.

    Anything that is not a known theme clears the cookie. That makes "Auto" the
    do-nothing value and means a hand-edited form cannot store a junk theme that
    matches no stylesheet block.
    """
    # Only ever redirect somewhere on this site: `next` arrives from a form
    # field, and a bare path cannot be pointed at another origin.
    target = auth.local_redirect(next)
    response = RedirectResponse(target, status_code=303)
    if theme in THEMES:
        response.set_cookie(
            THEME_COOKIE, theme, samesite="lax", max_age=60 * 60 * 24 * 365
        )
    else:
        response.delete_cookie(THEME_COOKIE)
    return response


@web.post("/logout")
def logout() -> Any:
    response = RedirectResponse("/login", status_code=303)
    response.delete_cookie(auth.COOKIE_NAME)
    return response


def mount(app: Any) -> None:
    app.mount("/static", StaticFiles(directory=str(HERE / "static")), name="static")
    app.include_router(web)
