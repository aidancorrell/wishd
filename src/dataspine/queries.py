"""Read-side queries.

Phase 00 has no UI, so these are the interface: the API and the CLI both call
straight into here.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

import psycopg

from .ingest import MAX_TREE_DEPTH


def get_run(conn: psycopg.Connection, run_id: UUID) -> dict[str, Any] | None:
    return conn.execute(
        "select * from run_summary where run_id = %s", (run_id,)
    ).fetchone()


def list_runs(
    conn: psycopg.Connection,
    *,
    integration: str | None = None,
    state: str | None = None,
    job_name: str | None = None,
    roots_only: bool = False,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict[str, Any]]:
    where = ["1 = 1"]
    params: dict[str, Any] = {"limit": limit, "offset": offset}
    if integration:
        where.append("integration = %(integration)s")
        params["integration"] = integration.upper()
    if state:
        where.append("state = %(state)s")
        params["state"] = state.upper()
    if job_name:
        where.append("job_name ilike %(job_name)s")
        params["job_name"] = f"%{job_name}%"
    if roots_only:
        where.append("parent_run_id is null")
    if since:
        where.append("started_at >= %(since)s")
        params["since"] = since
    if until:
        where.append("started_at < %(until)s")
        params["until"] = until
    return conn.execute(
        f"""
        select * from run_summary
        where {' and '.join(where)}
        order by started_at desc nulls last, run_id
        limit %(limit)s offset %(offset)s
        """,
        params,
    ).fetchall()


def job_history(conn: psycopg.Connection, job_id: int, *, limit: int = 50) -> list[dict[str, Any]]:
    """Recent runs of one job, newest first, with output volume attached.

    Duration and row count side by side is what makes "slower *and* fewer rows"
    distinguishable from "slower because more rows" -- the first is a problem,
    the second is Tuesday.
    """
    return conn.execute(
        """
        select s.*,
               (select max(rd.row_count)
                  from run_datasets rd
                 where rd.run_id = s.run_id and rd.direction = 'OUTPUT') as output_rows
        from run_summary s
        where s.job_id = %(job_id)s
        order by s.started_at desc nulls last
        limit %(limit)s
        """,
        {"job_id": job_id, "limit": limit},
    ).fetchall()


def get_job(conn: psycopg.Connection, job_id: int) -> dict[str, Any] | None:
    return conn.execute("select * from jobs where id = %s", (job_id,)).fetchone()


def list_jobs(conn: psycopg.Connection, *, limit: int = 200) -> list[dict[str, Any]]:
    """Jobs with their latest run state — the closest thing to a catalog we have
    before Phase 04 builds a real one."""
    return conn.execute(
        """
        select j.*,
               count(r.run_id)                                       as run_count,
               max(r.started_at)                                     as last_run_at,
               count(*) filter (where r.state = 'FAILED')            as failed_count,
               (array_agg(r.state order by r.started_at desc nulls last))[1] as last_state
        from jobs j
        left join runs r on r.job_id = j.id and r.is_placeholder = false
        group by j.id
        order by max(r.started_at) desc nulls last
        limit %(limit)s
        """,
        {"limit": limit},
    ).fetchall()


_TREE = """
with recursive tree as (
    select r.run_id, r.parent_run_id, 0 as level
    from runs r
    where r.run_id = %(run_id)s
  union all
    select c.run_id, c.parent_run_id, t.level + 1
    from runs c
    join tree t on c.parent_run_id = t.run_id
    where t.level < %(max_depth)s
)
select s.*, t.level
from tree t
join run_summary s on s.run_id = t.run_id
order by t.level, s.started_at nulls last, s.job_name
"""


def run_tree(conn: psycopg.Connection, run_id: UUID) -> list[dict[str, Any]]:
    """Every descendant of `run_id`, breadth-first, with a nesting level.

    Pass the root run id and you get the whole story of one pipeline execution:
    DAG -> task -> dbt invocation -> model -> Spark application -> SQL query.
    """
    return conn.execute(_TREE, {"run_id": run_id, "max_depth": MAX_TREE_DEPTH}).fetchall()


class RunIdError(ValueError):
    """A run id string that does not resolve to exactly one run.

    Carries the candidates when the problem is ambiguity, so the caller can show
    them instead of asking the user to guess a longer prefix.
    """

    def __init__(self, message: str, candidates: list[UUID] | None = None) -> None:
        super().__init__(message)
        self.candidates = candidates or []


def resolve_run_id(conn: psycopg.Connection, value: str) -> UUID:
    """Resolve a full run id, or any unambiguous prefix of one, to a run id.

    Prefix lookup exists because the run listings truncate: nobody is going to
    retype 36 characters from a terminal, and the whole point of `tree` is that
    you reach for it with an id you just read off a screen.

    UUIDv7 makes the prefix length load-bearing in a way v4 would not. The first
    48 bits are a millisecond timestamp, so runs that started close together
    share a long leading run of hex — including the Airflow, dbt and Spark runs
    of one pipeline, which is exactly the set someone is choosing between. A
    short prefix is therefore ambiguous by construction rather than by bad luck,
    which is why an ambiguous prefix lists the candidates rather than just
    telling the user to type more.
    """
    raw = value.strip().replace("-", "").lower()
    if not raw:
        raise RunIdError("no run id given")
    if not all(c in "0123456789abcdef" for c in raw):
        raise RunIdError(f"{value!r} is not a run id or a prefix of one")
    if len(raw) > 32:
        raise RunIdError(f"{value!r} is too long to be a run id")
    if len(raw) == 32:
        return UUID(raw)

    # Compare on the dashed text form, because that is what the user copied off
    # the screen -- a prefix that stops mid-group has already had its dashes
    # stripped above, so both `019fe472` and `019fe472-1e71-7c06` reach here as
    # bare hex and are matched against the same normalised column.
    rows = conn.execute(
        """
        select run_id from runs
        where replace(run_id::text, '-', '') like %s
        order by started_at desc nulls last
        limit 11
        """,
        (raw + "%",),
    ).fetchall()
    if not rows:
        raise RunIdError(f"no run matches {value!r}")
    if len(rows) > 1:
        raise RunIdError(
            f"{value!r} is ambiguous — {len(rows)} runs match" if len(rows) <= 10
            else f"{value!r} is ambiguous — more than 10 runs match",
            [r["run_id"] for r in rows[:10]],
        )
    return rows[0]["run_id"]


def root_of(conn: psycopg.Connection, run_id: UUID) -> UUID | None:
    row = conn.execute("select root_run_id from runs where run_id = %s", (run_id,)).fetchone()
    return row["root_run_id"] if row else None


def run_facets(conn: psycopg.Connection, run_id: UUID) -> dict[str, Any]:
    """Raw facets for the detail page, plus the SQL text pulled out of the job.

    The SQL is the single most useful thing on a failed run's page — it is what
    you would otherwise go find in the dbt project by hand — so it gets lifted
    out of the facet blob rather than left for someone to expand.
    """
    row = conn.execute(
        """
        select r.facets as run_facets, j.facets as job_facets
        from runs r join jobs j on j.id = r.job_id
        where r.run_id = %s
        """,
        (run_id,),
    ).fetchone()
    if not row:
        return {}
    job_facets = row["job_facets"] or {}
    sql_facet = job_facets.get("sql")
    return {
        "run_facets": row["run_facets"] or {},
        "job_facets": job_facets,
        "sql": sql_facet.get("query") if isinstance(sql_facet, dict) else None,
    }


def inherited_sql(conn: psycopg.Connection, run_id: UUID) -> dict[str, Any] | None:
    """Find the nearest ancestor carrying an SQL facet.

    A Spark SQL execution often does not carry the query text, while the dbt
    model that spawned it does. Walking up to find it is the correlation payoff
    in miniature -- but the answer must be labelled with where it came from,
    because showing a parent's SQL as if it were this run's would be a lie.
    """
    row = conn.execute(
        f"""
        with recursive up as (
            select run_id, parent_run_id, job_id, 0 as hops
            from runs where run_id = %(run_id)s
          union all
            select p.run_id, p.parent_run_id, p.job_id, u.hops + 1
            from runs p join up u on p.run_id = u.parent_run_id
            where u.hops < {MAX_TREE_DEPTH}
        )
        select u.hops, j.name as job_name, j.facets #>> '{{sql,query}}' as query
        from up u
        join jobs j on j.id = u.job_id
        where j.facets #>> '{{sql,query}}' is not null
        order by u.hops
        limit 1
        """,
        {"run_id": run_id},
    ).fetchone()
    if not row:
        return None
    return {"query": row["query"], "job_name": row["job_name"], "hops": row["hops"]}


def run_datasets(conn: psycopg.Connection, run_id: UUID) -> list[dict[str, Any]]:
    return conn.execute(
        """
        select d.namespace, d.name, rd.direction, rd.row_count, rd.size_bytes
        from run_datasets rd
        join datasets d on d.id = rd.dataset_id
        where rd.run_id = %s
        order by rd.direction desc, d.name
        """,
        (run_id,),
    ).fetchall()


def tree_datasets(conn: psycopg.Connection, root_run_id: UUID) -> list[dict[str, Any]]:
    """Datasets touched anywhere in a run tree.

    The Airflow task does not know it wrote `fct_orders` -- the Spark job three
    levels down does. Rolling I/O up the tree is how a failed DAG run becomes an
    answer to "which tables are now stale".

    Row counts are aggregated with max(), NOT sum(). One physical write is
    routinely reported at several levels of the tree: the Spark SQL execution
    emits it, and the dbt model that wrapped it emits the same numbers again on
    COMPLETE. Summing those turns a 1.6M-row table into a 3.2M-row table, which
    would then be fed to Phase 03's volume monitors as a fake doubling. When a
    dataset really is written by several distinct runs (partitioned loads),
    max() under-reports rather than inventing rows -- the safer direction to be
    wrong in, and `reported_by` shows when it applies.
    """
    return conn.execute(
        f"""
        with recursive tree as (
            select run_id, 0 as level from runs where run_id = %(run_id)s
          union all
            select c.run_id, t.level + 1
            from runs c join tree t on c.parent_run_id = t.run_id
            where t.level < {MAX_TREE_DEPTH}
        )
        select d.namespace, d.name, rd.direction,
               max(rd.row_count)  as row_count,
               max(rd.size_bytes) as size_bytes,
               count(*)           as reported_by
        from tree t
        join run_datasets rd on rd.run_id = t.run_id
        join datasets d on d.id = rd.dataset_id
        group by d.namespace, d.name, rd.direction
        order by rd.direction desc, d.name
        """,
        {"run_id": root_run_id},
    ).fetchall()


def ingest_health(conn: psycopg.Connection) -> dict[str, Any]:
    """The numbers that tell you whether correlation is actually working.

    `unstitched_runs` is the one to watch. If it climbs, some producer is
    emitting a parent facet pointing at a run nobody ever sent -- which means a
    silently broken link in the chain, the exact failure this project exists to
    make visible.
    """
    row = conn.execute(
        """
        select
          (select count(*) from events)                          as events,
          (select count(*) from jobs)                            as jobs,
          (select count(*) from runs)                            as runs,
          (select count(*) from runs where is_placeholder)       as placeholder_runs,
          (select count(*) from runs where parent_run_id is null) as root_runs,
          (select count(*) from unstitched_runs)                 as unstitched_runs,
          (select count(*) from datasets)                        as datasets,
          (select max(received_at) from events)                  as last_event_at
        """
    ).fetchone()
    by_integration = conn.execute(
        """
        select coalesce(integration, '(unknown)') as integration,
               count(*) as runs
        from run_summary
        group by 1 order by 2 desc
        """
    ).fetchall()
    row["runs_by_integration"] = {r["integration"]: r["runs"] for r in by_integration}
    return row


# ------------------------------------------------------------------ pipelines


def trees_for(
    conn: psycopg.Connection, root_ids: list[Any]
) -> dict[Any, list[dict[str, Any]]]:
    """Every descendant of each root, flattened to display order with a depth.

    **One query for the whole set, not one per root.** Fetching a tree per card
    would issue N+1 queries on a page that shows twenty of them; instead every
    descendant of the set is fetched at once and assembled in Python. The second
    query is a single index range scan on `root_run_id`.

    Flat-with-depth rather than nested: the template renders one row per run with
    an indent, and a recursive Jinja macro over a nested structure is markedly
    harder to read for the same output.
    """
    if not root_ids:
        return {}

    children = conn.execute(
        """
        select * from run_summary
        where root_run_id = any(%(roots)s)
          and parent_run_id is not null
        order by depth, started_at nulls last, job_name
        """,
        {"roots": list(root_ids)},
    ).fetchall()

    by_parent: dict[Any, list[dict[str, Any]]] = {}
    for child in children:
        by_parent.setdefault(child["parent_run_id"], []).append(child)

    def descend(run_id: Any, depth: int, seen: set[Any]) -> list[dict[str, Any]]:
        """`seen` guards a cycle -- the correlator forbids them, but a list page
        must not be the thing that hangs if one ever exists."""
        rows = []
        for child in by_parent.get(run_id, []):
            if child["run_id"] in seen:
                continue
            seen.add(child["run_id"])
            rows.append({**child, "depth_display": depth})
            rows.extend(descend(child["run_id"], depth + 1, seen))
        return rows

    return {root_id: descend(root_id, 1, {root_id}) for root_id in root_ids}


def list_pipelines(
    conn: psycopg.Connection,
    *,
    integration: str | None = None,
    state: str | None = None,
    job_name: str | None = None,
    since: datetime | None = None,
    limit: int = 20,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """Recent pipeline *executions*, each with its whole run tree attached.

    The run list used to be a flat table of runs, which reads well only if you
    already know which twenty rows belong together. A pipeline execution is the
    unit people actually think in -- "last night's analytics_daily" -- so this
    returns roots, each carrying its descendants in tree order.

    **Two queries, not N+1.** Fetching the tree per root would issue one query
    per card; instead the roots are selected, then every descendant of that set
    is fetched at once and assembled in Python. At 20 cards that is 2 queries
    rather than 21, and the second is a single index range scan on root_run_id.

    **Filters select whole pipelines, matching anywhere in the tree.** Applying
    them to the root alone looks tidier and is wrong in practice: `integration =
    SPARK` would return nothing at all, because a Spark run is never the root --
    it is always something an Airflow DAG caused. What someone means by that
    filter is "pipelines with Spark work in them", so a pipeline is kept when the
    root *or any descendant* matches, and the card is then shown whole. Hiding
    the non-matching steps instead would break the hierarchy the page exists to
    draw.

    `since` is the exception and stays on the root, because it is asking when the
    *execution* happened, not when some leaf of it did.
    """
    where = ["r.parent_run_id is null"]
    params: dict[str, Any] = {"limit": limit, "offset": offset}
    if since:
        where.append("r.started_at >= %(since)s")
        params["since"] = since

    member = []
    if integration:
        member.append("d.integration = %(integration)s")
        params["integration"] = integration.upper()
    if state:
        member.append("d.state = %(state)s")
        params["state"] = state.upper()
    if job_name:
        member.append("d.job_name ilike %(job_name)s")
        params["job_name"] = f"%{job_name}%"
    if member:
        where.append(
            f"""exists (
                select 1 from run_summary d
                where (d.run_id = r.run_id or d.root_run_id = r.run_id)
                  and {' and '.join(member)}
            )"""
        )

    roots = conn.execute(
        f"""
        select r.* from run_summary r
        where {' and '.join(where)}
        order by r.started_at desc nulls first, r.run_id
        limit %(limit)s offset %(offset)s
        """,
        params,
    ).fetchall()
    if not roots:
        return []

    trees = trees_for(conn, [r["run_id"] for r in roots])

    pipelines = []
    for root in roots:
        tree = trees.get(root["run_id"], [])
        states = [root["state"]] + [r["state"] for r in tree]
        pipelines.append(
            {
                "root": {**root, "depth_display": 0},
                "runs": tree,
                "run_count": len(tree) + 1,
                # A pipeline is failed if anything in it failed, even when the
                # root reports COMPLETED -- which happens when a producer only
                # ever sends its own terminal state. Surfacing the root's state
                # alone would show a green pipeline containing a red task.
                "has_failure": any(s == "FAILED" for s in states),
                "any_running": any(s == "RUNNING" for s in states),
            }
        )
    return pipelines


# ------------------------------------------------------------------- overview
#
# The run list answers "what happened". These answer "what is happening" and
# "what is happening *at the same time*", which is a different question and the
# one that gets asked at 02:00. Both are deliberately separate from
# `list_pipelines`: that function pages through history and carries whole trees,
# and neither property is wanted here.


def live_pipelines(
    conn: psycopg.Connection, *, limit: int = 50, root_ids: list[Any] | None = None
) -> list[dict[str, Any]]:
    """Pipeline executions with work still in flight, newest first.

    "In flight" is *any* run in the tree still RUNNING -- not the root's own
    state. A producer that reports its own terminal state without waiting for
    its children (dbt does this, and so does Spark) leaves a COMPLETED root above
    live work, and a board that trusted the root would go blank exactly when the
    cluster is busiest.

    Progress is steps finished over steps known, which is honest but not a
    percentage of *remaining* work: OpenLineage has no "about to run" event, so a
    tree grows as it executes. A pipeline can sit at 8/8 and then become 8/11.
    The template says `8 of 11 so far` rather than implying a forecast.

    `root_ids` adds named executions whatever their state. The Slack live feed
    needs it: an execution drops out of "in flight" the moment it finishes, which
    is the exact moment its message still has to be updated one last time to say
    how it ended. Without this the feed would leave every pipeline frozen at its
    second-to-last step.
    """
    rows = conn.execute(
        """
        with live as (
            (select distinct coalesce(root_run_id, run_id) as root_id
               from run_summary
              where state = 'RUNNING'
              limit %(limit)s)
            union
            select unnest(%(root_ids)s::uuid[])
        )
        select
            r.run_id, r.job_name, r.job_namespace, r.integration, r.state,
            r.started_at, r.duration_ms,
            -- Not in `run_summary`, which is deliberately narrow: the facets are
            -- wanted here only so the Slack feed can offer "open in dbt Cloud"
            -- beside its own link, and widening the view would pay for that on
            -- every read of the busiest query in the system.
            (select facets from runs where runs.run_id = r.run_id) as facets,
            (select count(*) from run_summary d
              where d.root_run_id = r.run_id or d.run_id = r.run_id) as total,
            (select count(*) from run_summary d
              where (d.root_run_id = r.run_id or d.run_id = r.run_id)
                and d.state in ('COMPLETED', 'FAILED', 'ABORTED')) as done,
            (select count(*) from run_summary d
              where (d.root_run_id = r.run_id or d.run_id = r.run_id)
                and d.state = 'RUNNING') as running,
            (select count(*) from run_summary d
              where (d.root_run_id = r.run_id or d.run_id = r.run_id)
                and d.state = 'FAILED') as failed,
            (select string_agg(distinct d.integration, ',' order by d.integration)
              from run_summary d
              where (d.root_run_id = r.run_id or d.run_id = r.run_id)
                and d.integration is not null) as integrations,
            (select string_agg(distinct d.job_namespace, '|')
              from run_summary d
              where (d.root_run_id = r.run_id or d.run_id = r.run_id)
                and d.integration = 'SPARK') as spark_namespaces,
            -- The newest event anywhere in the tree. A RUNNING run that has gone
            -- quiet is the case this project exists for: OpenLineage has no
            -- heartbeat, so a cluster that dies mid-run leaves its runs RUNNING
            -- forever and every duration computed from `now` grows without
            -- bound. Silence is the only evidence available, so surface it
            -- rather than presenting a two-week-old run as live work.
            (select max(d.last_event_at) from run_summary d
              where d.root_run_id = r.run_id or d.run_id = r.run_id) as last_event_at
        from run_summary r
        join live on live.root_id = r.run_id
        order by r.started_at desc nulls last
        """,
        {"limit": limit, "root_ids": [str(r) for r in (root_ids or [])]},
    ).fetchall()
    return [dict(r) for r in rows]


def job_baselines(
    conn: psycopg.Connection, job_names: list[str], *, samples: int = 20
) -> dict[str, float]:
    """Median completed duration per job name, for "is this one slow?".

    Median rather than mean because pipeline durations are long-tailed -- one
    stuck run at 40× drags a mean far enough that nothing ever looks slow again.
    Only COMPLETED runs count: a failed run's duration measures how long it took
    to give up, which is not a baseline for anything.

    Returns milliseconds, and omits any job without enough history rather than
    reporting a baseline from two samples. A comparison nobody can trust is worse
    than no comparison, because it gets ignored and then ignored when it matters.
    """
    if not job_names:
        return {}
    rows = conn.execute(
        """
        select job_name,
               percentile_cont(0.5) within group (order by duration_ms) as p50,
               count(*) as n
        from (
            select job_name, duration_ms,
                   row_number() over (partition by job_name order by started_at desc) as rn
            from run_summary
            where job_name = any(%(names)s)
              and state = 'COMPLETED'
              and duration_ms is not null
        ) recent
        where rn <= %(samples)s
        group by job_name
        having count(*) >= 3
        """,
        {"names": job_names, "samples": samples},
    ).fetchall()
    return {r["job_name"]: float(r["p50"]) for r in rows if r["p50"] is not None}


def concurrency_timeline(
    conn: psycopg.Connection, *, since: datetime, until: datetime | None = None
) -> list[dict[str, Any]]:
    """Root executions overlapping a window, for the timeline lanes.

    Selects on *overlap*, not on start time: a pipeline that began before the
    window and is still running is the single most interesting thing on this
    page, and a naive `started_at >= since` would drop exactly that row. A run
    with no `ended_at` is treated as still open, which is what RUNNING means.
    """
    rows = conn.execute(
        """
        select run_id, job_name, integration, state, started_at, ended_at, duration_ms
        from run_summary
        where parent_run_id is null
          and started_at is not null
          and (ended_at is null or ended_at >= %(since)s)
          and (%(until)s::timestamptz is null or started_at <= %(until)s)
        order by job_name, started_at
        """,
        {"since": since, "until": until},
    ).fetchall()
    return [dict(r) for r in rows]


def peak_concurrency(executions: list[dict[str, Any]], *, now: datetime) -> dict[str, Any]:
    """The busiest moment in the window, by a sweep over interval endpoints.

    Computed in Python rather than SQL on purpose: it is a handful of rows
    already in memory, and the alternative -- a self-join or a window function
    over generated time buckets -- is both slower and much harder to read for a
    number this small. Bucketing would also quantise the answer, and the useful
    form of "4 at once" is the exact instant, not "some time in that ten-minute
    bucket".
    """
    events: list[tuple[datetime, int]] = []
    for run in executions:
        start = run["started_at"]
        end = run["ended_at"] or now
        if start is None or end < start:
            continue
        events.append((start, 1))
        events.append((end, -1))
    if not events:
        return {"peak": 0, "at": None}

    # End before start at the same instant: a run ending exactly as another
    # begins is a handover, not a moment of two-way concurrency.
    events.sort(key=lambda e: (e[0], e[1]))
    current = peak = 0
    at = None
    for moment, delta in events:
        current += delta
        if current > peak:
            peak, at = current, moment
    return {"peak": peak, "at": at}
