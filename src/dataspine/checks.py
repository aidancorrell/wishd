"""Evaluating monitors against the spine.

Every metric in this module is computed from `runs`, `run_datasets` and
`datasets` -- tables the gateway already fills. **No monitor here needs warehouse
credentials, a scan, or any collection we do not already do.** That is not a
temporary limitation on the way to warehouse pollers; it is the reason this slice
ships first. A team that has not yet given us a Snowflake role can still find out
that last night's model wrote a tenth of its usual rows.

The module has exactly two halves, and keeping them apart is the design:

    collect(conn, monitor, since)   observations -- what happened, and when
    judge(monitor, points, now)     decisions    -- was that acceptable

`collect` is a pure function of the archive: run it over the last hour to pick up
new points, or over the last 90 days to build a baseline out of history that
predates the monitor. Same code, and that is what lets a monitor arm the moment it
is created rather than a week later. `judge` never touches the database, so a
threshold can be re-decided against stored history without re-collecting anything.

Three rules, inherited from `heuristics.py` because they were right there:
silence is the default, every finding carries the numbers that triggered it, and
thresholds are the user's opinion rather than ours.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg

from . import timing
from .ingest import MAX_TREE_DEPTH

# States a run must be in before its numbers mean anything. A FAILED run's row
# count is whatever it managed to write before dying, and feeding that to a volume
# monitor produces an alert about the failure you already knew about.
SUCCESS_STATES = ("COMPLETED",)


@dataclass
class Point:
    """One observation. `observed_at` is when the measured thing happened."""

    observed_at: datetime
    subject: str
    value: float | None
    context: dict[str, Any] = field(default_factory=dict)


@dataclass
class Result:
    status: str                     # ok | breach | insufficient_data | error
    message: str
    value: float | None = None
    threshold: dict[str, Any] = field(default_factory=dict)
    subject: str | None = None
    context: dict[str, Any] = field(default_factory=dict)


# ------------------------------------------------------------ target resolution


def _leaf(name: str) -> str:
    """The last segment of a dataset name, splitting on `/` or `.`."""
    return name.replace("\\", "/").rstrip("/").rsplit("/", 1)[-1].rsplit(".", 1)[-1]


def resolve_datasets(
    conn: psycopg.Connection, target: str, *, namespace: str | None = None
) -> list[dict[str, Any]]:
    """Every stored dataset row that is the logical table the user named.

    This is not over-engineering, it is a fact about real captures. One pipeline
    writing one table produces **two** dataset rows with nothing in common but the
    final path segment:

        dbt    postgres://postgres:5432   dataspine.analytics_marts.stg_orders
        Spark  file                       /tmp/warehouse/stg_orders

    On real EMR the pair becomes `hive://` and `s3://` (ADR-003 predicted the
    namespaces would differ across adapters; the captures show they differ within
    a single run tree). And the two rows carry different information -- dbt has no
    `outputStatistics` and no `schema` facet at all, Spark has both -- so a monitor
    resolving to only one of them silently loses either freshness or volume.

    Matching on the leaf segment is deliberately loose, and the looseness is made
    safe by never hiding it: the resolution is recorded in every result's context,
    and `dataspine monitors` prints it. `namespace:` pins the match when a name
    really is ambiguous across environments.

    A sequential scan is fine here. `datasets` is the catalog -- thousands of rows
    where `events` has millions -- and an expression index on the leaf would have
    to guess the same split rule this function makes explicit.
    """
    rows = conn.execute(
        "select id, namespace, name from datasets"
        + (" where namespace = %(ns)s" if namespace else ""),
        {"ns": namespace} if namespace else {},
    ).fetchall()
    leaf = _leaf(target)
    return [r for r in rows if r["name"] == target or _leaf(r["name"]) == leaf]


def resolve_jobs(conn: psycopg.Connection, target: str) -> list[dict[str, Any]]:
    """Jobs matching a target name. Exact match wins; otherwise substring.

    Exact-first matters because job names nest: `dbt-run-analytics` is a prefix of
    `dbt-run-analytics_marts`, and a monitor on the first must not quietly start
    watching the second.
    """
    exact = conn.execute(
        "select id, namespace, name from jobs where name = %s", (target,)
    ).fetchall()
    if exact:
        return exact
    return conn.execute(
        "select id, namespace, name from jobs where name ilike %s order by name",
        (f"%{target}%",),
    ).fetchall()


# ----------------------------------------------------------------- collection


def collect(
    conn: psycopg.Connection, monitor: dict[str, Any], *, since: datetime | None = None
) -> list[Point]:
    """Observations for one monitor. Idempotent, and safe to run over any window."""
    kind = monitor["kind"]
    config = monitor["config"] or {}
    namespace = config.get("namespace")

    if kind in ("freshness", "row_count", "schema_drift", "column_stats"):
        datasets = resolve_datasets(conn, monitor["target"], namespace=namespace)
        if not datasets:
            return []
        if kind == "column_stats":
            return _column_points(conn, [d["id"] for d in datasets], config, since=since)
        writes = _dataset_writes(conn, [d["id"] for d in datasets], since=since)
        if kind == "freshness":
            return _freshness_points(writes)
        if kind == "row_count":
            return _row_count_points(writes)
        return _schema_points(writes)

    if kind == "custom_sql":
        return _custom_sql_points(conn, monitor)

    jobs = resolve_jobs(conn, monitor["target"])
    if not jobs:
        return []
    if kind == "spark_spill":
        return _spill_points(conn, [j["id"] for j in jobs], since=since)
    if kind == "cost_per_run":
        return _cost_points(conn, [j["id"] for j in jobs], since=since)

    runs = _job_runs(conn, [j["id"] for j in jobs], since=since)
    if kind == "job_duration":
        return _duration_points(runs)
    if kind == "job_failure_rate":
        return _failure_points(runs)
    if kind == "queue_delay":
        return _queue_delay_points(runs)
    if kind == "job_retries":
        return _retry_points(runs)
    return []


def _dataset_writes(
    conn: psycopg.Connection, dataset_ids: list[int], *, since: datetime | None
) -> list[dict[str, Any]]:
    """One row per *logical* write of the table, oldest first.

    Grouped by run tree, not by run, and that is the whole trick. A single nightly
    build of `fct_orders` produces several write edges: the dbt model run reports
    one against `postgres://.../fct_orders`, the Spark SQL execution underneath it
    reports another against `file:/tmp/warehouse/fct_orders`, seconds apart. Keyed
    by run, those are two writes -- which would halve every inter-arrival gap and
    teach a freshness baseline that this table updates twice a minute.

    Keyed by `root_run_id` they are one write, because the correlator already
    proved they belong to one pipeline execution. This is the spine paying for
    itself: telling "two reports of one write" from "two writes" is not something
    you can do from either producer's events alone.

    Row counts aggregate with max(), never sum(), for the reason `tree_datasets`
    documents: the same physical write is reported at several levels of the tree,
    and summing turns a 1.6M-row table into a 3.2M-row one -- a fake doubling fed
    straight to a volume monitor.

    Writes observed by a *poller* (migration 011) are unioned in with the same
    columns. A table can legitimately be both -- written by our dbt project and
    polled from the warehouse catalog -- and a monitor must not care which it is
    looking at. Catalog metadata is coarser (approximate row counts, a
    modification time that may include DDL), so where the two describe the same
    moment the run-derived row wins; it came from the engine that did the writing.
    """
    return conn.execute(
        """
        with run_writes as (
            select coalesce(r.root_run_id, r.run_id)::text      as subject,
                   max(coalesce(r.ended_at, r.started_at))      as written_at,
                   max(rd.row_count)                            as row_count,
                   max(rd.size_bytes)                           as size_bytes,
                   -- Read from the write edge, never from `datasets.facets`. The
                   -- latter is a running merge holding the CURRENT schema, so
                   -- comparing two historical writes through it would compare
                   -- today's columns against themselves and never see drift. Only
                   -- the Spark integration sends a schema facet at all; on a
                   -- dbt-only stack this is legitimately null, which reads as
                   -- insufficient_data rather than as a passing check.
                   (array_agg(rd.facets -> 'schema' order by rd.updated_at desc)
                    filter (where rd.facets ? 'schema'))[1]     as schema,
                   count(*)                                     as reported_by,
                   0                                            as from_poller
            from run_datasets rd
            join runs r     on r.run_id = rd.run_id
            where rd.dataset_id = any(%(ids)s)
              and rd.direction = 'OUTPUT'
              and r.state = any(%(states)s)
              and coalesce(r.ended_at, r.started_at) is not null
            group by coalesce(r.root_run_id, r.run_id)
        ),
        polled as (
            select s.source || '@' || s.observed_at::text       as subject,
                   s.observed_at                                as written_at,
                   max(s.row_count)                             as row_count,
                   max(s.size_bytes)                            as size_bytes,
                   -- Wrapped to match the SchemaDatasetFacet shape the run path
                   -- produces, so `_columns` parses one form rather than two.
                   (array_agg(
                        jsonb_build_object(
                            'fields',
                            (select jsonb_agg(jsonb_build_object('name', k, 'type', v))
                               from jsonb_each_text(s.columns) as e(k, v))
                        ) order by s.recorded_at desc
                    ) filter (where s.columns is not null))[1]  as schema,
                   count(*)                                     as reported_by,
                   1                                            as from_poller
            from dataset_snapshots s
            where s.dataset_id = any(%(ids)s)
            group by s.source, s.observed_at
        ),
        combined as (
            select * from run_writes
            union all
            select * from polled
        )
        select distinct on (written_at)
               subject as root_run_id, written_at, row_count, size_bytes,
               schema, reported_by
        from combined
        where (%(since)s::timestamptz is null or written_at >= %(since)s)
        order by written_at, from_poller
        """,
        {"ids": dataset_ids, "states": list(SUCCESS_STATES), "since": since},
    ).fetchall()


def _job_runs(
    conn: psycopg.Connection, job_ids: list[int], *, since: datetime | None
) -> list[dict[str, Any]]:
    """Completed runs of the target job(s), oldest first.

    Placeholders are excluded: a placeholder is a run we know exists only because
    a child mentioned it, so its duration is null and its state is a guess.
    """
    return conn.execute(
        """
        select r.run_id, r.state, r.started_at, r.ended_at, r.facets,
               extract(epoch from (r.ended_at - r.started_at)) * 1000 as duration_ms
        from runs r
        where r.job_id = any(%(ids)s)
          and not r.is_placeholder
          and r.started_at is not null
          and (%(since)s::timestamptz is null or r.started_at >= %(since)s)
        order by r.started_at
        """,
        {"ids": job_ids, "since": since},
    ).fetchall()


def _freshness_points(writes: list[dict[str, Any]]) -> list[Point]:
    """Inter-arrival time: minutes since the previous write of this table.

    Not "how stale is it right now" -- that number depends on when we looked, and
    storing it would make the history a record of our polling interval rather than
    of the pipeline. Current staleness is computed at judgement time from the
    newest point's timestamp. What gets *stored* is how long the gap was, which is
    the thing a baseline can be built from: "this table normally updates every 60
    minutes" is learnable, "it was 43 minutes old when we asked" is not.

    The first ever write has no predecessor and therefore no interval. It is still
    recorded, with a null value, because its timestamp is what every freshness
    judgement is measured from.
    """
    points: list[Point] = []
    previous: datetime | None = None
    for write in writes:
        written_at = write["written_at"]
        gap = (written_at - previous).total_seconds() / 60 if previous else None
        points.append(
            Point(
                observed_at=written_at,
                subject=str(write["root_run_id"]),
                value=gap,
                context={"since_previous_minutes": gap},
            )
        )
        previous = written_at
    return points


def _row_count_points(writes: list[dict[str, Any]]) -> list[Point]:
    return [
        Point(
            observed_at=w["written_at"],
            subject=str(w["root_run_id"]),
            value=float(w["row_count"]),
            context={"size_bytes": w["size_bytes"], "reported_by": w["reported_by"]},
        )
        for w in writes
        if w["row_count"] is not None
    ]


def _schema_points(writes: list[dict[str, Any]]) -> list[Point]:
    """The column list at each write, as `{name: type}`.

    The value is the column count so the history plots like every other metric;
    the actual comparison is over the context, because "12 columns" and "12
    columns, one of which changed type" are the same number and very different
    events.
    """
    points: list[Point] = []
    for write in writes:
        columns = _columns(write["schema"])
        if columns is None:
            continue
        points.append(
            Point(
                observed_at=write["written_at"],
                subject=str(write["root_run_id"]),
                value=float(len(columns)),
                context={"columns": columns},
            )
        )
    return points


def _columns(schema_facet: Any) -> dict[str, str] | None:
    """`{column: type}` from a SchemaDatasetFacet, or None if there is not one.

    Field shape verified against the real openlineage-spark 1.52.0 capture:
    `{"fields": [{"name": "order_id", "type": "integer"}, ...]}`.
    """
    if not isinstance(schema_facet, dict):
        return None
    fields = schema_facet.get("fields")
    if not isinstance(fields, list):
        return None
    columns: dict[str, str] = {}
    for entry in fields:
        if isinstance(entry, dict) and isinstance(entry.get("name"), str):
            columns[entry["name"]] = str(entry.get("type", "?"))
    return columns or None


def _duration_points(runs: list[dict[str, Any]]) -> list[Point]:
    return [
        Point(
            observed_at=r["started_at"],
            subject=str(r["run_id"]),
            value=float(r["duration_ms"]) / 1000,
            context={"state": r["state"]},
        )
        for r in runs
        if r["duration_ms"] is not None and r["state"] in SUCCESS_STATES
    ]


def _failure_points(runs: list[dict[str, Any]]) -> list[Point]:
    """1.0 for a failed run, 0.0 for a successful one.

    RUNNING and UNKNOWN runs are skipped rather than counted as successes: a run
    still in flight has not yet had the chance to fail, and counting it as a
    success would dilute the rate downwards exactly when a job is hanging.
    """
    return [
        Point(
            observed_at=r["started_at"],
            subject=str(r["run_id"]),
            value=1.0 if r["state"] in ("FAILED", "ABORTED") else 0.0,
            context={"state": r["state"]},
        )
        for r in runs
        if r["state"] in ("COMPLETED", "FAILED", "ABORTED")
    ]


def _column_points(
    conn: psycopg.Connection,
    dataset_ids: list[int],
    config: dict[str, Any],
    *,
    since: datetime | None,
) -> list[Point]:
    """One statistic's history, from stored profiles.

    Reads what profiling already wrote rather than scanning on demand. A monitor
    evaluation must stay cheap and predictable -- if checking a null rate could
    trigger a table scan, `dataspine check` would become the most expensive thing
    on the schedule and the hourly cron would be the first casualty.
    """
    from . import profile as profile_mod

    rows = profile_mod.history(
        conn, dataset_ids, config["column"], config["metric"], since=since
    )
    return [
        Point(
            observed_at=row["observed_at"],
            subject=f"{config['column']}@{row['observed_at'].isoformat()}",
            value=float(row["value"]),
            # Sampling basis travels with the number. A rate from a 1% sample and
            # one from a full table are different claims.
            context={"sampled": row["sampled"], "scanned_rows": row["scanned_rows"]},
        )
        for row in rows
        if row["value"] is not None
    ]


def _cost_points(
    conn: psycopg.Connection, job_ids: list[int], *, since: datetime | None
) -> list[Point]:
    """Attributed cost per run of the target job.

    Descends the run tree for the same reason spill does: the bill attaches to a
    Spark application, and the job a human names in YAML is the dbt model or
    Airflow task above it.

    Runs with no attributed cost produce no point rather than a zero. A cost
    monitor over a period with no imported bill must read as insufficient_data,
    not as a clean budget -- on a dashboard "$0" and "we do not know" look
    identical and mean opposite things.
    """
    rows = conn.execute(
        f"""
        with recursive roots as (
            select run_id, run_id as top, started_at, 0 as level
            from runs
            where job_id = any(%(ids)s) and not is_placeholder and started_at is not null
              and (%(since)s::timestamptz is null or started_at >= %(since)s)
          union all
            select c.run_id, r.top, r.started_at, r.level + 1
            from runs c join roots r on c.parent_run_id = r.run_id
            where r.level < {MAX_TREE_DEPTH}
        )
        select r.top as run_id,
               min(r.started_at) as started_at,
               sum(ac.cost_usd)  as cost_usd
        from roots r
        join spark_apps s        on s.run_id = r.run_id
        join application_costs ac on ac.app_id = s.app_id
        group by r.top
        order by min(r.started_at)
        """,
        {"ids": job_ids, "since": since},
    ).fetchall()
    return [
        Point(
            observed_at=row["started_at"],
            subject=str(row["run_id"]),
            value=float(row["cost_usd"]),
        )
        for row in rows
        if row["cost_usd"] is not None
    ]


def _retry_points(runs: list[dict[str, Any]]) -> list[Point]:
    """Retries per run, from Airflow's attempt number.

    **Read `taskInstance.try_number`, not `task.retries`.** The field named
    `retries` is the configured ceiling and is identical on every run of a task --
    a monitor reading it would report a constant 3 forever and never fire once.
    Verified against the real Airflow 3.0.2 + provider 2.19.0 capture, where both
    fields are present and mean different things.

    A run without the facet is skipped rather than recorded as zero. Spark and
    dbt send no attempt number, and reporting a clean retry record for producers
    we cannot see would be worse than reporting nothing.
    """
    points: list[Point] = []
    for run in runs:
        facets = run["facets"] if isinstance(run["facets"], dict) else {}
        instance = ((facets.get("airflow") or {}).get("taskInstance") or {})
        attempt = instance.get("try_number")
        if not isinstance(attempt, int):
            continue
        points.append(
            Point(
                observed_at=run["started_at"],
                subject=str(run["run_id"]),
                # Attempts minus the first one. "try_number 3" is two retries, and
                # reporting three would overstate every failure by one.
                value=float(max(attempt - 1, 0)),
                context={"try_number": attempt, "state": run["state"]},
            )
        )
    return points


def _spill_points(
    conn: psycopg.Connection, job_ids: list[int], *, since: datetime | None
) -> list[Point]:
    """Bytes spilled per run, from metrics stored since Phase 02.

    Joined through the run tree rather than to the run directly: the event log
    attaches to the Spark application, but the job a human names in YAML is the
    dbt model or the Airflow task above it. Without the descent, a spill monitor
    would only ever work if you targeted the anonymous Spark SQL execution id.
    """
    rows = conn.execute(
        f"""
        with recursive roots as (
            select run_id, run_id as top, started_at, 0 as level
            from runs
            where job_id = any(%(ids)s) and not is_placeholder and started_at is not null
              and (%(since)s::timestamptz is null or started_at >= %(since)s)
          union all
            select c.run_id, r.top, r.started_at, r.level + 1
            from runs c join roots r on c.parent_run_id = r.run_id
            -- Same depth guard as every other tree query. The correlator's cycle
            -- guard runs at ingest, but a cycle that predates it (or arrives by
            -- replay) must not turn a monitor check into an infinite recursion.
            where r.level < {MAX_TREE_DEPTH}
        )
        select r.top as run_id,
               min(r.started_at) as started_at,
               sum(coalesce((s.metrics ->> 'disk_spilled_bytes')::numeric, 0)
                 + coalesce((s.metrics ->> 'memory_spilled_bytes')::numeric, 0)) as spilled
        from roots r
        join spark_apps s on s.run_id = r.run_id
        group by r.top
        order by min(r.started_at)
        """,
        {"ids": job_ids, "since": since},
    ).fetchall()
    return [
        Point(
            observed_at=row["started_at"],
            subject=str(row["run_id"]),
            value=float(row["spilled"] or 0),
        )
        for row in rows
    ]


# How long a user-supplied query may run before it is killed. A monitor is a
# question; one that holds a connection open indefinitely starves the pool the
# ingest gateway shares, turning a slow monitor into an ingest outage.
DEFAULT_QUERY_TIMEOUT_SECONDS = 30


def _custom_sql_points(conn: psycopg.Connection, monitor: dict[str, Any]) -> list[Point]:
    """Run the user's query and take one number from it.

    Two guards, both non-negotiable:

    **Read-only.** The query runs inside a read-only subtransaction, so a mistake
    -- or a malicious PR against the monitors file, which is a plausible attack
    on a repo where anyone can open one -- cannot mutate the data it is watching.
    A monitor is a question, not a migration.

    **Timed out.** `statement_timeout` bounds it, for the pool reason above.

    Both are set inside a savepoint so they revert cleanly and cannot leak
    read-only-ness onto the caller's connection, which would break the very next
    monitor's result write.
    """
    config = monitor["config"] or {}
    timeout = int(float(config.get("timeout_seconds", DEFAULT_QUERY_TIMEOUT_SECONDS)) * 1000)

    with conn.transaction(force_rollback=True):
        conn.execute(f"set local statement_timeout = {timeout}")
        conn.execute("set local transaction read only")
        row = conn.execute(monitor["target"]).fetchone()

    if row is None:
        return []
    value = next(iter(row.values()))
    if isinstance(value, bool) or not isinstance(value, int | float | Decimal):
        raise TypeError(
            f"custom_sql must select a number; got {type(value).__name__} ({value!r})"
        )
    return [
        Point(
            observed_at=datetime.now(UTC),
            # No run produced this, so the observation is keyed by the moment it
            # was taken. Unlike every other kind, re-running the query genuinely
            # is a new observation.
            subject=datetime.now(UTC).isoformat(timespec="seconds"),
            value=float(value),
        )
    ]


def _queue_delay_points(runs: list[dict[str, Any]]) -> list[Point]:
    """Time spent eligible but not started. See `timing.py` for why this is not
    OTel."""
    points: list[Point] = []
    for run in runs:
        delay = timing.queue_delay_seconds(run["facets"])
        if delay is None:
            continue
        points.append(
            Point(observed_at=run["started_at"], subject=str(run["run_id"]), value=delay)
        )
    return points


# ------------------------------------------------------------------- judgement


def judge(monitor: dict[str, Any], points: list[Point], *, now: datetime | None = None) -> Result:
    """Decide a monitor's status from its metric history. Never touches the DB."""
    now = now or datetime.now(UTC)
    config = monitor["config"] or {}
    kind = monitor["kind"]

    if not points:
        return Result(
            status="insufficient_data",
            message=f"no observations yet for `{monitor['target']}`",
        )

    ordered = sorted(points, key=lambda p: p.observed_at)

    if (monitor.get("mode") or "threshold") == "anomaly":
        return _judge_anomaly(monitor, ordered, config, now)

    if kind == "freshness":
        return _judge_freshness(ordered, config, now)
    if kind == "row_count":
        return _judge_bounds(ordered, config, unit="rows")
    if kind == "schema_drift":
        return _judge_schema(ordered, config)
    if kind == "job_duration":
        return _judge_max(ordered, config, "max_seconds", unit="s", label="duration")
    if kind == "queue_delay":
        return _judge_max(ordered, config, "max_seconds", unit="s", label="queue delay")
    if kind == "job_failure_rate":
        return _judge_failure_rate(ordered, config, now)
    if kind == "custom_sql":
        return _judge_bounds(ordered, config, unit="")
    if kind == "job_retries":
        return _judge_bounds(ordered, config, unit="retries")
    if kind == "column_stats":
        return _judge_bounds(ordered, config, unit=f"{config['column']} {config['metric']}")
    if kind == "spark_spill":
        return _judge_spill(ordered, config)
    if kind == "cost_per_run":
        return _judge_bounds(ordered, config, unit="USD")
    return Result(status="error", message=f"unknown monitor kind {kind!r}")


def _judge_spill(points: list[Point], config: dict[str, Any]) -> Result:
    """Spill in bytes, stated in GB.

    Bytes in the history and GB in the threshold is deliberate: nobody writes
    `max_bytes: 4294967296` in a YAML file correctly, and nobody wants a metric
    series quantised to whole gigabytes.
    """
    from .heuristics import format_bytes

    latest = points[-1]
    value = latest.value or 0.0
    limit_bytes = float(config["max_gb"]) * 1024**3
    threshold = {"max_gb": config["max_gb"]}

    if value > limit_bytes:
        return Result(
            status="breach", value=value, threshold=threshold, subject=latest.subject,
            message=(
                f"spilled {format_bytes(value)}, limit is {config['max_gb']}GB. "
                f"Executors ran out of memory and fell back to disk."
            ),
        )
    return Result(
        status="ok", value=value, threshold=threshold, subject=latest.subject,
        message=f"spilled {format_bytes(value)}",
    )


def _judge_anomaly(
    monitor: dict[str, Any], points: list[Point], config: dict[str, Any], now: datetime
) -> Result:
    """Learned bounds instead of a stated one.

    Freshness is the exception and needs saying: its *stored* series is the gap
    between writes, so the detector answers "is this gap unusual for this table",
    not "is it stale right now". That is the more useful question of the two --
    it catches a table that silently stopped updating at its own cadence without
    anyone having to know what that cadence is.
    """
    from . import anomaly

    verdict = anomaly.detect(
        points,
        now=now,
        sensitivity=float(config.get("sensitivity", anomaly.DEFAULT_SENSITIVITY)),
        min_points=int(config.get("min_training_points", anomaly.MIN_TRAINING_POINTS)),
        min_days=float(config.get("min_training_days", anomaly.MIN_TRAINING_DAYS)),
    )
    latest = points[-1]
    label = {
        "freshness": "gap since previous write",
        "row_count": "rows",
        "job_duration": "duration",
        "queue_delay": "queue delay",
    }.get(monitor["kind"], monitor["kind"])

    return Result(
        status=verdict.status,
        message=f"{label}: {verdict.message}",
        value=verdict.value,
        threshold={
            "mode": "anomaly",
            "expected": verdict.expected,
            "band": verdict.band_width,
        },
        subject=latest.subject,
        context=verdict.context,
    )


def _judge_freshness(points: list[Point], config: dict[str, Any], now: datetime) -> Result:
    latest = points[-1]
    limit = float(config["max_age_minutes"])
    age = (now - latest.observed_at).total_seconds() / 60
    threshold = {"max_age_minutes": limit}
    written = latest.observed_at.strftime("%Y-%m-%d %H:%M UTC")

    # The typical gap is stated alongside the breach so the reader can tell
    # "an hour late" from "this table has never once updated in under two hours",
    # which is the difference between an incident and a wrong threshold.
    gaps = [p.value for p in points if p.value is not None]
    context: dict[str, Any] = {"last_written_at": written}
    if gaps:
        context["typical_gap_minutes"] = round(sorted(gaps)[len(gaps) // 2], 1)

    if age > limit:
        typical = context.get("typical_gap_minutes")
        detail = f", typically every {typical:.0f}m" if typical else ""
        return Result(
            status="breach",
            value=round(age, 1),
            threshold=threshold,
            subject=latest.subject,
            context=context,
            message=(
                f"last written {written} — {_minutes(age)} ago, "
                f"limit is {_minutes(limit)}{detail}"
            ),
        )
    return Result(
        status="ok",
        value=round(age, 1),
        threshold=threshold,
        subject=latest.subject,
        context=context,
        message=f"last written {written} ({_minutes(age)} ago)",
    )


def _judge_bounds(points: list[Point], config: dict[str, Any], *, unit: str) -> Result:
    latest = points[-1]
    value = latest.value
    if value is None:
        return Result(status="insufficient_data", message="latest observation has no value")

    low, high = config.get("min"), config.get("max")
    threshold = {k: v for k, v in (("min", low), ("max", high)) if v is not None}
    previous = points[-2].value if len(points) > 1 else None
    context = {"previous": previous}

    change = ""
    if previous:
        change = f" ({_change(previous, value)} vs previous run)"

    shown = _quantity(value)
    label = f" {unit}" if unit else ""
    if low is not None and value < low:
        return Result(
            status="breach", value=value, threshold=threshold, subject=latest.subject,
            context=context,
            message=f"{shown}{label}, below the floor of {_quantity(low)}{change}",
        )
    if high is not None and value > high:
        return Result(
            status="breach", value=value, threshold=threshold, subject=latest.subject,
            context=context,
            message=f"{shown}{label}, above the ceiling of {_quantity(high)}{change}",
        )
    return Result(
        status="ok", value=value, threshold=threshold, subject=latest.subject,
        context=context, message=f"{shown}{label}{change}",
    )


def _quantity(value: float) -> str:
    """Format a metric value at a precision that does not destroy it.

    Integer formatting is right for row counts and catastrophic for rates: a null
    rate of 0.979 against a ceiling of 0.5 renders as "1, above the ceiling of 0",
    which is not merely ugly but unreadable as a claim. The same judgement path
    serves both kinds of number, so the formatter has to adapt to the magnitude
    rather than the monitor kind.
    """
    if value == int(value) and abs(value) >= 1:
        return f"{value:,.0f}"
    if abs(value) >= 1:
        return f"{value:,.2f}"
    # Below 1, keep enough significant figures that a small rate and a smaller one
    # are visibly different.
    return f"{value:.4g}"


def _judge_schema(points: list[Point], config: dict[str, Any]) -> Result:
    """Deterministic. No statistics, no training window, no threshold to argue with.

    Added columns are reported but do not breach. Adding a column is the single
    most common schema change in a healthy dbt project, and a monitor that pages
    on every additive migration is a monitor that gets muted in week two -- taking
    the removals and type changes with it.
    """
    latest = points[-1]
    columns = latest.context.get("columns") or {}
    if len(points) < 2:
        return Result(
            status="ok",
            value=latest.value,
            subject=latest.subject,
            context={"columns": columns},
            message=f"{len(columns)} columns; no previous schema to compare against yet",
        )

    before = points[-2].context.get("columns") or {}
    removed = sorted(set(before) - set(columns))
    added = sorted(set(columns) - set(before))
    retyped = sorted(
        f"{name}: {before[name]} → {columns[name]}"
        for name in set(before) & set(columns)
        if before[name] != columns[name]
    )

    context = {"added": added, "removed": removed, "retyped": retyped, "columns": columns}
    breaking = removed + retyped
    if breaking and not config.get("allow_removed_columns"):
        parts = []
        if removed:
            parts.append(f"removed {', '.join(removed)}")
        if retyped:
            parts.append(f"retyped {'; '.join(retyped)}")
        if added:
            parts.append(f"added {', '.join(added)}")
        return Result(
            status="breach",
            value=latest.value,
            subject=latest.subject,
            context=context,
            message="schema changed: " + "; ".join(parts),
        )
    if added:
        return Result(
            status="ok", value=latest.value, subject=latest.subject, context=context,
            message=f"added {', '.join(added)} — additive, not a breach",
        )
    return Result(
        status="ok", value=latest.value, subject=latest.subject, context=context,
        message=f"{len(columns)} columns, unchanged",
    )


def _judge_max(
    points: list[Point], config: dict[str, Any], key: str, *, unit: str, label: str
) -> Result:
    latest = points[-1]
    value = latest.value
    if value is None:
        return Result(status="insufficient_data", message="latest observation has no value")
    limit = float(config[key])
    values = [p.value for p in points if p.value is not None]
    typical = sorted(values)[len(values) // 2] if values else None
    context = {"typical": typical}

    if value > limit:
        detail = f", typically {typical:,.0f}{unit}" if typical else ""
        return Result(
            status="breach", value=value, threshold={key: limit}, subject=latest.subject,
            context=context,
            message=f"{label} {value:,.0f}{unit}, limit is {limit:,.0f}{unit}{detail}",
        )
    return Result(
        status="ok", value=value, threshold={key: limit}, subject=latest.subject,
        context=context, message=f"{label} {value:,.0f}{unit}",
    )


def _judge_failure_rate(points: list[Point], config: dict[str, Any], now: datetime) -> Result:
    """Failure rate over a window, not over the last run.

    A single failure in a job that runs every five minutes is noise; the same
    failure in a nightly job is the whole story. A rate over a stated window is the
    only form that means the same thing for both.
    """
    window_hours = float(config.get("window_hours", 24))
    cutoff = now - timedelta(hours=window_hours)
    in_window = [p for p in points if p.observed_at >= cutoff and p.value is not None]
    limit = float(config["max_rate"])
    threshold = {"max_rate": limit, "window_hours": window_hours}

    if not in_window:
        return Result(
            status="insufficient_data",
            threshold=threshold,
            message=f"no runs in the last {window_hours:g}h",
        )

    failures = sum(1 for p in in_window if p.value and p.value > 0)
    rate = failures / len(in_window)
    context = {"failures": failures, "runs": len(in_window)}
    summary = f"{failures}/{len(in_window)} runs failed in {window_hours:g}h ({rate:.0%})"

    if rate > limit:
        return Result(
            status="breach", value=rate, threshold=threshold, context=context,
            subject=in_window[-1].subject,
            message=f"{summary}, limit is {limit:.0%}",
        )
    return Result(
        status="ok", value=rate, threshold=threshold, context=context,
        subject=in_window[-1].subject, message=summary,
    )


def _minutes(value: float) -> str:
    if value < 60:
        return f"{value:.0f}m"
    hours, minutes = divmod(int(value), 60)
    if hours < 24:
        return f"{hours}h{minutes:02d}m"
    days, hours = divmod(hours, 24)
    return f"{days}d{hours}h"


def _change(previous: float, current: float) -> str:
    if previous == 0:
        return "up from 0"
    ratio = current / previous
    if ratio >= 1:
        return f"{ratio:.1f}× more" if ratio >= 1.1 else "about the same"
    return f"{1 / ratio:.1f}× fewer" if ratio <= 0.9 else "about the same"


# ------------------------------------------------------------------ persistence


def store_points(conn: psycopg.Connection, monitor_id: int, points: list[Point]) -> int:
    """Upsert observations. Returns how many rows were written.

    `on conflict do update` rather than `do nothing`: re-collecting an
    already-stored observation should correct it, not skip it. A run's row count
    genuinely changes between its START and COMPLETE events, and the later value is
    the right one.

    But a re-collect must never *destroy* information, hence the coalesce. The case
    is real: freshness stores the gap since the previous write, and a collection
    window that starts after that previous write cannot compute one. Without the
    coalesce, the routine hourly check would overwrite a correct gap with null
    every time it ran, and the baseline would erode to nothing.
    """
    if not points:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into metric_points (monitor_id, observed_at, subject, value, context)
            values (%s, %s, %s, %s, %s)
            on conflict (monitor_id, observed_at, subject) do update
                set value = coalesce(excluded.value, metric_points.value),
                    context = excluded.context,
                    recorded_at = now()
            """,
            [
                (monitor_id, p.observed_at, p.subject, p.value, json.dumps(p.context, default=str))
                for p in points
            ],
        )
    return len(points)


def store_result(
    conn: psycopg.Connection, monitor: dict[str, Any], result: Result
) -> dict[str, Any]:
    """Record an evaluation and update the monitor's denormalised status.

    `transitioned` is computed here, against the status the monitor held before
    this evaluation. It is what alerting fires on, so it is what makes a table that
    has been broken since 02:00 one alert rather than one per hour.
    """
    transitioned = monitor.get("last_status") != result.status
    row = conn.execute(
        """
        insert into monitor_results (monitor_id, status, value, threshold, message,
                                     subject, run_id, context, transitioned)
        values (%(monitor_id)s, %(status)s, %(value)s, %(threshold)s, %(message)s,
                %(subject)s, %(run_id)s, %(context)s, %(transitioned)s)
        returning *
        """,
        {
            "monitor_id": monitor["id"],
            "status": result.status,
            "value": result.value,
            "threshold": json.dumps(result.threshold),
            "message": result.message,
            "subject": result.subject,
            "run_id": _as_run_id(result.subject),
            "context": json.dumps(result.context, default=str),
            "transitioned": transitioned,
        },
    ).fetchone()
    conn.execute(
        "update monitors set last_status = %s, last_evaluated_at = now() where id = %s",
        (result.status, monitor["id"]),
    )
    return row


def _as_run_id(subject: str | None) -> str | None:
    """Subjects are run ids for every kind we have so far, but the column is typed
    and a future kind's subject will not be. Fail soft rather than 500 a check."""
    from uuid import UUID

    if not subject:
        return None
    try:
        return str(UUID(subject))
    except ValueError:
        return None


# ---------------------------------------------------------------- the whole path


def evaluate(
    conn: psycopg.Connection,
    monitor: dict[str, Any],
    *,
    since: datetime | None = None,
    now: datetime | None = None,
    history_limit: int = 500,
    record: bool = True,
) -> dict[str, Any]:
    """Collect, store, judge and record one monitor.

    Judgement reads points back out of the database rather than using the ones
    just collected, so a narrow collection window still decides against the full
    history -- including points backfilled when the monitor was created.

    `record=False` collects and judges but persists no result and does not move
    `last_status`. That is what `dataspine apply` uses to arm a monitor, and it is
    load-bearing rather than tidiness: recording there would move the monitor's
    status to `breach` with nobody notified, so the first real `check` would see
    no transition and never alert. The breach would be visible in the UI and
    silently undelivered -- found end to end, by tightening a threshold and
    watching the alert not arrive.
    """
    from . import monitors as monitors_mod

    try:
        points = collect(conn, monitor, since=since)
        stored = store_points(conn, monitor["id"], points)
        history = [
            # Feedback is folded into the context rather than passed separately,
            # so the detector reads one shape whether its points came from the
            # database or from a test. It is a property of the observation.
            Point(
                observed_at=row["observed_at"],
                subject=row["subject"],
                value=row["value"],
                context={**(row["context"] or {}), "feedback": row.get("feedback")},
            )
            for row in monitors_mod.recent_points(conn, monitor["id"], limit=history_limit)
        ]
        result = judge(monitor, history, now=now)
    except Exception as exc:  # noqa: BLE001 - one broken monitor must not stop the sweep
        result = Result(status="error", message=f"{type(exc).__name__}: {exc}")
        stored = 0

    transitioned = False
    if record:
        transitioned = store_result(conn, monitor, result)["transitioned"]
    return {
        "monitor": monitor["name"],
        "status": result.status,
        "message": result.message,
        "value": result.value,
        "collected": stored,
        "transitioned": transitioned,
    }


def check_all(
    conn: psycopg.Connection,
    *,
    schedule: str | None = None,
    name: str | None = None,
    since: datetime | None = None,
    now: datetime | None = None,
    alert: bool = True,
    client: Any = None,
) -> list[dict[str, Any]]:
    """Evaluate every enabled monitor, then deliver whatever transitioned.

    One monitor's failure never stops the sweep, and delivery never fails it
    either -- see `alerts.deliver`.

    Alerting is part of this function rather than a step the caller remembers,
    because "we detected it but nobody wired up the notify command" is the most
    boring way for a monitoring tool to be useless. `alert=False` exists for
    backfills and threshold experiments, where evaluating six weeks of history
    would otherwise page the on-call once per historical incident.
    """
    from . import alerts as alerts_mod
    from . import incidents as incidents_mod
    from . import monitors as monitors_mod

    if name:
        monitor = monitors_mod.get_monitor(conn, name)
        targets = [monitor] if monitor else []
    else:
        targets = monitors_mod.list_monitors(conn, enabled_only=True, schedule=schedule)

    results = [evaluate(conn, m, since=since, now=now) for m in targets]
    if alert:
        # Suppression sits between evaluation and delivery, never inside either.
        # Every breach is still evaluated, recorded and visible in the UI; what
        # lineage decides is only which of them a human is *told* about. One late
        # source table breaching fifty downstream monitors is one page about the
        # source, not fifty about its consequences.
        alerts_mod.deliver(
            incidents_mod.suppress(conn, results, now=now), client=client, conn=conn
        )
    return results
