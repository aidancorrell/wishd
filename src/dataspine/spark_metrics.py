"""Persisting Spark event-log metrics and joining them to runs."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import psycopg

from .sparklog import SparkAppSummary

log = logging.getLogger("dataspine.spark_metrics")

APP_ID_PATH = "{spark_applicationDetails,applicationId}"


def _ts(millis: int | None) -> datetime | None:
    return datetime.fromtimestamp(millis / 1000, tz=UTC) if millis else None


def find_run_for_app(conn: psycopg.Connection, app_id: str) -> UUID | None:
    """Locate the OpenLineage run that reported this Spark application.

    Prefers the shallowest match: the Spark integration emits an APPLICATION run
    plus a SQL_JOB run per execution, all carrying the same applicationId, and
    the application-level run is the one these metrics describe.
    """
    row = conn.execute(
        f"""
        select r.run_id
        from runs r
        where r.facets #>> '{APP_ID_PATH}' = %s
        order by r.depth
        limit 1
        """,
        (app_id,),
    ).fetchone()
    return row["run_id"] if row else None


def store(
    conn: psycopg.Connection, summary: SparkAppSummary, *, source_uri: str | None = None
) -> UUID | None:
    """Upsert a parsed application. Returns the run it linked to, if any."""
    if not summary.app_id:
        log.warning("event log has no application id; not storing (source=%s)", source_uri)
        return None

    run_id = find_run_for_app(conn, summary.app_id)
    conn.execute(
        """
        insert into spark_apps (app_id, run_id, app_name, started_at, ended_at,
                                duration_ms, metrics, source_uri)
        values (%(app_id)s, %(run_id)s, %(app_name)s, %(started_at)s, %(ended_at)s,
                %(duration_ms)s, %(metrics)s, %(source_uri)s)
        on conflict (app_id) do update set
            -- coalesce: a relink must not be undone by a later re-ingest that
            -- happened to run before the OpenLineage events landed.
            run_id      = coalesce(excluded.run_id, spark_apps.run_id),
            app_name    = coalesce(excluded.app_name, spark_apps.app_name),
            started_at  = coalesce(excluded.started_at, spark_apps.started_at),
            ended_at    = coalesce(excluded.ended_at, spark_apps.ended_at),
            duration_ms = coalesce(excluded.duration_ms, spark_apps.duration_ms),
            metrics     = excluded.metrics,
            source_uri  = coalesce(excluded.source_uri, spark_apps.source_uri),
            updated_at  = now()
        """,
        {
            "app_id": summary.app_id,
            "run_id": run_id,
            "app_name": summary.app_name,
            "started_at": _ts(summary.start_time_ms),
            "ended_at": _ts(summary.end_time_ms),
            "duration_ms": summary.duration_ms,
            "metrics": json.dumps(summary.to_dict()),
            "source_uri": source_uri,
        },
    )
    return run_id


def relink_orphans(conn: psycopg.Connection) -> int:
    """Attach metrics stored before their OpenLineage run arrived.

    Backfill-then-live is the normal order when importing history, so this is a
    routine reconciliation rather than a repair.
    """
    cur = conn.execute(
        f"""
        update spark_apps s
        set run_id = r.run_id, updated_at = now()
        from runs r
        where s.run_id is null
          and r.facets #>> '{APP_ID_PATH}' = s.app_id
        """
    )
    return cur.rowcount or 0


def get_by_app_id(conn: psycopg.Connection, app_id: str | None) -> dict[str, Any] | None:
    if not app_id:
        return None
    return conn.execute("select * from spark_apps where app_id = %s", (app_id,)).fetchone()


def get_for_run(conn: psycopg.Connection, run_id: UUID) -> dict[str, Any] | None:
    """Metrics for a run, including a run nested under the Spark application.

    A dbt model's run has no application id of its own, but the Spark
    application beneath it does -- so look down the tree as well as at the run
    itself. Without this, the metrics would only ever show on the one node that
    happened to carry the facet.
    """
    row = conn.execute("select * from spark_apps where run_id = %s", (run_id,)).fetchone()
    if row:
        return row
    return conn.execute(
        """
        with recursive tree as (
            select run_id, 0 as level from runs where run_id = %s
          union all
            select c.run_id, t.level + 1
            from runs c join tree t on c.parent_run_id = t.run_id
            where t.level < 8
        )
        select s.* from tree t join spark_apps s on s.run_id = t.run_id
        order by t.level limit 1
        """,
        (run_id,),
    ).fetchone()
