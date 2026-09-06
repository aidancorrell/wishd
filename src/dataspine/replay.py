"""Rebuild derived state from the raw event archive.

The `events` table is the source of truth; `runs`, `jobs`, `datasets` and
`run_datasets` are a projection of it. That distinction is what makes correlator
changes safe: when the stitching logic improves -- and it will, the first time a
real producer emits something the simulator does not -- you re-derive history
instead of living with whatever the old logic happened to record.

This is also the repair tool for the failure mode that actually happens: a
producer misconfigured for a week, fixed, and now a month of trees are wrong.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import datetime
from typing import Any

import psycopg
from pydantic import ValidationError

from .events import RunEvent
from .ingest import ingest_run_event

log = logging.getLogger("dataspine.replay")


def iter_events(
    conn: psycopg.Connection,
    *,
    since: datetime | None = None,
    until: datetime | None = None,
    batch_size: int = 1000,
) -> Iterator[dict[str, Any]]:
    """Stream archived payloads in receipt order.

    Ordered by `id`, not `event_time`: we replay in the order we actually
    received things, so a replay reproduces the same sequence of decisions the
    live path made. Ordering by event_time would silently *fix* out-of-order
    delivery and hide correlator bugs that only appear under real conditions.
    """
    where = ["event_kind = 'RUN'"]
    params: dict[str, Any] = {"batch": batch_size}
    if since:
        where.append("received_at >= %(since)s")
        params["since"] = since
    if until:
        where.append("received_at < %(until)s")
        params["until"] = until

    last_id = 0
    while True:
        params["last_id"] = last_id
        rows = conn.execute(
            f"""
            select id, payload from events
            where {' and '.join(where)} and id > %(last_id)s
            order by id
            limit %(batch)s
            """,
            params,
        ).fetchall()
        if not rows:
            return
        for row in rows:
            last_id = row["id"]
            yield row["payload"]


def replay(
    conn: psycopg.Connection,
    *,
    since: datetime | None = None,
    until: datetime | None = None,
    truncate: bool = True,
    progress: Any = None,
) -> dict[str, int]:
    """Re-derive runs/jobs/datasets from archived events.

    `truncate=True` drops the projection first, which is what you want after a
    correlator change: a partial rebuild over stale rows can leave a tree half
    stitched by old logic and half by new. It deliberately does NOT touch
    `events` -- losing the archive would make this operation one-way.

    Runs inside the caller's transaction, so a failure mid-replay rolls back to
    the previous projection rather than leaving an empty database.
    """
    stats = {"events": 0, "ingested": 0, "invalid": 0}

    if truncate:
        if since or until:
            raise ValueError(
                "truncate=True rebuilds everything, so a time window makes no sense. "
                "Pass truncate=False to re-ingest a window on top of existing state."
            )
        # events is intentionally absent: it is the source, not the projection.
        conn.execute("truncate run_datasets, runs, datasets, jobs restart identity cascade")

    for payload in iter_events(conn, since=since, until=until):
        stats["events"] += 1
        try:
            event = RunEvent.model_validate(payload)
        except ValidationError:
            # Archived because we accept anything; skipped now because it was
            # never a valid RunEvent. Counted so the number is visible.
            stats["invalid"] += 1
            continue
        # archive=False: we are reading the archive, not adding to it.
        ingest_run_event(conn, event, archive=False)
        stats["ingested"] += 1
        if progress and stats["events"] % 500 == 0:
            progress(stats)

    return stats
