"""Partition maintenance and retention.

Two jobs, both boring by design:

  `ensure_partitions` provisions month partitions ahead of time so live inserts
  land in a real partition rather than the DEFAULT backstop.

  `apply_retention` drops whole partitions that fall outside the keep window.

Retention deliberately never touches the DEFAULT partition. It holds exactly the
rows whose timestamps we did not anticipate, which is the population most likely
to represent a bug worth investigating -- and dropping it would silently resume
losing out-of-range rows.

Every function takes a `table`, defaulting to `events`. `metric_points`
(migration 007) is partitioned the same way for the same reasons, and ADR-005
turns on not needing a second mechanism to maintain it.
"""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime

import psycopg

log = logging.getLogger("dataspine.retention")

# Every table this module maintains. `dataspine maintain` walks them all, so
# adding a partitioned table here is the only step needed to get it maintained.
PARTITIONED_TABLES = ("events", "metric_points")


def default_partition(table: str = "events") -> str:
    return f"{table}_default"


def _partition_re(table: str) -> re.Pattern[str]:
    return re.compile(rf"^{re.escape(table)}_(\d{{4}})_(\d{{2}})$")


def is_partitioned(conn: psycopg.Connection, table: str = "events") -> bool:
    row = conn.execute(
        "select relkind from pg_class where relname = %s and relkind = 'p'", (table,)
    ).fetchone()
    return row is not None


def _month_bounds(year: int, month: int) -> tuple[str, str]:
    start = datetime(year, month, 1, tzinfo=UTC)
    end = datetime(year + (month == 12), (month % 12) + 1, 1, tzinfo=UTC)
    return start.isoformat(), end.isoformat()


def _months_from(around: datetime, count: int) -> list[tuple[int, int]]:
    months = []
    year, month = around.year, around.month
    for _ in range(count):
        months.append((year, month))
        month += 1
        if month > 12:
            year, month = year + 1, 1
    return months


def ensure_partitions(
    conn: psycopg.Connection,
    *,
    table: str = "events",
    around: datetime | None = None,
    months_ahead: int = 3,
) -> list[str]:
    """Create month partitions starting at `around`. Returns what it created.

    Idempotent: `create table if not exists` plus a check for existing
    attachment, so running this from a cron every hour is harmless.
    """
    around = around or datetime.now(UTC)
    existing = set(partition_names(conn, table))
    created: list[str] = []

    for year, month in _months_from(around, months_ahead):
        name = f"{table}_{year:04d}_{month:02d}"
        if name in existing:
            continue
        start, end = _month_bounds(year, month)
        try:
            # Savepoint per partition: one blocked month must not roll back the
            # caller's transaction, or a single stray row would abort the whole
            # maintenance run (and anything else batched with it).
            with conn.transaction():
                conn.execute(
                    f"create table {name} partition of {table} "
                    f"for values from ('{start}') to ('{end}')"
                )
        except (psycopg.errors.CheckViolation, psycopg.errors.InvalidObjectDefinition):
            # Rows for this month already sit in the DEFAULT partition, so
            # Postgres refuses to attach a partition that would have to claim
            # them. (It reports this as a check violation on the default
            # partition's constraint, which is not the error you would guess.)
            #
            # Not fatal: the default keeps accepting writes, which is precisely
            # why it exists. We lose partition-level retention for that month
            # until the rows are moved, and we say so.
            log.warning(
                "cannot create %s: rows for that month are already in %s. "
                "Writes still succeed via the default partition, but that month "
                "cannot be dropped by retention until those rows are moved.",
                name,
                default_partition(table),
            )
            continue
        created.append(name)

    if created:
        log.info("created %s partitions: %s", table, ", ".join(created))
    return created


def partition_names(conn: psycopg.Connection, table: str = "events") -> list[str]:
    return [
        r["relname"]
        for r in conn.execute(
            """
            select c.relname
            from pg_class c
            join pg_inherits i on i.inhrelid = c.oid
            join pg_class p on p.oid = i.inhparent
            where p.relname = %s
            order by c.relname
            """,
            (table,),
        ).fetchall()
    ]


def apply_retention(
    conn: psycopg.Connection,
    *,
    keep_months: int,
    table: str = "events",
    now: datetime | None = None,
) -> list[str]:
    """Drop month partitions older than the keep window. Returns what it dropped.

    `keep_months` counts back from the current month inclusive, so keep_months=3
    in August keeps June, July and August.
    """
    now = now or datetime.now(UTC)
    cutoff_year, cutoff_month = now.year, now.month
    for _ in range(max(keep_months - 1, 0)):
        cutoff_month -= 1
        if cutoff_month < 1:
            cutoff_year, cutoff_month = cutoff_year - 1, 12

    dropped: list[str] = []
    pattern = _partition_re(table)
    for name in partition_names(conn, table):
        # Never the backstop: it holds the rows whose timestamps we did not
        # anticipate, and dropping it would quietly resume losing them.
        if name == default_partition(table):
            continue
        match = pattern.match(name)
        if not match:
            continue
        year, month = int(match.group(1)), int(match.group(2))
        if (year, month) < (cutoff_year, cutoff_month):
            conn.execute(f"drop table {name}")
            dropped.append(name)

    if dropped:
        log.info("dropped %s partitions: %s", table, ", ".join(dropped))
    return dropped
