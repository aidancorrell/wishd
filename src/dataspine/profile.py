"""Column profiling, with a budget that is the point rather than a precaution.

Every other collector in this project reads metadata an engine maintains for its
own planner, and costs the user nothing. This one reads the data. That single
difference drives the whole design:

  **Opt-in.** Nothing is profiled unless a monitor asks for it. A default install
  never reads a data value, which is the promise the rest of the collection layer
  is built on and not one to give up quietly.

  **Budgeted.** Above `max_rows` the profiler samples instead of scanning, and
  records that it did. A null rate from a 1% sample and one from a full table are
  different claims, and a tool that presents them identically is lying by
  omission. `max_rows=0` means never scan, and is obeyed.

  **One pass.** All statistics for a table come from a single query. Ten queries
  per table is ten table scans, which is how a "lightweight" profiler becomes the
  most expensive thing running against a warehouse.

Sketches (HLL, t-digest) are deliberately absent -- see the roadmap's deferral.
Exact aggregates over a bounded sample answer the same questions here, and every
engine spells its sketch functions differently, so the abstraction would have to
be built before it could be shown to be worth it.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg

log = logging.getLogger("dataspine.profile")

# Above this, sample. Chosen to be large enough that most dbt marts are profiled
# exactly and small enough that a fact table is not.
DEFAULT_MAX_ROWS = 1_000_000

METRICS = (
    "null_rate", "uniqueness", "cardinality", "min", "max", "mean", "sum",
    "stddev", "zero_rate", "negative_rate",
)

# Column -> stored column name. The monitor spec speaks the roadmap's vocabulary;
# the table stores unambiguous names.
METRIC_COLUMNS = {
    "null_rate": "null_rate",
    "uniqueness": "uniqueness",
    "cardinality": "distinct_count",
    "min": "min_value",
    "max": "max_value",
    "mean": "mean_value",
    "sum": "sum_value",
    "stddev": "stddev_value",
    "zero_rate": "zero_rate",
    "negative_rate": "negative_rate",
}

NUMERIC_TYPES = (
    "smallint", "integer", "bigint", "decimal", "numeric", "real",
    "double precision", "money",
)


@dataclass
class ColumnProfile:
    column: str
    row_count: int | None = None
    null_rate: float | None = None
    distinct_count: int | None = None
    uniqueness: float | None = None
    min: float | None = None
    max: float | None = None
    mean: float | None = None
    sum: float | None = None
    stddev: float | None = None
    zero_rate: float | None = None
    negative_rate: float | None = None
    sampled: bool = False
    scanned_rows: int | None = None


def enabled_for(conn: psycopg.Connection, table: str) -> bool:
    """Whether anything has asked for this table to be profiled.

    Profiling exists to serve `column_stats` monitors. With none defined, running
    it would be spending someone's warehouse budget to populate a table nothing
    reads.
    """
    row = conn.execute(
        """
        select 1 from monitors
        where kind = 'column_stats' and enabled
          and (target = %(table)s or %(table)s like '%%' || target || '%%'
               or target like '%%' || %(table)s || '%%')
        limit 1
        """,
        {"table": table},
    ).fetchone()
    return row is not None


def profile_table(
    conn: psycopg.Connection,
    table: str,
    *,
    columns: list[str] | None = None,
    max_rows: int = DEFAULT_MAX_ROWS,
) -> list[ColumnProfile]:
    """Profile every column of `table` in one pass. Returns one profile each.

    `max_rows` is a hard budget: zero refuses to scan at all, and anything above
    the table's size profiles it exactly.
    """
    if max_rows <= 0:
        return []

    described = _describe(conn, table, columns)
    if not described:
        return []

    estimate = _estimated_rows(conn, table, max_rows)
    sampled = estimate > max_rows
    # TABLESAMPLE SYSTEM is page-level and therefore cheap; it is also biased for
    # clustered data, which is acceptable for a rate and recorded either way.
    fraction = min(100.0 * max_rows / estimate, 100.0) if sampled and estimate else 100.0
    source = (
        f"{_ident(table)} tablesample system ({fraction:.4f})" if sampled else _ident(table)
    )

    selects = ["count(*) as _rows"]
    for name, data_type in described.items():
        col = _ident(name)
        selects += [
            f"count({col}) as {_alias(name, 'nonnull')}",
            f"count(distinct {col}) as {_alias(name, 'distinct')}",
        ]
        if data_type in NUMERIC_TYPES:
            selects += [
                f"min({col})::double precision as {_alias(name, 'min')}",
                f"max({col})::double precision as {_alias(name, 'max')}",
                f"avg({col})::double precision as {_alias(name, 'mean')}",
                f"sum({col})::double precision as {_alias(name, 'sum')}",
                f"stddev_samp({col})::double precision as {_alias(name, 'stddev')}",
                f"count(*) filter (where {col} = 0) as {_alias(name, 'zero')}",
                f"count(*) filter (where {col} < 0) as {_alias(name, 'neg')}",
            ]

    # One query, one scan. Ten queries would be ten scans of the same table.
    row = conn.execute(f"select {', '.join(selects)} from {source}").fetchone()  # noqa: S608
    scanned = row["_rows"] or 0

    profiles = []
    for name, data_type in described.items():
        non_null = row[_alias(name, "nonnull")] or 0
        distinct = row[_alias(name, "distinct")]
        numeric = data_type in NUMERIC_TYPES
        profiles.append(
            ColumnProfile(
                column=name,
                row_count=scanned,
                null_rate=(scanned - non_null) / scanned if scanned else None,
                distinct_count=distinct,
                uniqueness=(distinct / non_null) if non_null and distinct is not None else None,
                min=row[_alias(name, "min")] if numeric else None,
                max=row[_alias(name, "max")] if numeric else None,
                mean=row[_alias(name, "mean")] if numeric else None,
                sum=row[_alias(name, "sum")] if numeric else None,
                stddev=row[_alias(name, "stddev")] if numeric else None,
                zero_rate=(
                    (row[_alias(name, "zero")] or 0) / scanned if numeric and scanned else None
                ),
                negative_rate=(
                    (row[_alias(name, "neg")] or 0) / scanned if numeric and scanned else None
                ),
                sampled=sampled,
                scanned_rows=scanned,
            )
        )
    return profiles


def _describe(
    conn: psycopg.Connection, table: str, columns: list[str] | None
) -> dict[str, str]:
    rows = conn.execute(
        """
        select column_name, data_type
        from information_schema.columns
        where table_name = %s
        order by ordinal_position
        """,
        (table.split(".")[-1],),
    ).fetchall()
    described = {r["column_name"]: r["data_type"] for r in rows}
    if columns:
        described = {k: v for k, v in described.items() if k in columns}
    return described


def _estimated_rows(conn: psycopg.Connection, table: str, budget: int) -> int:
    """How big is this table, without paying to find out exactly.

    The planner's `reltuples` is free and usually right. But it is **-1 on a table
    that has never been analysed**, and a freshly loaded table is exactly the case
    where a naive reading returns "0 rows", concludes no sampling is needed, and
    scans the whole thing. That is the budget failing silently in the one
    situation it exists for.

    So when the planner has nothing, fall back to a probe that stops at the budget
    itself: `select count(*) from (select 1 from t limit budget+1)`. It reads at
    most one row more than we were already prepared to read, which makes finding
    out the size no more expensive than the scan we are deciding about.
    """
    row = conn.execute(
        "select reltuples::bigint as n from pg_class where relname = %s",
        (table.split(".")[-1],),
    ).fetchone()
    estimate = int(row["n"]) if row and row["n"] is not None else -1
    if estimate >= 0:
        return estimate

    probe = conn.execute(
        f"select count(*) as n from (select 1 from {_ident(table)} limit %s) probe"  # noqa: S608
        , (budget + 1,)
    ).fetchone()
    return int(probe["n"]) if probe else 0


def _ident(name: str) -> str:
    """Quote an identifier. Profiling targets come from YAML in the user's repo,
    which is reviewed -- but a table called `orders; drop table` must still be a
    table name and not a statement."""
    return '"' + name.replace('"', '""') + '"'


def _alias(column: str, suffix: str) -> str:
    """A collision-free result alias.

    Column names can contain anything, so the alias is positional-ish rather than
    derived: `"email"` and `"email nonnull"` must not collide with a real column
    called `email_nonnull`.
    """
    safe = "".join(c if c.isalnum() else "_" for c in column)
    return f"c_{abs(hash(column)) % 10**6}_{safe[:20]}_{suffix}"


# ------------------------------------------------------------------- storage


def store_profiles(
    conn: psycopg.Connection,
    dataset_id: int,
    profiles: list[ColumnProfile],
    *,
    observed_at: datetime,
) -> int:
    """Persist profiles for one observation moment. Idempotent on re-run."""
    for entry in profiles:
        conn.execute(
            """
            insert into column_profiles
                (dataset_id, observed_at, column_name, row_count, null_rate,
                 distinct_count, uniqueness, min_value, max_value, mean_value,
                 sum_value, stddev_value, zero_rate, negative_rate, sampled, scanned_rows)
            values (%(dataset_id)s, %(observed_at)s, %(column)s, %(row_count)s, %(null_rate)s,
                    %(distinct_count)s, %(uniqueness)s, %(min)s, %(max)s, %(mean)s,
                    %(sum)s, %(stddev)s, %(zero_rate)s, %(negative_rate)s, %(sampled)s,
                    %(scanned_rows)s)
            on conflict (dataset_id, observed_at, column_name) do update set
                row_count = excluded.row_count,
                null_rate = excluded.null_rate,
                distinct_count = excluded.distinct_count,
                uniqueness = excluded.uniqueness,
                min_value = excluded.min_value,
                max_value = excluded.max_value,
                mean_value = excluded.mean_value,
                sum_value = excluded.sum_value,
                stddev_value = excluded.stddev_value,
                zero_rate = excluded.zero_rate,
                negative_rate = excluded.negative_rate,
                sampled = excluded.sampled,
                scanned_rows = excluded.scanned_rows,
                recorded_at = now()
            """,
            {
                "dataset_id": dataset_id,
                "observed_at": observed_at,
                "column": entry.column,
                "row_count": entry.row_count,
                "null_rate": entry.null_rate,
                "distinct_count": entry.distinct_count,
                "uniqueness": entry.uniqueness,
                "min": entry.min,
                "max": entry.max,
                "mean": entry.mean,
                "sum": entry.sum,
                "stddev": entry.stddev,
                "zero_rate": entry.zero_rate,
                "negative_rate": entry.negative_rate,
                "sampled": entry.sampled,
                "scanned_rows": entry.scanned_rows,
            },
        )
    return len(profiles)


def history(
    conn: psycopg.Connection,
    dataset_ids: list[int],
    column: str,
    metric: str,
    *,
    since: datetime | None = None,
) -> list[dict[str, Any]]:
    """Stored values of one column statistic, oldest first."""
    stored = METRIC_COLUMNS.get(metric)
    if stored is None:
        raise ValueError(f"unknown column metric {metric!r}")
    return conn.execute(
        f"""
        select observed_at, {stored} as value, sampled, scanned_rows
        from column_profiles
        where dataset_id = any(%(ids)s)
          and column_name = %(column)s
          and {stored} is not null
          and (%(since)s::timestamptz is null or observed_at >= %(since)s)
        order by observed_at
        """,  # noqa: S608 - `stored` comes from METRIC_COLUMNS, never from user input
        {"ids": dataset_ids, "column": column, "since": since},
    ).fetchall()
