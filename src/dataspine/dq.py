"""Results from data-quality engines we do not own.

This module is a decision *not* to build something.

Snowflake data metric functions and Databricks DQ rules already run inside the
warehouse: closer to the data, on compute the customer is already paying for, and
maintained by the vendor. Reimplementing them would ask a team to operate two
systems that disagree with each other about the same table, and the argument
about which one is right is not a fight worth starting.

So we read their results and treat them as one more signal beside our own. The
payoff is positional rather than technical -- it makes dataspine the place you
look, rather than another thing you have to look at.

Rows arrive by `POST /api/v1/dq/{source}`, so whatever already runs those checks
on a schedule can forward them without us holding warehouse credentials.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

import psycopg

log = logging.getLogger("dataspine.dq")

STATUSES = ("pass", "fail", "error")


def import_results(
    conn: psycopg.Connection, *, source: str, rows: list[dict[str, Any]]
) -> int:
    """Store externally-produced check results. Returns rows written.

    Idempotent on `(source, table, check, measured_at)`: re-importing an
    overlapping window is the normal case when someone forwards "the last 24
    hours" every hour, and it must not multiply rows.
    """
    written = 0
    for row in rows:
        table = str(row.get("table") or "").strip()
        check = str(row.get("check") or "").strip()
        measured_at = _as_datetime(row.get("measured_at"))
        if not table or not check or measured_at is None:
            log.warning("skipping DQ result with no table, check or timestamp: %r", row)
            continue

        status = str(row.get("status") or "").lower()
        if status not in STATUSES:
            # Normalise the common boolean-ish spellings rather than rejecting
            # them; every engine spells this differently and a dropped failure is
            # worse than a guessed one.
            status = "pass" if status in ("true", "ok", "success", "passed") else "fail"

        conn.execute(
            """
            insert into external_checks
                (source, dataset_id, table_name, check_name, status, value,
                 measured_at, details)
            values (
                %(source)s,
                -- Resolved by the same leaf-segment rule monitors use, so a
                -- warehouse-qualified name matches the dataset our producers
                -- report under a different prefix.
                --
                -- Case-insensitively, because the warehouses genuinely disagree:
                -- Snowflake reports FCT_ORDERS, everything else reports
                -- fct_orders, and an unquoted SQL identifier means the same
                -- table either way. Matching exactly would leave every dbt Cloud
                -- test on Snowflake attached to no dataset at all. An exact hit
                -- still wins the ordering.
                (select id from datasets
                  where lower(name) = lower(%(table)s)
                     or regexp_replace(lower(name), '^.*[./]', '')
                        = regexp_replace(lower(%(table)s), '^.*[./]', '')
                  order by (name = %(table)s) desc, (lower(name) = lower(%(table)s)) desc
                  limit 1),
                %(table)s, %(check)s, %(status)s, %(value)s, %(measured_at)s, %(details)s
            )
            on conflict (source, table_name, check_name, measured_at) do update set
                status = excluded.status,
                value = excluded.value,
                details = excluded.details,
                recorded_at = now()
            """,
            {
                "source": source,
                "table": table,
                "check": check,
                "status": status,
                "value": _as_float(row.get("value")),
                "measured_at": measured_at,
                "details": json.dumps(row.get("details") or {}),
            },
        )
        written += 1
    return written


def failing(conn: psycopg.Connection, *, limit: int = 100) -> list[dict[str, Any]]:
    """Currently-failing external checks, newest measurement per check.

    Distinct on the check rather than every row: a DMF that has failed hourly for
    three days is one problem, and listing it 72 times would bury everything else
    for the same reason ungrouped alerts do.
    """
    return conn.execute(
        """
        select * from (
            select distinct on (source, table_name, check_name)
                   id, source, dataset_id, table_name, check_name, status, value, measured_at
            from external_checks
            order by source, table_name, check_name, measured_at desc
        ) latest
        where status = 'fail'
        order by measured_at desc
        limit %s
        """,
        (limit,),
    ).fetchall()


def transitions(
    conn: psycopg.Connection, *, since: datetime, limit: int = 100
) -> list[dict[str, Any]]:
    """Checks whose latest result differs from the one before it.

    The same rule monitor alerting uses, for the same reason: a `not_null` test
    that has failed on every hourly run for three days is one problem, and
    telling someone about it 72 times is how the channel gets muted -- taking
    every future alert with it.

    A recovery counts as a transition. A check that only ever speaks when it
    breaks leaves an unresolved failure indistinguishable from an ongoing one,
    and somebody has to remember to go and look.

    A first-ever result that passes is not a transition. Otherwise importing a
    project's history would announce every test in it as good news.
    """
    return conn.execute(
        """
        with ranked as (
            select source, table_name, check_name, status, value, measured_at, details,
                   lag(status) over (
                       partition by source, table_name, check_name order by measured_at
                   ) as previous
            from external_checks
        )
        -- Every transition in the window, not only each check's latest result.
        -- Looking at the latest alone loses a transition entirely whenever
        -- another run lands before the next sweep -- a dbt job on a tighter
        -- schedule than `dataspine notify` would silently hide its own
        -- failures. The ledger, not this query, is what keeps each one to a
        -- single message.
        select * from ranked
        where measured_at >= %(since)s
          and status is distinct from previous
          and (status in ('fail', 'error') or previous in ('fail', 'error'))
        order by (status = 'pass'), measured_at desc, source, table_name, check_name
        limit %(limit)s
        """,
        {"since": since, "limit": limit},
    ).fetchall()


def recent(conn: psycopg.Connection, *, limit: int = 100) -> list[dict[str, Any]]:
    return conn.execute(
        "select * from external_checks order by measured_at desc, id desc limit %s",
        (limit,),
    ).fetchall()


def _as_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return None


def _as_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
