"""Event-table partitioning and retention.

`events` is the append-only archive every other table is derived from, so it
grows fastest and is the first thing that will hurt. Monthly range partitions
make retention a metadata operation (`drop table`) instead of a
multi-hour `DELETE` that bloats the heap and fights autovacuum.

The safety property that matters most: **an event must never fail to insert
because a partition is missing.** A lost event is a hole in a run tree forever,
and no retention scheme is worth that. Hence a DEFAULT partition as the backstop,
plus a maintenance command that provisions months ahead of time.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from dataspine import retention
from dataspine.events import RunEvent
from dataspine.ingest import ingest_run_event
from dataspine.simulate import build_pipeline


def _insert_at(conn, when: datetime, run_name: str = "j") -> None:
    conn.execute(
        """
        insert into events (received_at, event_time, event_kind, event_type,
                            job_namespace, job_name, producer, payload)
        values (%s, %s, 'RUN', 'START', 'test', %s, 'test', %s)
        """,
        (when, when, run_name, json.dumps({"stub": True})),
    )


def test_events_is_partitioned_by_month(conn):
    assert retention.is_partitioned(conn), "events is not a partitioned table"


def test_rows_land_in_their_month_partition(conn):
    retention.ensure_partitions(conn, around=datetime(2026, 3, 15, tzinfo=UTC), months_ahead=2)
    _insert_at(conn, datetime(2026, 3, 10, tzinfo=UTC))
    _insert_at(conn, datetime(2026, 4, 10, tzinfo=UTC))

    counts = {
        r["partition"]: r["rows"]
        for r in conn.execute(
            """
            select c.relname as partition, count(e.*) as rows
            from pg_class c
            join pg_inherits i on i.inhrelid = c.oid
            join pg_class p on p.oid = i.inhparent
            left join events e on true
            where p.relname = 'events' and c.relname like 'events_2026%%'
            group by c.relname
            """
        ).fetchall()
    }
    assert "events_2026_03" in counts
    assert "events_2026_04" in counts

    march = conn.execute("select count(*) c from events_2026_03").fetchone()["c"]
    april = conn.execute("select count(*) c from events_2026_04").fetchone()["c"]
    assert march == 1 and april == 1


def test_an_event_outside_every_partition_is_never_lost(conn):
    """The backstop. A clock-skewed producer, a backfill with an odd timestamp,
    or a missed maintenance run must not turn into a rejected insert."""
    far_future = datetime.now(UTC) + timedelta(days=3650)
    _insert_at(conn, far_future, run_name="from_the_future")

    row = conn.execute(
        "select count(*) c from events where job_name = 'from_the_future'"
    ).fetchone()
    assert row["c"] == 1, "an event was rejected because no partition covered it"


def test_ingest_still_works_after_partitioning(conn):
    for event in build_pipeline(fail_model=None, start=datetime.now(UTC)):
        ingest_run_event(conn, RunEvent.model_validate(event))
    assert conn.execute("select count(*) c from events").fetchone()["c"] > 0
    assert conn.execute("select count(*) c from runs").fetchone()["c"] > 0


def test_retention_drops_only_old_partitions(conn):
    now = datetime(2026, 8, 15, tzinfo=UTC)
    retention.ensure_partitions(conn, around=datetime(2026, 3, 15, tzinfo=UTC), months_ahead=6)
    _insert_at(conn, datetime(2026, 3, 10, tzinfo=UTC), "old")
    _insert_at(conn, datetime(2026, 8, 10, tzinfo=UTC), "recent")

    dropped = retention.apply_retention(conn, keep_months=3, now=now)

    assert "events_2026_03" in dropped
    assert "events_2026_08" not in dropped
    remaining = conn.execute("select job_name from events order by job_name").fetchall()
    names = [r["job_name"] for r in remaining]
    assert "recent" in names
    assert "old" not in names


def test_retention_is_a_metadata_operation(conn):
    """Retention must DROP partitions, not DELETE rows. A delete-based scheme on
    the largest table in the system is how you get a locked, bloated archive."""
    retention.ensure_partitions(conn, around=datetime(2026, 1, 15, tzinfo=UTC), months_ahead=1)
    before = _partition_names(conn)
    retention.apply_retention(conn, keep_months=1, now=datetime(2026, 8, 15, tzinfo=UTC))
    after = _partition_names(conn)
    assert len(after) < len(before), "no partition was dropped"


def test_retention_never_drops_the_default_partition(conn):
    """Dropping the backstop would silently start losing out-of-range events."""
    retention.apply_retention(conn, keep_months=0, now=datetime(2030, 1, 1, tzinfo=UTC))
    assert "events_default" in _partition_names(conn)


def test_ensure_partitions_is_idempotent(conn):
    now = datetime(2026, 8, 15, tzinfo=UTC)
    first = retention.ensure_partitions(conn, around=now, months_ahead=3)
    second = retention.ensure_partitions(conn, around=now, months_ahead=3)
    assert first, "no partitions were created on the first call"
    assert second == [], "second call recreated partitions"


def test_replay_still_reproduces_the_projection(conn):
    """Partitioning changes how `events` is stored; replay reads it in id order
    and must be unaffected."""
    from dataspine.replay import replay

    for event in build_pipeline(fail_model="fct_order_items", start=datetime.now(UTC)):
        ingest_run_event(conn, RunEvent.model_validate(event))
    before = sorted(
        (str(r["run_id"]), r["state"])
        for r in conn.execute("select run_id, state from runs").fetchall()
    )

    replay(conn)

    after = sorted(
        (str(r["run_id"]), r["state"])
        for r in conn.execute("select run_id, state from runs").fetchall()
    )
    assert before == after


@pytest.mark.parametrize("keep", [1, 6, 12])
def test_retention_keeps_the_requested_window(conn, keep):
    now = datetime(2026, 12, 15, tzinfo=UTC)
    retention.ensure_partitions(conn, around=datetime(2026, 1, 15, tzinfo=UTC), months_ahead=12)
    retention.apply_retention(conn, keep_months=keep, now=now)
    kept = [p for p in _partition_names(conn) if p.startswith("events_2026_")]
    # keep_months counts back from the current month inclusive.
    assert len(kept) <= keep + 1, f"kept {kept} for keep_months={keep}"


def _partition_names(conn) -> list[str]:
    return [
        r["relname"]
        for r in conn.execute(
            """
            select c.relname from pg_class c
            join pg_inherits i on i.inhrelid = c.oid
            join pg_class p on p.oid = i.inhparent
            where p.relname = 'events'
            """
        ).fetchall()
    ]


def test_ensure_partitions_survives_rows_already_in_default(conn):
    """Found by running `dataspine maintain` on a real database.

    If rows for month M already sit in the DEFAULT partition, Postgres refuses to
    attach a partition for M -- it would have to claim those rows. The error is
    CheckViolation, not the InvalidObjectDefinition you might expect. Maintenance
    must degrade to a warning rather than aborting: the default partition keeps
    accepting writes, so nothing is lost, and every other month still gets
    provisioned.
    """
    stray = datetime(2027, 5, 10, tzinfo=UTC)
    _insert_at(conn, stray, "stray")  # lands in DEFAULT; no 2027_05 partition yet

    created = retention.ensure_partitions(conn, around=stray, months_ahead=3)

    assert "events_2027_05" not in created, "should not have claimed the blocked month"
    assert "events_2027_06" in created, "other months must still be provisioned"
    # And the stray event is still there.
    assert conn.execute(
        "select count(*) c from events where job_name = 'stray'"
    ).fetchone()["c"] == 1
