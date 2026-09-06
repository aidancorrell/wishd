"""Replay tests.

The promise: `events` is the source of truth and everything else is a
projection. If replay does not reproduce the projection exactly, that promise is
false and correlator changes become one-way doors.
"""

from __future__ import annotations

from datetime import UTC, datetime

from dataspine import queries
from dataspine.events import RunEvent
from dataspine.ingest import ingest_run_event
from dataspine.replay import replay
from dataspine.simulate import build_pipeline, shuffle_events

T0 = datetime(2026, 8, 6, 2, 0, 0, tzinfo=UTC)


def _ingest_all(conn, fail_model="fct_order_items"):
    for event in shuffle_events(build_pipeline(fail_model=fail_model, start=T0), seed=5):
        ingest_run_event(conn, RunEvent.model_validate(event))


def _projection(conn):
    return {
        "runs": sorted(
            (str(r["run_id"]), r["job_name"], r["state"], r["depth"], str(r["root_run_id"]))
            for r in conn.execute("select * from run_summary").fetchall()
        ),
        "datasets": sorted(
            (r["namespace"], r["name"]) for r in conn.execute("select * from datasets").fetchall()
        ),
        "edges": conn.execute("select count(*) c from run_datasets").fetchone()["c"],
    }


def test_replay_reproduces_the_projection_exactly(conn):
    _ingest_all(conn)
    before = _projection(conn)
    events_before = conn.execute("select count(*) c from events").fetchone()["c"]

    stats = replay(conn)

    assert stats["ingested"] == events_before
    assert _projection(conn) == before


def test_replay_does_not_grow_the_archive(conn):
    """Replay reads the archive. If it also wrote to it, every rebuild would
    double the source of truth and the second replay would be garbage."""
    _ingest_all(conn)
    before = conn.execute("select count(*) c from events").fetchone()["c"]

    replay(conn)
    replay(conn)

    assert conn.execute("select count(*) c from events").fetchone()["c"] == before


def test_replay_repairs_a_projection_corrupted_by_hand(conn):
    """Stands in for the real case: correlator logic was wrong for a week, or a
    producer was misconfigured. The archive is intact, so the fix is a rebuild."""
    _ingest_all(conn)
    expected = _projection(conn)

    conn.execute("update runs set state = 'UNKNOWN', depth = 99, root_run_id = run_id")
    conn.execute("delete from run_datasets")
    assert _projection(conn) != expected

    replay(conn)
    assert _projection(conn) == expected


def test_replay_counts_events_that_were_never_valid(conn):
    """The gateway archives whatever arrives, including junk. Replay must skip
    it and say how much it skipped rather than failing the whole rebuild."""
    _ingest_all(conn)
    conn.execute(
        """
        insert into events (event_time, event_kind, event_type, payload)
        values (now(), 'RUN', 'START', '{"not": "an event"}'::jsonb)
        """
    )

    stats = replay(conn)
    assert stats["invalid"] == 1
    assert stats["ingested"] == stats["events"] - 1
    # And the good data still rebuilt.
    assert queries.ingest_health(conn)["unstitched_runs"] == 0


def test_windowed_replay_refuses_to_truncate(conn):
    """Truncating everything and then replaying only a window would silently
    delete history. Make that combination impossible rather than documented."""
    _ingest_all(conn)
    try:
        replay(conn, since=T0, truncate=True)
    except ValueError as exc:
        assert "truncate" in str(exc)
    else:
        raise AssertionError("expected ValueError")
