"""A table's whole write history, from one metadata read.

Captured 2026-08-31: the same dbt project run three times against a real Iceberg
REST catalog, so `stg_customers` carries three snapshots with three timestamps
and a row count each. That is a freshness series and a volume series, sitting in
a file we were already reading and throwing away.

**Why this is worth the code.** Phase 03's stated rule is that monitors arm on
day one — `dataspine apply` backfills metric history out of the run archive
rather than waiting a week to learn a baseline it could already compute. Polled
tables had no equivalent: we knew only what we had observed since being
installed, so a freshness or volume monitor on a source table was blind for as
long as it took to accumulate points. For Iceberg there was never a reason to
wait; the table has been keeping the series the whole time.

The rule that makes it correct is `observed_at`. Each row is stamped with the
*snapshot's* timestamp, not the read time. Stamping "now" would collapse a
month of history into one instant and teach every future baseline that the
table changes whenever we happen to poll — the exact mistake
`metric_points.observed_at` was introduced to prevent.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dataspine import sources

FIXTURE = Path(__file__).parent / "fixtures" / "iceberg_metadata_3_snapshots.json"


@pytest.fixture()
def history():
    return sources.read_iceberg_history(
        FIXTURE, namespace="iceberg://rest", name="stg_customers"
    )


def test_the_whole_retained_history_is_returned(history):
    """Three dbt runs, three snapshots. The current-snapshot reader sees one."""
    assert len(history) == 3

    current = sources.read_iceberg_metadata(
        FIXTURE, namespace="iceberg://rest", name="stg_customers"
    )
    assert current is not None
    assert len([current]) == 1, "the point of comparison"


def test_each_snapshot_carries_its_own_timestamp_not_the_read_time(history):
    """The property the whole feature turns on."""
    stamps = [s.last_modified for s in history]
    assert all(stamps), "a snapshot with no timestamp would be undated history"
    assert len(set(stamps)) == 3, "timestamps collapsed — this is a series, not a point"


def test_history_is_ordered_oldest_first(history):
    """`snapshots` is conventionally in commit order and nothing guarantees it.
    A series sorted wrongly is silently nonsense rather than obviously broken."""
    stamps = [s.last_modified for s in history]
    assert stamps == sorted(stamps)


def test_row_counts_come_through_as_a_volume_series(history):
    counts = [s.row_count for s in history]
    assert all(c is not None for c in counts)
    assert all(c >= 0 for c in counts)


def test_the_operation_is_preserved(history):
    """`overwrite` and `append` mean different things to a volume monitor: one
    replaces the count, the other adds to it."""
    for snapshot in history:
        assert snapshot.extra["operation"], "operation is how a reader interprets the delta"
        assert "snapshot_id" in snapshot.extra


def test_only_the_current_snapshot_carries_a_schema(history):
    """Iceberg keeps every schema it has used but does not record which snapshot
    used which. Attaching today's columns to a year-old snapshot would
    manufacture a drift history that never happened."""
    current = [s for s in history if s.extra["is_current"]]
    historical = [s for s in history if not s.extra["is_current"]]

    assert len(current) == 1
    assert current[0].columns, "the current snapshot should know its schema"
    assert all(s.columns is None for s in historical)


def test_history_is_bounded(history):
    """A table compacted hourly for a year has thousands of retained snapshots;
    one read must not become an unbounded write."""
    limited = sources.read_iceberg_history(
        FIXTURE, namespace="ns", name="t", limit=2
    )
    assert len(limited) == 2
    # Newest-biased: recent history is what a baseline needs.
    assert limited[-1].last_modified == history[-1].last_modified


def test_a_snapshotless_table_yields_nothing(tmp_path):
    """A table created but never written. Returning a fabricated point would put
    a zero into someone's volume baseline."""
    empty = tmp_path / "empty.metadata.json"
    empty.write_text(json.dumps({"current-snapshot-id": None, "snapshots": []}))
    assert sources.read_iceberg_history(empty, namespace="ns", name="t") == []


def test_unreadable_metadata_yields_nothing_rather_than_raising(tmp_path):
    missing = tmp_path / "nope.metadata.json"
    assert sources.read_iceberg_history(missing, namespace="ns", name="t") == []


# ------------------------------------------------------------------ storage


def test_polling_backfills_the_series_in_one_pass(conn):
    """The payoff: a monitor declared today, armed with real history."""
    spec = sources.SourceSpec(
        name="marts", type="iceberg", namespace="iceberg://rest",
        dataset="stg_customers", path=str(FIXTURE),
    )
    written = sources.poll(conn, spec)
    assert written == 3, "history was not backfilled"

    rows = conn.execute(
        "select observed_at, row_count from dataset_snapshots "
        "order by observed_at"
    ).fetchall()
    assert len(rows) == 3
    assert rows[0]["observed_at"] < rows[-1]["observed_at"]


def test_re_polling_does_not_duplicate_history(conn):
    """Keyed on (dataset, observed_at, source), so re-reading the same snapshots
    updates rather than multiplying them — the same discipline the CUR importer
    needed once its key turned out to be wrong."""
    spec = sources.SourceSpec(
        name="marts", type="iceberg", namespace="iceberg://rest",
        dataset="stg_customers", path=str(FIXTURE),
    )
    sources.poll(conn, spec)
    sources.poll(conn, spec)

    count = conn.execute(
        "select count(*) c from dataset_snapshots"
    ).fetchone()["c"]
    assert count == 3


def test_history_can_be_switched_off(conn):
    """Current-state-only stays available for anyone who wants one row per poll."""
    spec = sources.SourceSpec(
        name="marts", type="iceberg", namespace="iceberg://rest",
        dataset="stg_customers", path=str(FIXTURE),
    )
    assert sources.poll(conn, spec, history=False) == 1
