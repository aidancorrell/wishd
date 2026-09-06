"""Load and query-latency floors.

The roadmap target is 10k events/min sustained (~167/s). That number is not
arbitrary: a single busy Spark application emits hundreds of events per run, and
a fleet of a few hundred concurrent applications lands roughly there.

These assert **floors, not benchmarks**. The numbers are set well below what the
machine actually does, because a test that fails when CI is busy teaches people
to ignore failures. The job here is to catch an order-of-magnitude regression --
an accidental N+1, a dropped index, a query that starts seq-scanning at scale --
not to measure performance precisely.

Run the real numbers with:  pytest tests/test_load.py -s -m load
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

import pytest

from dataspine import queries
from dataspine.events import RunEvent
from dataspine.ingest import ingest_run_event
from dataspine.simulate import build_pipeline

pytestmark = pytest.mark.load

TARGET_EVENTS_PER_SEC = 167  # 10k/min


def _load_pipelines(conn, count: int, models=("a", "b", "c")) -> int:
    base = datetime(2026, 8, 1, tzinfo=UTC)
    total = 0
    for i in range(count):
        for payload in build_pipeline(
            models=models, fail_model=None, start=base + timedelta(hours=i)
        ):
            ingest_run_event(conn, RunEvent.model_validate(payload))
            total += 1
    return total


def test_sustained_ingest_throughput(conn, capsys):
    """Single-connection ingest must clear the target with headroom.

    Measured on one connection deliberately: the gateway runs several workers, so
    if one connection alone clears the bar, the fleet target is comfortable.
    """
    conn.execute("set synchronous_commit = off")  # matches a sane prod config

    start = time.monotonic()
    total = _load_pipelines(conn, count=40)
    elapsed = time.monotonic() - start
    rate = total / elapsed

    with capsys.disabled():
        print(f"\n  ingest: {total} events in {elapsed:.2f}s = {rate:.0f} events/s")

    assert rate > TARGET_EVENTS_PER_SEC, (
        f"ingest at {rate:.0f} events/s is below the {TARGET_EVENTS_PER_SEC}/s target"
    )


def test_run_tree_query_stays_fast_at_scale(conn, capsys):
    """Tree traversal is the hot read: the UI runs it on every run page.

    A recursive CTE over an unindexed parent column degrades quietly as the table
    grows, which is exactly the kind of regression this catches.
    """
    _load_pipelines(conn, count=40)
    conn.execute("analyze runs")
    conn.execute("analyze jobs")

    root = queries.list_runs(conn, roots_only=True, limit=1)[0]["run_id"]

    # Warm, then measure.
    queries.run_tree(conn, root)
    start = time.monotonic()
    for _ in range(20):
        nodes = queries.run_tree(conn, root)
    elapsed_ms = (time.monotonic() - start) / 20 * 1000

    total_runs = conn.execute("select count(*) c from runs").fetchone()["c"]
    with capsys.disabled():
        print(f"  run_tree: {elapsed_ms:.1f}ms over {total_runs} runs ({len(nodes)} nodes)")

    assert elapsed_ms < 100, f"run_tree took {elapsed_ms:.1f}ms; expected well under 100ms"


def test_run_list_query_stays_fast_at_scale(conn, capsys):
    """The landing page. Ordered by started_at desc with a limit -- must use the
    index rather than sorting the whole table."""
    _load_pipelines(conn, count=40)
    conn.execute("analyze runs")

    start = time.monotonic()
    for _ in range(20):
        queries.list_runs(conn, limit=50)
    elapsed_ms = (time.monotonic() - start) / 20 * 1000

    with capsys.disabled():
        print(f"  list_runs: {elapsed_ms:.1f}ms")
    assert elapsed_ms < 100, f"list_runs took {elapsed_ms:.1f}ms"


def test_hot_queries_use_indexes_not_seq_scans(conn, capsys):
    """Guards the indexes themselves.

    Timing thresholds alone would not catch a dropped index at test-table sizes
    -- Postgres seq-scans small tables happily and quickly. Asserting on the plan
    catches it at any size.
    """
    _load_pipelines(conn, count=25)
    conn.execute("analyze runs")
    conn.execute("analyze jobs")
    conn.execute("analyze run_datasets")

    root = queries.list_runs(conn, roots_only=True, limit=1)[0]["run_id"]

    plans = {
        "job history": (
            "select * from runs where job_id = 1 order by started_at desc limit 50",
            (),
        ),
        "children lookup": ("select * from runs where parent_run_id = %s", (root,)),
        "run by id": ("select * from runs where run_id = %s", (root,)),
        "dataset io": ("select * from run_datasets where run_id = %s", (root,)),
    }

    report = []
    for label, (sql, params) in plans.items():
        plan = "\n".join(
            r["QUERY PLAN"] for r in conn.execute(f"explain {sql}", params).fetchall()
        )
        seq = "Seq Scan" in plan
        report.append(f"    {label:18} {'SEQ SCAN' if seq else 'index'}")
        assert not seq, f"{label} is doing a sequential scan:\n{plan}"

    with capsys.disabled():
        print("  query plans:\n" + "\n".join(report))


def test_ingest_does_not_degrade_as_the_table_grows(conn, capsys):
    """Throughput must stay roughly flat, not fall off a cliff.

    A per-event cost that scales with table size is the signature of a missing
    index; comparing an early batch against a later one surfaces it without
    needing a huge dataset.
    """
    conn.execute("set synchronous_commit = off")

    start = time.monotonic()
    first = _load_pipelines(conn, count=15)
    first_rate = first / (time.monotonic() - start)

    _load_pipelines(conn, count=30)  # grow the table

    start = time.monotonic()
    later = _load_pipelines(conn, count=15)
    later_rate = later / (time.monotonic() - start)

    with capsys.disabled():
        print(f"  ingest early: {first_rate:.0f}/s   after growth: {later_rate:.0f}/s")

    assert later_rate > first_rate * 0.5, (
        f"ingest halved as the table grew ({first_rate:.0f} -> {later_rate:.0f}/s); "
        "suspect a missing index"
    )


def test_ingest_health_stays_fast_at_scale(conn, capsys):
    """`ingest_health` renders on the run-list landing page, so it is on the hot
    path for every UI visit.

    The original `unstitched_runs` view was a NOT EXISTS anti-join that scanned
    every run with a parent -- O(all runs), and the single slowest thing in the
    system at 7k runs. Since ingest always creates a placeholder row for a known
    parent, "unstitched" is equivalent to "my parent row is still a placeholder",
    which can be driven from the (tiny) placeholder set instead.
    """
    _load_pipelines(conn, count=60)
    conn.execute("analyze runs")
    conn.execute("analyze jobs")
    conn.execute("analyze events")

    queries.ingest_health(conn)
    start = time.monotonic()
    for _ in range(10):
        queries.ingest_health(conn)
    elapsed_ms = (time.monotonic() - start) / 10 * 1000

    total_runs = conn.execute("select count(*) c from runs").fetchone()["c"]
    with capsys.disabled():
        print(f"  ingest_health: {elapsed_ms:.1f}ms over {total_runs} runs")

    assert elapsed_ms < 5, f"ingest_health took {elapsed_ms:.1f}ms; it is on every page load"


def test_unstitched_view_does_not_scan_every_run(conn, capsys):
    """Plan-level guard for the same thing. Timing alone would not catch the
    anti-join coming back at small table sizes."""
    _load_pipelines(conn, count=40)
    conn.execute("analyze runs")

    plan = "\n".join(
        r["QUERY PLAN"]
        for r in conn.execute("explain select count(*) from unstitched_runs").fetchall()
    )
    with capsys.disabled():
        print("  unstitched plan: " + plan.splitlines()[1].strip())

    assert "Seq Scan on runs" not in plan, f"unstitched_runs scans all runs:\n{plan}"


def test_every_parent_reference_resolves(conn):
    """The invariant the fast `unstitched_runs` depends on.

    Ingest calls _ensure_placeholder for every parent it hears about, so a
    dangling parent_run_id should be impossible. If that ever stops being true,
    the placeholder-driven view would silently under-report unstitched runs --
    so assert the invariant directly rather than trusting it.
    """
    _load_pipelines(conn, count=10)
    dangling = conn.execute(
        """
        select count(*) c from runs r
        where r.parent_run_id is not null
          and not exists (select 1 from runs p where p.run_id = r.parent_run_id)
        """
    ).fetchone()["c"]
    assert dangling == 0, f"{dangling} runs reference a parent row that does not exist"
