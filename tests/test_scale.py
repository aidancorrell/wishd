"""Query latency at a million runs.

The roadmap's Phase 06 benchmark item. `test_load.py` already guards throughput
and query plans at a few thousand runs; this asks the different question of
whether the read side still works when the archive is the size it will be after
a year of a real fleet.

**A million runs is not hypothetical.** A single nightly DAG with 40 dbt models,
each spawning a Spark application with a handful of SQL executions, is a few
hundred runs a night. A hundred such pipelines is ~10⁶ runs inside a year — and
that is a mid-sized platform team, not a hyperscaler.

Rows are inserted directly rather than pushed through the correlator, on purpose.
Ingest throughput is `test_load.py`'s job; what is unproven at this size is
whether the *queries* hold up, and paying an hour of ingest to find out would
mean nobody ever ran this.

These are floors, generously set, for the same reason as the load suite: a test
that fails when the machine is busy teaches people to ignore failures. What they
catch is an order-of-magnitude regression — a dropped index, a plan that flips
to a sequential scan somewhere between ten thousand rows and a million.

    pytest tests/test_scale.py -s -m scale
"""

from __future__ import annotations

import time
from datetime import UTC, datetime

import pytest

from dataspine import catalog, identity, lineage, queries

pytestmark = pytest.mark.scale

RUNS = 1_000_000
TREES = 20_000          # runs are grouped into pipeline executions
DATASETS = 5_000

# Generous ceilings. On the development laptop these come in far under; the
# point is to notice a seq scan, not to certify a number.
TREE_MS = 250
LIST_MS = 400
HEALTH_MS = 1500
CATALOG_MS = 1500


@pytest.fixture(scope="module")
def million(database_url):
    """A million runs in ~20k trees, generated in Postgres rather than Python.

    `generate_series` builds the whole archive server-side: a million
    round-trips from Python would take longer than the benchmark is worth, and
    would be measuring psycopg rather than the schema.
    """
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        existing = conn.execute("select count(*) as n from runs").fetchone()["n"]
        if existing < RUNS:
            _generate(conn)
            conn.commit()
        yield conn
        # Left in place for the session: regenerating for each test would
        # dominate the runtime of the suite.


def _generate(conn) -> None:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    print(f"\n  generating {RUNS:,} runs...", flush=True)
    started = time.perf_counter()

    conn.execute(
        """
        insert into jobs (namespace, name, integration, job_type)
        select 'bench', 'job_' || g,
               (array['AIRFLOW','DBT','SPARK'])[1 + g % 3],
               (array['DAG','MODEL','SQL_JOB'])[1 + g % 3]
        from generate_series(1, 500) g
        on conflict (namespace, name) do nothing
        """
    )
    conn.execute(
        """
        insert into datasets (namespace, name)
        select 'bench://lake', 'schema_' || (g %% 50) || '.table_' || g
        from generate_series(1, %s) g
        on conflict (namespace, name) do nothing
        """,
        (DATASETS,),
    )

    # Roots first, so children can reference them.
    conn.execute(
        """
        insert into runs (run_id, job_id, root_run_id, state, started_at, ended_at, depth)
        select gen_random_uuid(),
               (select id from jobs where namespace='bench' order by id limit 1),
               null, 'COMPLETED', %s + (g || ' minutes')::interval,
               %s + (g || ' minutes')::interval + interval '5 minutes', 0
        from generate_series(1, %s) g
        """,
        (base, base, TREES),
    )
    conn.execute("update runs set root_run_id = run_id where root_run_id is null")

    # Then the bulk, distributed across those trees.
    per_tree = RUNS // TREES
    print(f"  {TREES:,} trees x {per_tree} runs each", flush=True)
    conn.execute(
        """
        with roots as (
            select run_id, started_at, row_number() over (order by started_at) as n
            from runs where parent_run_id is null
        )
        insert into runs (run_id, job_id, parent_run_id, root_run_id, depth,
                          state, started_at, ended_at)
        select gen_random_uuid(),
               (select id from jobs where namespace='bench'
                 order by id offset (g %% 500) limit 1),
               r.run_id, r.run_id, 1 + (g %% 3),
               case when g %% 17 = 0 then 'FAILED' else 'COMPLETED' end,
               r.started_at + ((g %% 300) || ' seconds')::interval,
               r.started_at + ((g %% 300) + 30 || ' seconds')::interval
        from roots r
        cross join generate_series(1, %s) g
        """,
        (per_tree,),
    )

    # Dataset edges on a sample: enough to make the catalog and lineage queries
    # do real work without another million rows.
    conn.execute(
        """
        insert into run_datasets (run_id, dataset_id, direction, row_count)
        select r.run_id, d.id, dir.direction, 1000 + r.depth
        from (select run_id, depth from runs where depth > 0 limit 200000) r
        cross join (values ('INPUT'), ('OUTPUT')) as dir(direction)
        join datasets d
          on d.id = 1 + ((
                ('x' || substr(md5(r.run_id::text || dir.direction), 1, 8))
                ::bit(32)::bigint
             ) %% %s)
        on conflict do nothing
        """,
        (DATASETS,),
    )
    conn.execute("analyze runs, jobs, datasets, run_datasets")

    elapsed = time.perf_counter() - started
    total = conn.execute("select count(*) as n from runs").fetchone()["n"]
    print(f"  generated {total:,} runs in {elapsed:.0f}s", flush=True)


def _timed(label: str, fn) -> float:
    start = time.perf_counter()
    result = fn()
    elapsed = (time.perf_counter() - start) * 1000
    size = len(result) if hasattr(result, "__len__") else "-"
    print(f"  {label:<28} {elapsed:8.1f}ms   rows={size}", flush=True)
    return elapsed


# --------------------------------------------------------------------- reads


def test_the_archive_really_is_a_million_runs(million):
    total = million.execute("select count(*) as n from runs").fetchone()["n"]
    print(f"\n  runs: {total:,}")
    assert total >= RUNS


def test_run_tree_stays_fast(million):
    """The Phase 00 payoff query. If anything degrades at scale it is this, and
    it is the one people hit from a link in an alert."""
    root = million.execute(
        "select run_id from runs where parent_run_id is null limit 1"
    ).fetchone()["run_id"]

    _timed("run_tree (warm-up)", lambda: queries.run_tree(million, root))
    elapsed = _timed("run_tree", lambda: queries.run_tree(million, root))
    assert elapsed < TREE_MS


def test_list_runs_stays_fast(million):
    """The landing page. Ordered by started_at desc, which is exactly the shape
    that goes quadratic if the index is dropped."""
    elapsed = _timed("list_runs", lambda: queries.list_runs(million, limit=50))
    assert elapsed < LIST_MS


def test_filtered_list_runs_stays_fast(million):
    rows = queries.list_runs(million, state="FAILED", limit=50)
    elapsed = _timed(
        "list_runs (failed)",
        lambda: queries.list_runs(million, state="FAILED", limit=50),
    )
    # An empty result would measure an index probe that finds nothing, which is
    # not the query anyone runs when something is broken.
    assert rows, "the generator should produce failed runs to filter for"
    assert elapsed < LIST_MS


def test_ingest_health_stays_fast(million):
    """`unstitched_runs` was an O(all runs) anti-join once, and was the slowest
    thing in the system. Migration 004 fixed it; this is the guard that it stays
    fixed at a size where the old form would be unusable."""
    elapsed = _timed("ingest_health", lambda: [queries.ingest_health(million)])
    assert elapsed < HEALTH_MS


def test_the_correlation_alarm_stays_fast(million):
    elapsed = _timed(
        "unstitched_runs",
        lambda: million.execute("select count(*) as n from unstitched_runs").fetchall(),
    )
    assert elapsed < HEALTH_MS


# ------------------------------------------------------- phase 04/05 read side


def test_catalog_listing_stays_fast(million):
    """The catalog counts upstream, downstream and monitors per entity. Those are
    correlated subqueries, which is the shape most likely to bite at scale."""
    identity.resolve(million)
    catalog.reindex(million)
    elapsed = _timed("catalog listing", lambda: catalog.list_entries(million, limit=100))
    assert elapsed < CATALOG_MS


def test_search_stays_fast(million):
    elapsed = _timed("catalog search", lambda: catalog.search(million, "table_42"))
    assert elapsed < CATALOG_MS


def test_lineage_traversal_stays_fast(million):
    lineage.resolve(million)
    entity = million.execute(
        """
        select upstream_id as id from lineage_edges
        group by upstream_id order by count(*) desc limit 1
        """
    ).fetchone()
    if entity is None:
        pytest.skip("no lineage edges resolved from the benchmark data")
    elapsed = _timed(
        "downstream walk (depth 5)",
        lambda: lineage.downstream(million, entity["id"], depth=5),
    )
    assert elapsed < CATALOG_MS


# --------------------------------------------------------------------- plans


def test_the_tree_query_still_uses_an_index_at_scale(million):
    """A plan guard, not a timing one.

    Timings drift with the machine; a sequential scan over a million runs is a
    fact about the schema. This is what catches a dropped index on a laptop fast
    enough that the clock never notices.
    """
    root = million.execute(
        "select run_id from runs where parent_run_id is null limit 1"
    ).fetchone()["run_id"]
    plan = "\n".join(
        row["QUERY PLAN"]
        for row in million.execute(
            "explain (format text) "
            "select run_id from runs where parent_run_id = %s", (root,)
        ).fetchall()
    )
    print(f"\n  plan: {plan.splitlines()[0].strip()}")
    assert "Seq Scan" not in plan, plan
