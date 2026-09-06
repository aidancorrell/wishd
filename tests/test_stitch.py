"""Re-joining run trees a shared Spark session split apart.

D6 found that on dbt-spark over a shared Thrift Server, dbt and the Spark
listener emit **disjoint run trees** — zero shared root run ids. The correlation
this project is built on silently does not happen on its own reference stack.
D3's first candidate fix (a session-scoped `SET` of
`spark.openlineage.parentRunId`) was tried and does not work: the listener reads
that config once at startup, so every query on a long-lived Thrift Server gets
the *application's* run as parent.

The fix that does work needs no producer change at all, because dbt is already
telling us. **dbt's `query_comment` is on by default and embeds its `node_id` in
every statement it sends:**

    /* {"app": "dbt", "dbt_version": "1.12.0", ...,
        "node_id": "model.analytics.stg_customers"} */
    drop table if exists analytics_marts_marts.stg_customers

The Spark listener captures the full SQL text, comment included. So the dbt job
name is *inside* the SQL Spark reports — an exact, producer-supplied identifier
that maps directly onto a job name we already store. Not a heuristic, and
emphatically not the name-and-time fuzzy merge Phase 04 refused: this is dbt
telling us which node ran, in dbt's own vocabulary.

Stitching runs post-hoc rather than at ingest for the same reason identity
resolution does: arrival order is not ours to control, and the dbt run routinely
lands after the Spark query it explains.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from dataspine import identity, stitch

NOW = datetime(2026, 8, 11, 12, 0, tzinfo=UTC)

DBT_COMMENT = (
    '/* {{"app": "dbt", "dbt_version": "1.12.0", "profile_name": "analytics_spark", '
    '"target_name": "thrift", "node_id": "{node}"}} */\n'
)


def _job(conn, name, integration, *, sql=None) -> int:
    facets = {"sql": {"query": sql}} if sql else {}
    return conn.execute(
        "insert into jobs (namespace, name, integration, facets) "
        "values (%s, %s, %s, %s) "
        "on conflict (namespace, name) do update set facets = excluded.facets "
        "returning id",
        (f"{integration.lower()}://ns", name, integration, json.dumps(facets)),
    ).fetchone()["id"]


def _run(conn, job_id, *, start, length=60, parent=None, root=None):
    run_id = uuid4()
    conn.execute(
        "insert into runs (run_id, job_id, parent_run_id, root_run_id, state, "
        "started_at, ended_at) values (%s,%s,%s,%s,'COMPLETED',%s,%s)",
        (run_id, job_id, parent, root or parent or run_id, start,
         start + timedelta(seconds=length)),
    )
    return run_id


def _dbt_model(conn, node, *, start, length=60):
    """A dbt model run, as openlineage-dbt reports it."""
    return _run(conn, _job(conn, node, "DBT"), start=start, length=length)


def _spark_query(conn, name, node, *, start, app_run=None, length=10):
    """A Spark SQL run whose captured SQL carries dbt's query comment."""
    sql = DBT_COMMENT.format(node=node) + f"create table {name} as select 1"
    job = _job(conn, f"thrift.{name}", "SPARK", sql=sql)
    return _run(conn, job, start=start, length=length, parent=app_run,
                root=app_run)


@pytest.fixture()
def thrift_split(conn):
    """The real shape: a dbt tree and a Thrift Server tree, disjoint."""
    app_job = _job(conn, "dataspine_thrift", "SPARK")
    app_run = _run(conn, app_job, start=NOW - timedelta(hours=2), length=7200)

    model_run = _dbt_model(conn, "model.analytics.stg_orders", start=NOW, length=60)
    query_run = _spark_query(
        conn, "stg_orders", "model.analytics.stg_orders",
        start=NOW + timedelta(seconds=5), app_run=app_run,
    )
    return {"app": app_run, "model": model_run, "query": query_run}


# ------------------------------------------------------------------ stitching


def test_a_spark_query_is_reparented_to_the_dbt_node_that_issued_it(conn, thrift_split):
    stitched = stitch.stitch_query_comments(conn)
    assert stitched == 1

    row = conn.execute(
        "select parent_run_id, root_run_id from runs where run_id = %s",
        (thrift_split["query"],),
    ).fetchone()
    assert row["parent_run_id"] == thrift_split["model"]


def test_stitching_puts_both_producers_in_one_tree(conn, thrift_split):
    """The whole point. Before: two roots. After: one."""
    before = conn.execute(
        "select count(distinct coalesce(root_run_id, run_id)) as n from runs"
    ).fetchone()["n"]
    stitch.stitch_query_comments(conn)
    after = conn.execute(
        "select root_run_id from runs where run_id = %s", (thrift_split["query"],)
    ).fetchone()["root_run_id"]

    assert before == 2
    assert after == thrift_split["model"]


def test_stitching_lets_identity_resolution_merge_the_table(conn):
    """The payoff, end to end.

    Without a shared tree, dbt's `analytics.fct_orders` and Spark's
    `/warehouse/fct_orders` are two entities and the lineage graph splits in
    half. Co-write evidence needs the tree that stitching restores.
    """
    app_run = _run(conn, _job(conn, "dataspine_thrift", "SPARK"),
                   start=NOW - timedelta(hours=1), length=3600)
    model_run = _dbt_model(conn, "model.analytics.fct_orders", start=NOW)
    query_run = _spark_query(conn, "fct_orders", "model.analytics.fct_orders",
                             start=NOW + timedelta(seconds=5), app_run=app_run)

    def dataset(namespace, name):
        return conn.execute(
            "insert into datasets (namespace, name) values (%s,%s) returning id",
            (namespace, name),
        ).fetchone()["id"]

    for run, ns, name in (
        (model_run, "spark://thrift:10000", "analytics_marts.fct_orders"),
        (query_run, "file", "/warehouse/fct_orders"),
    ):
        conn.execute(
            "insert into run_datasets (run_id, dataset_id, direction) "
            "values (%s,%s,'OUTPUT')",
            (run, dataset(ns, name)),
        )

    identity.resolve(conn)
    assert len([e for e in identity.entities(conn) if e["name"] == "fct_orders"]) == 2

    stitch.stitch_query_comments(conn)
    identity.resolve(conn)

    merged = [e for e in identity.entities(conn) if e["name"] == "fct_orders"]
    assert len(merged) == 1
    assert len(merged[0]["datasets"]) == 2


# --------------------------------------------------------------- refusals


def test_a_run_already_in_the_dbt_tree_is_left_alone(conn):
    """Where propagation genuinely works — a `spark-submit` with an explicit
    parent run id — stitching must not touch it. Overriding real correlation
    with inferred correlation would be a downgrade."""
    model_run = _dbt_model(conn, "model.analytics.fct_orders", start=NOW)
    query_run = _spark_query(conn, "fct_orders", "model.analytics.fct_orders",
                             start=NOW + timedelta(seconds=5),
                             app_run=model_run)

    assert stitch.stitch_query_comments(conn) == 0
    row = conn.execute(
        "select parent_run_id from runs where run_id = %s", (query_run,)
    ).fetchone()
    assert row["parent_run_id"] == model_run


def test_a_node_id_naming_no_known_run_is_not_stitched(conn):
    """dbt's run may not have arrived, or may never arrive. Inventing a parent
    would be worse than leaving the split visible."""
    app_run = _run(conn, _job(conn, "dataspine_thrift", "SPARK"), start=NOW)
    _spark_query(conn, "orphan", "model.analytics.never_seen",
                 start=NOW + timedelta(seconds=5), app_run=app_run)

    assert stitch.stitch_query_comments(conn) == 0


def test_a_dbt_run_from_a_different_time_is_not_stitched(conn):
    """The same model runs nightly. A query must join the invocation that issued
    it, not last Tuesday's — otherwise cost and lineage land on the wrong run."""
    app_run = _run(conn, _job(conn, "dataspine_thrift", "SPARK"),
                   start=NOW - timedelta(days=7), length=10)
    _dbt_model(conn, "model.analytics.fct_orders", start=NOW - timedelta(days=7))
    _spark_query(conn, "fct_orders", "model.analytics.fct_orders",
                 start=NOW, app_run=app_run)

    assert stitch.stitch_query_comments(conn) == 0


def test_sql_without_a_dbt_comment_is_ignored(conn):
    """Hand-written Spark jobs and non-dbt SQL have no node id and must not be
    guessed at."""
    app_run = _run(conn, _job(conn, "dataspine_thrift", "SPARK"), start=NOW)
    job = _job(conn, "thrift.adhoc", "SPARK", sql="select 1 from somewhere")
    _run(conn, job, start=NOW + timedelta(seconds=5), parent=app_run, root=app_run)

    assert stitch.stitch_query_comments(conn) == 0


def test_a_malformed_comment_does_not_raise(conn):
    app_run = _run(conn, _job(conn, "dataspine_thrift", "SPARK"), start=NOW)
    job = _job(conn, "thrift.broken", "SPARK",
               sql='/* {"app": "dbt", "node_id": broken json */ select 1')
    _run(conn, job, start=NOW + timedelta(seconds=5), parent=app_run, root=app_run)

    assert stitch.stitch_query_comments(conn) == 0


def test_stitching_is_idempotent(conn, thrift_split):
    stitch.stitch_query_comments(conn)
    for _ in range(3):
        assert stitch.stitch_query_comments(conn) == 0


# ------------------------------------------------------------------ parsing


def test_node_id_is_extracted_from_a_real_dbt_comment():
    sql = (
        '/* {"app": "dbt", "dbt_version": "1.12.0", "profile_name": '
        '"analytics_spark", "target_name": "thrift", "node_id": '
        '"model.analytics.stg_customers"} */\n'
        "drop table if exists analytics_marts_marts.stg_customers"
    )
    assert stitch.dbt_node_id(sql) == "model.analytics.stg_customers"


def test_only_dbt_comments_count():
    """A comment containing `node_id` that is not dbt's must not be trusted."""
    assert stitch.dbt_node_id('/* {"node_id": "something"} */ select 1') is None


@pytest.mark.parametrize("sql", ["", None, "select 1", "/* not json */ select 1"])
def test_absent_or_unparseable_comments_yield_nothing(sql):
    assert stitch.dbt_node_id(sql) is None


def test_extraction_works_on_the_real_capture():
    """Against the verbatim dbt-spark-over-thrift capture, not a constructed
    string."""
    from pathlib import Path

    events = json.loads(
        (Path(__file__).parent / "fixtures" / "dbt_spark_thrift_1.52.0.json").read_text()
    )
    nodes = set()
    for event in events:
        if "spark" not in (event.get("producer") or ""):
            continue
        query = ((event.get("job") or {}).get("facets") or {}).get("sql", {}).get("query")
        node = stitch.dbt_node_id(query)
        if node:
            nodes.add(node)

    assert nodes == {
        "model.analytics.fct_orders",
        "model.analytics.stg_orders",
        "model.analytics.stg_customers",
    }


# ------------------------------------------------------------- the new alarm


def test_disjoint_trees_are_detectable_where_unstitched_runs_is_blind(conn):
    """The alarm `unstitched_runs` structurally cannot raise.

    That view fires on a *dangling* parent — a run naming a parent nobody sent.
    A shared Thrift Server produces something different and worse: two
    internally consistent trees that never touch. Nothing dangles, so the Phase
    00 alarm reads zero while the lineage graph is split in half. This looks for
    the symptom instead.
    """
    def dataset(namespace, name):
        return conn.execute(
            "insert into datasets (namespace, name) values (%s,%s) "
            "on conflict (namespace, name) do update set updated_at = now() returning id",
            (namespace, name),
        ).fetchone()["id"]

    def write(run, dataset_id):
        conn.execute(
            "insert into run_datasets (run_id, dataset_id, direction) "
            "values (%s,%s,'OUTPUT') on conflict do nothing",
            (run, dataset_id),
        )

    dbt_run = _run(conn, _job(conn, "model.analytics.fct_orders", "DBT"), start=NOW)
    spark_run = _run(conn, _job(conn, "thrift.fct_orders", "SPARK"), start=NOW)
    write(dbt_run, dataset("spark://thrift:10000", "analytics.fct_orders"))
    write(spark_run, dataset("file", "/warehouse/fct_orders"))

    assert conn.execute("select count(*) as n from unstitched_runs").fetchone()["n"] == 0, (
        "nothing dangles — which is exactly why the old alarm cannot see this"
    )

    split = stitch.disjoint_trees(conn)
    assert [row["name"] for row in split] == ["fct_orders"]
    assert set(split[0]["integrations"]) == {"DBT", "SPARK"}


def test_one_producer_writing_two_tables_is_not_an_alarm(conn):
    """Two trees writing the same name from the *same* integration is ordinary —
    a nightly job running twice. The alarm needs both a split and a
    cross-producer disagreement, or it fires on every healthy schedule."""
    job = _job(conn, "model.analytics.fct_orders", "DBT")
    dataset_id = conn.execute(
        "insert into datasets (namespace, name) values ('db','analytics.fct_orders') "
        "returning id"
    ).fetchone()["id"]
    for day in (1, 2):
        run = _run(conn, job, start=NOW - timedelta(days=day))
        conn.execute(
            "insert into run_datasets (run_id, dataset_id, direction) "
            "values (%s,%s,'OUTPUT')",
            (run, dataset_id),
        )

    assert stitch.disjoint_trees(conn) == []


def test_stitching_clears_the_alarm(conn, thrift_split):
    """The two halves fit together: the alarm names the problem, stitching fixes
    it, and the alarm goes quiet."""
    def dataset(namespace, name):
        return conn.execute(
            "insert into datasets (namespace, name) values (%s,%s) returning id",
            (namespace, name),
        ).fetchone()["id"]

    for run, ns, name in (
        (thrift_split["model"], "spark://thrift:10000", "analytics.stg_orders"),
        (thrift_split["query"], "file", "/warehouse/stg_orders"),
    ):
        conn.execute(
            "insert into run_datasets (run_id, dataset_id, direction) values (%s,%s,'OUTPUT')",
            (run, dataset(ns, name)),
        )

    assert [r["name"] for r in stitch.disjoint_trees(conn)] == ["stg_orders"]
    stitch.stitch_query_comments(conn)
    assert stitch.disjoint_trees(conn) == []
