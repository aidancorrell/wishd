"""Correlator tests.

These encode the promises the rest of the system rests on:
  1. arrival order does not matter
  2. ingesting the same event twice changes nothing
  3. run state only moves forward
  4. a run whose parent never arrives is visible, not silently dropped
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from dataspine import queries
from dataspine.events import RunEvent
from dataspine.ingest import ingest_run_event
from dataspine.simulate import build_pipeline, shuffle_events

T0 = datetime(2026, 8, 6, 2, 0, 0, tzinfo=UTC)


def ingest(conn, event: dict[str, Any]):
    return ingest_run_event(conn, RunEvent.model_validate(event))


def make_event(
    event_type: str,
    run_id: UUID,
    name: str,
    *,
    parent: tuple[UUID, str] | None = None,
    root: tuple[UUID, str] | None = None,
    at: datetime = T0,
    namespace: str = "test",
) -> dict[str, Any]:
    facets: dict[str, Any] = {}
    if parent:
        parent_run, parent_name = parent
        block: dict[str, Any] = {
            "_producer": "test",
            "_schemaURL": "test",
            "run": {"runId": str(parent_run)},
            "job": {"namespace": namespace, "name": parent_name},
        }
        if root:
            root_run, root_name = root
            block["root"] = {
                "run": {"runId": str(root_run)},
                "job": {"namespace": namespace, "name": root_name},
            }
        facets["parent"] = block
    return {
        "eventTime": at.isoformat(),
        "producer": "test",
        "schemaURL": "test",
        "eventType": event_type,
        "run": {"runId": str(run_id), "facets": facets},
        "job": {"namespace": namespace, "name": name, "facets": {}},
    }


# ------------------------------------------------------------------- ordering


def test_child_before_parent_still_stitches(conn):
    """The child arrives first. This is the normal case, not the edge case:
    a Spark listener flushes on its own schedule and routinely beats the
    Airflow task event that logically precedes it."""
    dag, task = uuid4(), uuid4()

    ingest(conn, make_event("START", task, "dag.task", parent=(dag, "dag")))

    # The parent is known only through the child's facet -> placeholder.
    row = conn.execute("select * from runs where run_id = %s", (dag,)).fetchone()
    assert row is not None
    assert row["is_placeholder"] is True

    ingest(conn, make_event("START", dag, "dag", at=T0 - timedelta(seconds=5)))

    row = conn.execute("select * from runs where run_id = %s", (dag,)).fetchone()
    assert row["is_placeholder"] is False
    assert row["state"] == "RUNNING"

    tree = queries.run_tree(conn, dag)
    assert [n["job_name"] for n in tree] == ["dag", "dag.task"]
    assert [n["level"] for n in tree] == [0, 1]


def test_deep_chain_repairs_when_middle_arrives_last(conn):
    """A → B → C where B lands last. Until B arrives, C's depth is a guess;
    once B lands, the subtree repair has to correct it."""
    a, b, c = uuid4(), uuid4(), uuid4()

    ingest(conn, make_event("START", c, "c", parent=(b, "b")))
    ingest(conn, make_event("START", a, "a"))
    ingest(conn, make_event("START", b, "b", parent=(a, "a")))

    rows = {
        r["job_name"]: r
        for r in conn.execute(
            "select j.name as job_name, r.* from runs r join jobs j on j.id = r.job_id"
        ).fetchall()
    }
    assert rows["a"]["depth"] == 0
    assert rows["b"]["depth"] == 1
    assert rows["c"]["depth"] == 2, "descendant depth was not repaired after B arrived"
    assert rows["c"]["root_run_id"] == a
    assert rows["b"]["root_run_id"] == a


def test_full_pipeline_is_order_independent(conn):
    """The real test: ingest a 5-level pipeline shuffled, and assert the tree
    is identical to the in-order ingest."""
    events = build_pipeline(fail_model=None, start=T0)

    for event in shuffle_events(events, seed=7):
        ingest(conn, event)

    roots = queries.list_runs(conn, roots_only=True, limit=10)
    assert len(roots) == 1
    tree = queries.run_tree(conn, roots[0]["run_id"])

    levels = {n["job_name"]: n["level"] for n in tree}
    assert levels["analytics_daily"] == 0
    assert levels["analytics_daily.dbt_run_marts"] == 1
    # Names verified against openlineage-dbt 1.52.0, not invented.
    assert levels["dbt-run-analytics"] == 2
    assert levels["model.analytics.fct_orders"] == 3
    assert levels["dbt_spark_analytics.fct_orders"] == 4
    # Naming pattern verified against openlineage-spark 1.52.0.
    assert (
        levels["dbt_spark_analytics.execute_insert_into_hadoop_fs_relation_command"
               ".warehouse_fct_orders"]
        == 5
    )
    assert all(n["is_placeholder"] is False for n in tree)


# ---------------------------------------------------------------- idempotency


def test_double_ingest_is_a_no_op(conn):
    events = build_pipeline(fail_model=None, start=T0)
    for event in events:
        ingest(conn, event)
    snapshot = _fingerprint(conn)

    for event in events:
        ingest(conn, event)

    after = _fingerprint(conn)
    # event_count is expected to double; nothing structural may change.
    assert snapshot["runs"] == after["runs"]
    assert snapshot["jobs"] == after["jobs"]
    assert snapshot["datasets"] == after["datasets"]
    assert snapshot["edges"] == after["edges"]
    assert snapshot["states"] == after["states"]


def _fingerprint(conn) -> dict[str, Any]:
    return {
        "runs": conn.execute("select count(*) c from runs").fetchone()["c"],
        "jobs": conn.execute("select count(*) c from jobs").fetchone()["c"],
        "datasets": conn.execute("select count(*) c from datasets").fetchone()["c"],
        "edges": conn.execute("select count(*) c from run_datasets").fetchone()["c"],
        "states": sorted(
            (str(r["run_id"]), r["state"])
            for r in conn.execute("select run_id, state from runs").fetchall()
        ),
    }


# --------------------------------------------------------------- state machine


def test_state_never_moves_backwards(conn):
    run = uuid4()
    ingest(conn, make_event("START", run, "j", at=T0))
    ingest(conn, make_event("COMPLETE", run, "j", at=T0 + timedelta(seconds=30)))
    # A retried/buffered START delivered after the fact must not resurrect it.
    ingest(conn, make_event("START", run, "j", at=T0))

    row = conn.execute("select * from runs where run_id = %s", (run,)).fetchone()
    assert row["state"] == "COMPLETED"
    assert row["started_at"] == T0
    assert row["ended_at"] == T0 + timedelta(seconds=30)


def test_other_events_never_change_state(conn):
    run = uuid4()
    ingest(conn, make_event("START", run, "j"))
    ingest(conn, make_event("OTHER", run, "j", at=T0 + timedelta(seconds=1)))
    row = conn.execute("select state from runs where run_id = %s", (run,)).fetchone()
    assert row["state"] == "RUNNING"


def test_failure_carries_the_error_message(conn):
    events = build_pipeline(fail_model="fct_order_items", start=T0)
    for event in events:
        ingest(conn, event)

    failed = conn.execute(
        """
        select j.name, r.state, r.error_message
        from runs r join jobs j on j.id = r.job_id
        where r.state = 'FAILED'
        order by j.name
        """
    ).fetchall()
    names = [r["name"] for r in failed]
    # The failure propagates from the Spark SQL execution all the way to the DAG.
    assert "analytics_daily" in names
    assert "analytics_daily.dbt_run_marts" in names
    assert any("fct_order_items" in n for n in names)

    spark = next(r for r in failed if n_is_spark_sql(r["name"]))
    assert "Container killed by YARN" in spark["error_message"]


def n_is_spark_sql(name: str) -> bool:
    # Real openlineage-spark names are <app>.<command>.<db>_<table>, so the
    # command is in the middle rather than at the end.
    return "execute_insert_into_hadoop_fs_relation_command" in name


# -------------------------------------------------------------------- orphans


def test_orphan_run_is_visible_not_dropped(conn):
    """A parent facet pointing at a run nobody ever sent is a broken link in the
    chain. It must show up in `unstitched_runs` rather than vanishing."""
    child, ghost = uuid4(), uuid4()
    ingest(conn, make_event("START", child, "child", parent=(ghost, "ghost")))

    orphans = conn.execute("select * from unstitched_runs").fetchall()
    assert [o["job_name"] for o in orphans] == ["child"]

    health = queries.ingest_health(conn)
    assert health["unstitched_runs"] == 1
    assert health["placeholder_runs"] == 1


def test_orphan_clears_when_the_parent_shows_up(conn):
    child, parent = uuid4(), uuid4()
    ingest(conn, make_event("START", child, "child", parent=(parent, "parent")))
    assert queries.ingest_health(conn)["unstitched_runs"] == 1

    ingest(conn, make_event("START", parent, "parent"))
    assert queries.ingest_health(conn)["unstitched_runs"] == 0


def test_self_parenting_run_is_ignored(conn):
    """Seen in the wild from wrappers that reuse one run id for the whole
    invocation. Left unguarded it produces a cycle in the recursive CTE."""
    run = uuid4()
    ingest(conn, make_event("START", run, "j", parent=(run, "j")))
    row = conn.execute("select * from runs where run_id = %s", (run,)).fetchone()
    assert row["parent_run_id"] is None
    assert row["root_run_id"] == run
    assert queries.run_tree(conn, run)  # terminates


# ----------------------------------------------------------------- root facet


def test_root_facet_is_a_hint_not_the_answer(conn):
    """The `root` facet pre-creates the ancestor, but does not decide the root.

    Real openlineage-dbt (1.52.0, verified 2026-08-07) sets `root` to its own
    *parent* — the Airflow task — because dbt cannot see the DAG above it.
    Trusting the facet verbatim rooted every dbt run at the task, so asking for
    the tree from a dbt run returned a subtree with the DAG and its sibling
    tasks missing.

    We resolve the chain ourselves because we see every producer's events and
    no single producer does.
    """
    dag, mid, leaf = uuid4(), uuid4(), uuid4()
    ingest(conn, make_event("START", leaf, "leaf", parent=(mid, "mid"), root=(dag, "dag")))

    # The hint is used for what it is good for: we now know `dag` exists.
    placeholder = conn.execute("select * from runs where run_id = %s", (dag,)).fetchone()
    assert placeholder is not None and placeholder["is_placeholder"] is True

    # ...but with the chain incomplete we claim only what we can prove.
    leaf_row = conn.execute("select root_run_id from runs where run_id = %s", (leaf,)).fetchone()
    assert leaf_row["root_run_id"] == mid

    # Once the middle arrives, the chain resolves and repair corrects the leaf.
    ingest(conn, make_event("START", mid, "mid", parent=(dag, "dag")))
    leaf_row = conn.execute("select root_run_id from runs where run_id = %s", (leaf,)).fetchone()
    assert leaf_row["root_run_id"] == dag


def test_producer_claiming_its_parent_as_root_does_not_truncate_the_tree(conn):
    """Regression for the exact shape openlineage-dbt emits.

    dag -> task -> dbt, where the dbt event says root == task. Every node must
    still resolve to the DAG, or `tree <dbt-run-id>` silently drops the DAG and
    every sibling task.
    """
    dag, task, dbt_run, model = uuid4(), uuid4(), uuid4(), uuid4()

    ingest(conn, make_event("START", dag, "analytics_daily"))
    ingest(conn, make_event("START", task, "analytics_daily.dbt_run_marts",
                            parent=(dag, "analytics_daily"), root=(dag, "analytics_daily")))
    # dbt names the task as BOTH parent and root -- locally true, globally wrong.
    ingest(conn, make_event("START", dbt_run, "dbt-run-analytics",
                            parent=(task, "analytics_daily.dbt_run_marts"),
                            root=(task, "analytics_daily.dbt_run_marts")))
    ingest(conn, make_event("START", model, "model.analytics.fct_orders",
                            parent=(dbt_run, "dbt-run-analytics"),
                            root=(task, "analytics_daily.dbt_run_marts")))

    roots = {
        r["job_name"]: r["root_run_id"]
        for r in conn.execute("select * from run_summary").fetchall()
    }
    assert roots["dbt-run-analytics"] == dag
    assert roots["model.analytics.fct_orders"] == dag

    # And the payoff: from the deepest run you get the whole pipeline.
    assert queries.root_of(conn, model) == dag
    tree = queries.run_tree(conn, queries.root_of(conn, model))
    assert [n["job_name"] for n in tree][0] == "analytics_daily"
    assert len(tree) == 4


# --------------------------------------------------------------- sql lookup


def test_sql_is_inherited_from_the_nearest_ancestor(conn):
    """A Spark execution that reports no query text should still show one, taken
    from the dbt model above it — the correlation payoff in miniature."""
    parent, child = uuid4(), uuid4()
    query = "insert overwrite table analytics.fct_orders select 1"

    with_sql = make_event("START", parent, "model.fct_orders")
    with_sql["job"]["facets"] = {
        "sql": {"_producer": "test", "_schemaURL": "test", "query": query}
    }
    ingest(conn, with_sql)
    ingest(conn, make_event("START", child, "spark.exec", parent=(parent, "model.fct_orders")))

    found = queries.inherited_sql(conn, child)
    assert found["query"] == query
    assert found["job_name"] == "model.fct_orders"
    assert found["hops"] == 1, "must report how far up it looked, so the UI can label it"


def test_sql_lookup_prefers_the_run_s_own_query(conn):
    run = uuid4()
    event = make_event("START", run, "spark.exec")
    event["job"]["facets"] = {
        "sql": {"_producer": "test", "_schemaURL": "test", "query": "select own"}
    }
    ingest(conn, event)

    found = queries.inherited_sql(conn, run)
    assert found["hops"] == 0
    assert found["query"] == "select own"


def test_sql_lookup_returns_none_when_nothing_upstream_has_sql(conn):
    run = uuid4()
    ingest(conn, make_event("START", run, "j"))
    assert queries.inherited_sql(conn, run) is None


# ---------------------------------------------------------------- dataset i/o


def test_dataset_io_rolls_up_the_tree(conn):
    """The Airflow task never mentions a dataset; the Spark job four levels down
    does. Rolling I/O up is what turns a failed DAG run into 'these tables are
    now stale'."""
    events = build_pipeline(fail_model=None, start=T0)
    for event in events:
        ingest(conn, event)

    root = queries.list_runs(conn, roots_only=True, limit=1)[0]
    datasets = queries.tree_datasets(conn, root["run_id"])

    outputs = {d["name"] for d in datasets if d["direction"] == "OUTPUT"}
    inputs = {d["name"] for d in datasets if d["direction"] == "INPUT"}
    assert "warehouse/marts/fct_orders" in outputs
    assert "warehouse/staging/stg_orders" in inputs
    assert all(d["row_count"] for d in datasets if d["direction"] == "OUTPUT")


def test_rollup_does_not_double_count_the_same_write(conn):
    """Regression: one physical write is reported at two levels of the tree (the
    Spark SQL execution and the dbt model wrapping it). Aggregating those with
    sum() reports double the rows, which would look like a volume anomaly to
    Phase 03 rather than a bug in here."""
    for event in build_pipeline(models=("fct_orders",), fail_model=None, start=T0):
        ingest(conn, event)

    root = queries.list_runs(conn, roots_only=True, limit=1)[0]
    fct = next(
        d
        for d in queries.tree_datasets(conn, root["run_id"])
        if d["name"].endswith("fct_orders") and d["direction"] == "OUTPUT"
    )

    # Both the dbt model and the Spark execution reported this write...
    assert fct["reported_by"] == 2
    # ...but the row count must be one write's worth, not their sum.
    truth = conn.execute(
        """
        select distinct rd.row_count
        from run_datasets rd join datasets d on d.id = rd.dataset_id
        where d.name like '%%fct_orders' and rd.direction = 'OUTPUT'
        """
    ).fetchall()
    assert len(truth) == 1
    assert fct["row_count"] == truth[0]["row_count"]
