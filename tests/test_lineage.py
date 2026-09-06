"""Lineage: table edges, column edges, and walking the graph.

The roadmap assumed column lineage would mean SQLGlot over collected SQL text.
The captures say otherwise: **openlineage-spark 1.52.0 already emits a
`columnLineage` facet**, with per-field input columns, transformation type
(DIRECT/INDIRECT), subtype (IDENTITY/JOIN/…) and a masking flag. That is derived
from Spark's *resolved logical plan* — it knows the catalog, so it beats anything
a parser can conclude from text that references schemas we may not have.

So the order is: **facet first, SQLGlot as fallback** for producers that send SQL
but no facet — which on the reference stack means dbt. See ADR-007.

The tests are organised around what each source can and cannot prove. The
SQLGlot half is deliberately full of cases where it declines: a `select *`, an
unqualified column with two candidate tables. Recording a guess there would put
wrong edges in a graph people use to decide what broke, and a missing edge is
recoverable where a wrong one is not.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from dataspine import identity, lineage

NOW = datetime(2026, 8, 9, 12, 0, tzinfo=UTC)


def _job(conn, name, integration="SPARK", sql=None) -> int:
    facets = {"sql": {"query": sql}} if sql else {}
    return conn.execute(
        "insert into jobs (namespace, name, integration, facets) values ('t', %s, %s, %s) "
        "on conflict (namespace, name) do update set facets = excluded.facets returning id",
        (name, integration, json.dumps(facets)),
    ).fetchone()["id"]


def _dataset(conn, namespace, name, *, facets=None) -> int:
    return conn.execute(
        "insert into datasets (namespace, name, facets) values (%s, %s, %s) "
        "on conflict (namespace, name) do update set facets = excluded.facets returning id",
        (namespace, name, json.dumps(facets or {})),
    ).fetchone()["id"]


def _run(conn, job_id, *, root=None, at=NOW):
    run_id = uuid4()
    conn.execute(
        "insert into runs (run_id, job_id, root_run_id, state, started_at, ended_at) "
        "values (%s, %s, %s, 'COMPLETED', %s, %s)",
        (run_id, job_id, root or run_id, at, at),
    )
    return run_id


def _io(conn, run_id, dataset_id, direction):
    conn.execute(
        "insert into run_datasets (run_id, dataset_id, direction) values (%s, %s, %s) "
        "on conflict do nothing",
        (run_id, dataset_id, direction),
    )


def _model(conn, name, *, inputs=(), sql=None, integration="SPARK", facets=None, at=NOW):
    """One job reading `inputs` and writing `name`."""
    job = _job(conn, f"build_{name}", integration, sql=sql)
    run = _run(conn, job, at=at)
    out = _dataset(conn, "file", f"/warehouse/{name}", facets=facets)
    _io(conn, run, out, "OUTPUT")
    for upstream in inputs:
        _io(conn, run, _dataset(conn, "file", f"/warehouse/{upstream}"), "INPUT")
    return out


def _column_lineage_facet(mapping):
    """The shape openlineage-spark 1.52.0 really sends."""
    return {
        "columnLineage": {
            "fields": {
                column: {
                    "inputFields": [
                        {
                            "namespace": "file",
                            "name": f"/warehouse/{table}",
                            "field": field,
                            "transformations": [
                                {"type": kind, "subtype": subtype, "masking": masking,
                                 "description": ""}
                            ],
                        }
                        for table, field, kind, subtype, masking in sources
                    ]
                }
                for column, sources in mapping.items()
            }
        }
    }


def _resolve(conn):
    identity.resolve(conn)
    return lineage.resolve(conn)


# --------------------------------------------------------------- table edges


def test_table_edges_come_from_run_inputs_and_outputs(conn):
    _model(conn, "stg_orders")
    _model(conn, "fct_orders", inputs=["stg_orders"])
    _resolve(conn)

    edges = lineage.edges(conn)
    assert len(edges) == 1
    assert (edges[0]["upstream_name"], edges[0]["downstream_name"]) == (
        "stg_orders", "fct_orders"
    )


def test_edges_join_entities_not_raw_datasets(conn):
    """The reason identity had to come first.

    dbt and Spark report the same table under different names inside one run
    tree. Without entity resolution the graph has `analytics.fct_orders` and
    `/warehouse/fct_orders` as separate nodes and the chain breaks in the middle.
    """
    root_job = _job(conn, "analytics_daily", "AIRFLOW")
    root = _run(conn, root_job)

    stg = _dataset(conn, "file", "/warehouse/stg_orders")
    dbt_fct = _dataset(conn, "postgres://db", "analytics.fct_orders")
    spark_fct = _dataset(conn, "file", "/warehouse/fct_orders")

    dbt_run = _run(conn, _job(conn, "model.fct_orders", "DBT"), root=root)
    _io(conn, dbt_run, stg, "INPUT")
    _io(conn, dbt_run, dbt_fct, "OUTPUT")

    spark_run = _run(conn, _job(conn, "spark.fct_orders"), root=root)
    _io(conn, spark_run, stg, "INPUT")
    _io(conn, spark_run, spark_fct, "OUTPUT")

    _resolve(conn)

    edges = lineage.edges(conn)
    assert len(edges) == 1, "one logical edge, not one per producer"
    assert edges[0]["downstream_name"] == "fct_orders"


def test_a_self_referencing_run_produces_no_edge(conn):
    """An incremental model reads and writes the same table. A self-edge would
    make every downstream walk cycle forever."""
    job = _job(conn, "incremental")
    run = _run(conn, job)
    table = _dataset(conn, "file", "/warehouse/events")
    _io(conn, run, table, "INPUT")
    _io(conn, run, table, "OUTPUT")
    _resolve(conn)

    assert lineage.edges(conn) == []


def test_resolution_is_idempotent(conn):
    _model(conn, "stg_orders")
    _model(conn, "fct_orders", inputs=["stg_orders"])
    for _ in range(4):
        _resolve(conn)
    assert len(lineage.edges(conn)) == 1


# -------------------------------------------------------- column edges (facet)


def test_column_lineage_comes_from_the_facet_when_the_producer_sends_one(conn):
    _model(conn, "stg_orders")
    _model(conn, "stg_customers")
    _model(
        conn, "fct_orders",
        inputs=["stg_orders", "stg_customers"],
        facets=_column_lineage_facet({
            "order_id": [("stg_orders", "order_id", "DIRECT", "IDENTITY", False)],
            "segment": [("stg_customers", "segment", "DIRECT", "IDENTITY", False)],
        }),
    )
    _resolve(conn)

    columns = lineage.column_edges(conn, downstream="fct_orders")
    by_column = {c["downstream_column"]: c for c in columns}

    assert by_column["order_id"]["upstream_name"] == "stg_orders"
    assert by_column["order_id"]["upstream_column"] == "order_id"
    assert by_column["segment"]["upstream_name"] == "stg_customers"
    assert by_column["order_id"]["source"] == "facet"


def test_transformation_type_and_masking_survive(conn):
    """A masked column is a different fact about a lineage edge than an
    unmasked one, and it is the kind of thing a compliance question turns on."""
    _model(conn, "raw_users")
    _model(
        conn, "dim_users",
        inputs=["raw_users"],
        facets=_column_lineage_facet({
            "email_hash": [("raw_users", "email", "DIRECT", "TRANSFORMATION", True)],
            "signup_day": [("raw_users", "created_at", "INDIRECT", "GROUP_BY", False)],
        }),
    )
    _resolve(conn)

    columns = {
        c["downstream_column"]: c
        for c in lineage.column_edges(conn, downstream="dim_users")
    }
    assert columns["email_hash"]["masking"] is True
    assert columns["signup_day"]["transformation_type"] == "INDIRECT"
    assert columns["signup_day"]["transformation_subtype"] == "GROUP_BY"


def test_one_column_can_have_several_sources(conn):
    _model(conn, "a")
    _model(conn, "b")
    _model(
        conn, "merged",
        inputs=["a", "b"],
        facets=_column_lineage_facet({
            "id": [("a", "id", "DIRECT", "IDENTITY", False),
                   ("b", "id", "DIRECT", "IDENTITY", False)],
        }),
    )
    _resolve(conn)

    sources = {c["upstream_name"] for c in lineage.column_edges(conn, downstream="merged")}
    assert sources == {"a", "b"}


# ------------------------------------------------------ column edges (SQLGlot)


def test_sqlglot_fills_in_when_there_is_sql_but_no_facet(conn):
    """dbt sends SQL and no columnLineage facet — verified in the captures."""
    _model(conn, "stg_orders")
    _model(conn, "stg_customers")
    _model(
        conn, "fct_orders",
        inputs=["stg_orders", "stg_customers"],
        integration="DBT",
        sql=(
            "create table fct_orders as "
            "select o.order_id, o.order_total, c.segment "
            "from stg_orders o join stg_customers c on o.customer_id = c.customer_id"
        ),
    )
    _resolve(conn)

    columns = {
        c["downstream_column"]: c
        for c in lineage.column_edges(conn, downstream="fct_orders")
    }
    assert columns["order_id"]["upstream_name"] == "stg_orders"
    assert columns["segment"]["upstream_name"] == "stg_customers"
    assert columns["segment"]["source"] == "sql"


def test_the_facet_wins_over_sql_for_the_same_column(conn):
    """Both sources present. The facet knows the catalog; the parser is guessing
    from text, so it must not overwrite."""
    _model(conn, "stg_orders")
    _model(
        conn, "fct_orders",
        inputs=["stg_orders"],
        sql="create table fct_orders as select order_id from stg_orders",
        facets=_column_lineage_facet({
            "order_id": [("stg_orders", "order_id", "DIRECT", "IDENTITY", False)],
        }),
    )
    _resolve(conn)

    columns = lineage.column_edges(conn, downstream="fct_orders")
    assert len(columns) == 1
    assert columns[0]["source"] == "facet"


def test_a_select_star_produces_no_column_edges(conn):
    """The parser cannot know the columns without a schema, and inventing them
    would put wrong edges in a graph people use to decide what broke."""
    _model(conn, "stg_orders")
    _model(
        conn, "copy_orders",
        inputs=["stg_orders"],
        integration="DBT",
        sql="create table copy_orders as select * from stg_orders",
    )
    _resolve(conn)

    assert lineage.column_edges(conn, downstream="copy_orders") == []
    # The table-level edge is still known, and still useful.
    assert len(lineage.edges(conn)) == 1


def test_an_ambiguous_unqualified_column_is_declined(conn):
    """`select id from a join b` — `id` could come from either. A missing edge is
    recoverable; a wrong one is not."""
    _model(conn, "a")
    _model(conn, "b")
    _model(
        conn, "joined",
        inputs=["a", "b"],
        integration="DBT",
        sql="create table joined as select id from a join b on a.k = b.k",
    )
    _resolve(conn)

    edges = lineage.column_edges(conn, downstream="joined")
    assert all(e["downstream_column"] != "id" for e in edges)


def test_unparseable_sql_does_not_abort_resolution(conn):
    """dbt emits DDL and vendor-specific statements. One that sqlglot cannot read
    must not stop the rest of the graph being built."""
    _model(conn, "stg_orders")
    _model(conn, "broken", inputs=["stg_orders"], integration="DBT",
           sql="ALTER TABLE something SET TBLPROPERTIES ('x'='y') NOT REAL SQL ((")
    _model(conn, "fine", inputs=["stg_orders"], integration="DBT",
           sql="create table fine as select order_id from stg_orders")
    _resolve(conn)

    assert [c["downstream_column"] for c in lineage.column_edges(conn, downstream="fine")] == [
        "order_id"
    ]


# ------------------------------------------------------------------ traversal


@pytest.fixture()
def chain(conn):
    """raw -> stg -> fct -> report, plus an unrelated table."""
    _model(conn, "raw_orders")
    _model(conn, "stg_orders", inputs=["raw_orders"])
    _model(conn, "fct_orders", inputs=["stg_orders"])
    _model(conn, "report_daily", inputs=["fct_orders"])
    _model(conn, "unrelated")
    _resolve(conn)
    return conn


def test_upstream_walks_to_the_source(chain):
    found = lineage.upstream(chain, _entity(chain, "report_daily"), depth=10)
    assert {n["name"] for n in found} == {"fct_orders", "stg_orders", "raw_orders"}


def test_downstream_walks_to_the_leaves(chain):
    found = lineage.downstream(chain, _entity(chain, "raw_orders"), depth=10)
    assert {n["name"] for n in found} == {"stg_orders", "fct_orders", "report_daily"}


def test_depth_limits_the_walk(chain):
    found = lineage.upstream(chain, _entity(chain, "report_daily"), depth=1)
    assert {n["name"] for n in found} == {"fct_orders"}


def test_distance_is_reported_so_nearest_cause_is_findable(chain):
    found = {n["name"]: n["distance"] for n in
             lineage.upstream(chain, _entity(chain, "report_daily"), depth=10)}
    assert found["fct_orders"] == 1
    assert found["raw_orders"] == 3


def test_a_cycle_terminates(conn):
    """Lineage graphs acquire cycles through incremental models and bad replays.
    A traversal that does not terminate takes the whole UI down with it."""
    _model(conn, "a", inputs=["b"])
    _model(conn, "b", inputs=["a"])
    _resolve(conn)

    found = lineage.downstream(conn, _entity(conn, "a"), depth=50)
    assert {n["name"] for n in found} == {"b", "a"}


def test_the_graph_view_carries_nodes_and_edges_for_rendering(chain):
    graph = lineage.graph(chain, _entity(chain, "fct_orders"), depth=2)
    names = {n["name"] for n in graph["nodes"]}

    assert "fct_orders" in names
    assert "stg_orders" in names and "report_daily" in names
    assert "unrelated" not in names
    assert graph["focus"]["name"] == "fct_orders"
    for edge in graph["edges"]:
        assert edge["upstream_id"] in {n["id"] for n in graph["nodes"]}
        assert edge["downstream_id"] in {n["id"] for n in graph["nodes"]}


def test_grouping_summarises_by_namespace(conn):
    """Depth control alone is not enough on a real warehouse; a graph of 4,000
    tables needs to collapse to something a person can look at."""
    _model(conn, "a")
    _dataset(conn, "s3://lake", "raw/one")
    _dataset(conn, "s3://lake", "raw/two")
    _resolve(conn)

    groups = lineage.grouped(conn)
    by_namespace = {g["namespace"]: g["entities"] for g in groups}
    assert by_namespace["s3://lake"] == 2


def _entity(conn, name: str) -> int:
    return identity.find(conn, name)[0]["id"]
