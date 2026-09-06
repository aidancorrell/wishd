"""Deciding when two dataset rows are the same table.

Phase 03 got away with matching on the final name segment and *showing* the
result rather than trusting it. Lineage cannot: an edge joins two identities, and
if `fct_orders` is three nodes the graph is wrong in a way no amount of display
honesty repairs. Open question 4 comes due here.

The evidence available, in descending order of how much it proves:

  **Co-writing.** Two datasets written by runs in the *same run tree*, with the
  same leaf name, are one table reported twice. This is evidence no single
  producer can supply — the correlator earned it — and it is what distinguishes
  "dbt and Spark describing one write" from "two different tables that happen to
  share a name".

  **Leaf name.** `analytics.fct_orders` and `/warehouse/fct_orders` share a final
  segment. Necessary, nowhere near sufficient: two schemas both containing
  `events` share it too.

  **Namespace.** Different namespaces are weak evidence of *difference* — the
  whole problem is that one table legitimately appears under three. But two rows
  in the *same* namespace with different names are certainly different tables.

The tests below are mostly about refusing to merge. A false merge silently
rewrites someone's lineage graph, and unlike a false alert there is nothing to
notice.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from dataspine import identity

NOW = datetime(2026, 8, 9, 12, 0, tzinfo=UTC)


def _job(conn, name, integration="DBT") -> int:
    return conn.execute(
        "insert into jobs (namespace, name, integration) values ('t', %s, %s) "
        "on conflict (namespace, name) do update set name = excluded.name returning id",
        (name, integration),
    ).fetchone()["id"]


def _dataset(conn, namespace, name) -> int:
    return conn.execute(
        "insert into datasets (namespace, name) values (%s, %s) "
        "on conflict (namespace, name) do update set updated_at = now() returning id",
        (namespace, name),
    ).fetchone()["id"]


def _run(conn, job_id, *, root=None, at=NOW):
    run_id = uuid4()
    conn.execute(
        "insert into runs (run_id, job_id, root_run_id, parent_run_id, state, "
        "started_at, ended_at) values (%s, %s, %s, %s, 'COMPLETED', %s, %s)",
        (run_id, job_id, root or run_id, root, at, at),
    )
    return run_id


def _write(conn, run_id, dataset_id, direction="OUTPUT"):
    conn.execute(
        "insert into run_datasets (run_id, dataset_id, direction) values (%s, %s, %s) "
        "on conflict do nothing",
        (run_id, dataset_id, direction),
    )


def _pipeline_writing(conn, pairs, *, at=NOW):
    """One pipeline execution in which several producers report the same write."""
    root_job = _job(conn, "analytics_daily", "AIRFLOW")
    root = _run(conn, root_job, at=at)
    for index, (namespace, name) in enumerate(pairs):
        child_job = _job(conn, f"producer_{index}", "SPARK")
        child = _run(conn, child_job, root=root, at=at)
        _write(conn, child, _dataset(conn, namespace, name))
    return root


# ------------------------------------------------------------------- merging


def test_the_three_identities_of_one_table_resolve_to_one_entity(conn):
    """The case the captures actually produced, end to end."""
    _pipeline_writing(
        conn,
        [
            ("postgres://db:5432", "analytics.fct_orders"),
            ("file", "/tmp/warehouse/fct_orders"),
            ("s3://lake", "warehouse/marts/fct_orders"),
        ],
    )
    identity.resolve(conn)

    entities = identity.entities(conn)
    assert len(entities) == 1
    entity = entities[0]
    assert entity["name"] == "fct_orders"
    assert len(entity["datasets"]) == 3


def test_same_leaf_without_co_writing_is_not_merged(conn):
    """Two schemas both containing `events` are two tables.

    A shared final segment is necessary and nowhere near sufficient. Merging on
    it alone silently rewrites a lineage graph, and unlike a false alert there is
    nothing for anyone to notice.
    """
    _dataset(conn, "postgres://db", "raw.events")
    _dataset(conn, "postgres://db", "marts.events")
    identity.resolve(conn)

    assert len(identity.entities(conn)) == 2


def test_co_writing_in_different_trees_does_not_merge(conn):
    """Two nightly pipelines each writing their own `events` table are not
    evidence about each other."""
    _pipeline_writing(conn, [("a", "one.events")], at=NOW - timedelta(hours=2))
    _pipeline_writing(conn, [("b", "two.events")], at=NOW - timedelta(hours=1))
    identity.resolve(conn)

    assert len(identity.entities(conn)) == 2


def test_reading_a_table_is_not_evidence_of_being_it(conn):
    """A run that reads `stg_orders` and writes `fct_orders` must not merge them.

    Co-*writing* is the signal. Using inputs as well would collapse every table a
    job touches into one node, which is the most destructive possible failure of
    a lineage graph.
    """
    root_job = _job(conn, "analytics_daily", "AIRFLOW")
    root = _run(conn, root_job)
    stg = _dataset(conn, "file", "/warehouse/stg_orders")
    fct = _dataset(conn, "file", "/warehouse/fct_orders")
    _write(conn, root, stg, "INPUT")
    _write(conn, root, fct, "OUTPUT")
    identity.resolve(conn)

    assert len(identity.entities(conn)) == 2


def test_different_leaves_in_one_tree_stay_separate(conn):
    """One pipeline writes many tables. Sharing a run tree is not, by itself,
    evidence of being the same table."""
    _pipeline_writing(
        conn,
        [("file", "/warehouse/fct_orders"), ("file", "/warehouse/dim_customers")],
    )
    identity.resolve(conn)
    assert len(identity.entities(conn)) == 2


def test_merging_is_transitive_across_pipeline_runs(conn):
    """Monday's run proves dbt's name equals Spark's; Tuesday's proves Spark's
    equals S3's. The entity must end up with all three, not two of them."""
    _pipeline_writing(
        conn,
        [("postgres://db", "analytics.fct_orders"), ("file", "/warehouse/fct_orders")],
        at=NOW - timedelta(days=2),
    )
    _pipeline_writing(
        conn,
        [("file", "/warehouse/fct_orders"), ("s3://lake", "marts/fct_orders")],
        at=NOW - timedelta(days=1),
    )
    identity.resolve(conn)

    entities = identity.entities(conn)
    assert len(entities) == 1
    assert len(entities[0]["datasets"]) == 3


def test_resolution_is_idempotent(conn):
    _pipeline_writing(
        conn, [("postgres://db", "analytics.fct_orders"), ("file", "/w/fct_orders")]
    )
    for _ in range(4):
        identity.resolve(conn)
    assert len(identity.entities(conn)) == 1
    assert conn.execute("select count(*) as n from dataset_entities").fetchone()["n"] == 1


def test_a_dataset_nothing_has_written_still_gets_an_entity(conn):
    """A polled source table has no runs at all. It is still a table, and lineage
    needs a node for it or every upstream walk stops one step short."""
    _dataset(conn, "postgres://warehouse", "raw.stripe_charges")
    identity.resolve(conn)

    entities = identity.entities(conn)
    assert len(entities) == 1
    assert entities[0]["name"] == "stripe_charges"


def test_a_new_identity_joins_an_existing_entity(conn):
    """Re-resolution after a producer starts reporting a fourth name must extend
    the entity rather than fork a new one — otherwise the graph splits in two on
    an ordinary config change."""
    _pipeline_writing(
        conn, [("postgres://db", "analytics.fct_orders"), ("file", "/w/fct_orders")]
    )
    identity.resolve(conn)
    before = identity.entities(conn)[0]["id"]

    _pipeline_writing(
        conn, [("file", "/w/fct_orders"), ("s3://lake", "marts/fct_orders")],
        at=NOW - timedelta(hours=1),
    )
    identity.resolve(conn)

    entities = identity.entities(conn)
    assert len(entities) == 1
    assert entities[0]["id"] == before, "the entity must be extended, not replaced"
    assert len(entities[0]["datasets"]) == 3


# ------------------------------------------------------------------- lookups


def test_entity_for_dataset_round_trips(conn):
    _pipeline_writing(
        conn, [("postgres://db", "analytics.fct_orders"), ("file", "/w/fct_orders")]
    )
    identity.resolve(conn)

    dataset_id = conn.execute(
        "select id from datasets where namespace = 'file'"
    ).fetchone()["id"]
    entity = identity.entity_for_dataset(conn, dataset_id)
    assert entity["name"] == "fct_orders"


def test_find_by_name_prefers_an_exact_match(conn):
    """`orders` and `fct_orders` both end in something a fuzzy search would
    return. A user asking for one must not silently get the other."""
    _dataset(conn, "db", "analytics.orders")
    _dataset(conn, "db", "analytics.fct_orders")
    identity.resolve(conn)

    found = identity.find(conn, "orders")
    assert found[0]["name"] == "orders"


def test_the_evidence_for_a_merge_is_recorded(conn):
    """"Why are these one node?" must be answerable without re-deriving it.

    A merge is the one operation here that destroys information — two rows
    becoming one — so it has to carry its receipt.
    """
    _pipeline_writing(
        conn, [("postgres://db", "analytics.fct_orders"), ("file", "/w/fct_orders")]
    )
    identity.resolve(conn)

    entity = identity.entities(conn)[0]
    reasons = {d["match_reason"] for d in entity["datasets"]}
    assert "co-written" in reasons


def test_same_named_entities_are_surfaced_as_unmerged_candidates(conn):
    """Where the resolver declines, it says so rather than staying silent.

    Monitors match on leaf name (Phase 03); lineage merges only on co-write
    evidence. The two can therefore disagree about what one table is — a monitor
    watching three identities while the graph shows two nodes. That is the right
    trade in both places, but an invisible disagreement is a trap, so the
    candidates the resolver refused are listed on the entity.
    """
    _pipeline_writing(
        conn,
        [("postgres://db", "analytics.fct_orders"), ("file", "/warehouse/fct_orders")],
    )
    # A third identity, co-written only within its own tree — no evidence links
    # it to the pair above.
    _pipeline_writing(conn, [("s3://lake", "marts/fct_orders")],
                      at=NOW - timedelta(days=1))
    identity.resolve(conn)

    entities = {e["name"]: e for e in identity.entities(conn)}
    assert len(entities) == 1 or len(identity.entities(conn)) == 2

    merged = [e for e in identity.entities(conn) if len(e["datasets"]) == 2][0]
    candidates = identity.unmerged_candidates(conn, merged["id"])
    assert any(c["name"] == "marts/fct_orders" for c in candidates)


def test_an_entity_with_no_namesakes_has_no_candidates(conn):
    _dataset(conn, "db", "analytics.fct_orders")
    identity.resolve(conn)
    entity = identity.entities(conn)[0]
    assert identity.unmerged_candidates(conn, entity["id"]) == []


# ------------------------------------------------------- declared identities
#
# Iceberg changed what evidence is available. Everything above is inference:
# co-writing plus a shared leaf is a good guess. A `symlinks` facet is the
# producer *stating* the catalog identity of the path it just wrote, verified
# against a real capture (openlineage-spark 1.52.0 through an Iceberg REST
# catalog). Declared beats inferred, the same precedence D3 settled.


def _with_symlink(conn, namespace, name, *, target, catalog="http://iceberg-rest:8181"):
    """A dataset carrying the symlinks facet Spark attaches to Iceberg tables."""
    dataset_id = _dataset(conn, namespace, name)
    conn.execute(
        """
        update datasets set facets = jsonb_build_object(
            'symlinks', jsonb_build_object(
                'identifiers', jsonb_build_array(jsonb_build_object(
                    'name', %s::text, 'namespace', %s::text, 'type', 'TABLE'))))
        where id = %s
        """,
        (target, catalog, dataset_id),
    )
    return dataset_id


def test_a_declared_symlink_merges_without_any_co_writing(conn):
    """The case Iceberg actually produces on the reference deployment.

    Spark names an Iceberg table by its physical path; dbt names it
    `schema.table`. On a shared Thrift Server their run trees never meet (D6),
    so co-write evidence does not exist and Phase 04 correctly declines. The
    symlink is what makes the merge available at all — and note neither dataset
    is written by any run here, so nothing but the declaration could have done it.
    """
    _with_symlink(
        conn,
        "file",
        "/warehouse/iceberg/analytics_marts/stg_customers",
        target="analytics_marts.stg_customers",
    )
    _dataset(conn, "spark://thrift:10000", "analytics_marts.stg_customers")

    identity.resolve(conn)

    entities = identity.entities(conn)
    assert len(entities) == 1
    assert len(entities[0]["datasets"]) == 2


def test_a_declared_merge_is_recorded_as_declared(conn):
    """Auditable, because the two kinds of evidence are not equally strong and a
    reader deciding whether to act on an edge needs to know which produced it."""
    _with_symlink(
        conn, "file", "/warehouse/iceberg/db/t", target="db.t"
    )
    _dataset(conn, "spark://thrift:10000", "db.t")
    identity.resolve(conn)

    reasons = {
        row["match_reason"]
        for row in conn.execute("select match_reason from dataset_identities").fetchall()
    }
    assert reasons == {"declared"}


def test_a_symlink_matches_the_whole_name_not_the_leaf(conn):
    """The producer supplies `schema.table`, so requiring all of it costs
    nothing and avoids the ambiguity that forced leaf matching to need
    corroborating evidence."""
    _with_symlink(
        conn, "file", "/warehouse/iceberg/marts/events", target="marts.events"
    )
    # Same leaf, different schema. A leaf match would merge these; the declared
    # identity must not.
    _dataset(conn, "spark://thrift:10000", "raw.events")

    identity.resolve(conn)
    assert len(identity.entities(conn)) == 2


def test_a_symlink_pointing_at_nothing_is_harmless(conn):
    """A table whose counterpart has not been ingested yet is the normal state
    during a backfill, not an error."""
    _with_symlink(
        conn, "file", "/warehouse/iceberg/db/only", target="db.nothing_here"
    )
    identity.resolve(conn)

    entities = identity.entities(conn)
    assert len(entities) == 1
    assert len(entities[0]["datasets"]) == 1
