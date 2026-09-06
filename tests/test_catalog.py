"""The catalog, and search over it.

The roadmap is specific that this is **a view over entities already stored**, not
a new system. Every fact on a catalog page — what writes this table, what it
depends on, which monitors watch it, when it last changed — is already in the
database because something else needed it. A catalog that required its own
ingestion would be a second source of truth to keep in sync, and the one that
drifts is always the one nobody is paged about.

The only genuinely new state is the part no pipeline can infer: ownership and
description. Those are annotations, and they are the reason `dataspine apply`
gained a `datasets:` block rather than the catalog gaining a form.

Search is Postgres full-text, per the roadmap's "only reach for anything else if
a benchmark forces it".
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from dataspine import catalog, identity, lineage, monitors

NOW = datetime(2026, 8, 9, 12, 0, tzinfo=UTC)


def _model(conn, name, *, inputs=(), integration="SPARK", at=NOW):
    job = conn.execute(
        "insert into jobs (namespace, name, integration) values ('t', %s, %s) "
        "on conflict (namespace, name) do update set name = excluded.name returning id",
        (f"build_{name}", integration),
    ).fetchone()["id"]
    run = uuid4()
    conn.execute(
        "insert into runs (run_id, job_id, root_run_id, state, started_at, ended_at) "
        "values (%s, %s, %s, 'COMPLETED', %s, %s)",
        (run, job, run, at, at),
    )
    conn.execute(
        "insert into run_datasets (run_id, dataset_id, direction, row_count) "
        "values (%s, %s, 'OUTPUT', 100)",
        (run, _dataset(conn, name)),
    )
    for upstream in inputs:
        conn.execute(
            "insert into run_datasets (run_id, dataset_id, direction) "
            "values (%s, %s, 'INPUT') on conflict do nothing",
            (run, _dataset(conn, upstream)),
        )


def _dataset(conn, name) -> int:
    return conn.execute(
        "insert into datasets (namespace, name) values ('file', %s) "
        "on conflict (namespace, name) do update set updated_at = now() returning id",
        (f"/warehouse/{name}",),
    ).fetchone()["id"]


@pytest.fixture()
def warehouse(conn):
    _model(conn, "raw_orders")
    _model(conn, "stg_orders", inputs=["raw_orders"])
    _model(conn, "fct_orders", inputs=["stg_orders"])
    _model(conn, "dim_customers")
    identity.resolve(conn)
    lineage.resolve(conn)
    return conn


def _entity(conn, name):
    return identity.find(conn, name)[0]["id"]


# ------------------------------------------------------------------- listing


def test_the_catalog_lists_every_table_with_its_context(warehouse):
    conn = warehouse
    entries = catalog.list_entries(conn)
    by_name = {e["name"]: e for e in entries}

    assert set(by_name) == {"raw_orders", "stg_orders", "fct_orders", "dim_customers"}
    assert by_name["stg_orders"]["upstream_count"] == 1
    assert by_name["stg_orders"]["downstream_count"] == 1
    assert by_name["dim_customers"]["upstream_count"] == 0


def test_an_entry_carries_what_writes_it(warehouse):
    conn = warehouse
    entry = catalog.get_entry(conn, _entity(conn, "fct_orders"))
    assert "build_fct_orders" in {j["name"] for j in entry["produced_by"]}


def test_an_entry_carries_its_monitors_and_their_status(warehouse):
    conn = warehouse
    monitors.apply_specs(
        conn,
        [monitors.MonitorSpec("fct_fresh", "freshness", "dataset", "fct_orders",
                              {"max_age_minutes": 60}, source="m.yml")],
        sources=["m.yml"],
    )
    conn.execute("update monitors set last_status = 'breach' where name = 'fct_fresh'")

    entry = catalog.get_entry(conn, _entity(conn, "fct_orders"))
    assert entry["monitors"][0]["name"] == "fct_fresh"
    assert entry["monitors"][0]["last_status"] == "breach"


def test_an_unmonitored_table_is_visible_as_unmonitored(warehouse):
    """The most useful thing a catalog can surface on day one is what nobody is
    watching."""
    conn = warehouse
    entries = {e["name"]: e for e in catalog.list_entries(conn)}
    assert entries["fct_orders"]["monitor_count"] == 0


def test_an_entry_carries_the_identities_it_was_assembled_from(warehouse):
    """A reader has to be able to see that three names are one table, or the
    catalog looks like it is hiding something."""
    conn = warehouse
    entry = catalog.get_entry(conn, _entity(conn, "fct_orders"))
    assert entry["identities"][0]["name"] == "/warehouse/fct_orders"


def test_last_written_comes_from_the_run_archive(warehouse):
    conn = warehouse
    entry = catalog.get_entry(conn, _entity(conn, "fct_orders"))
    assert entry["last_written_at"] is not None


# ----------------------------------------------------------------- annotation


def test_ownership_and_description_are_declared_in_yaml(conn):
    """The one thing no pipeline can infer, so the one thing that needs input.

    It goes in the same reviewed YAML as monitors rather than a form, for the
    same reason: an owner set by clicking is an owner nobody can diff.
    """
    _model(conn, "fct_orders")
    identity.resolve(conn)

    specs = catalog.parse_annotations({
        "datasets": [
            {
                "name": "fct_orders",
                "owner": "analytics-eng",
                "description": "One row per order, post-refunds.",
                "tags": ["gold", "pii-free"],
            }
        ]
    })
    catalog.apply_annotations(conn, specs, sources=["cat.yml"])

    entry = catalog.get_entry(conn, _entity(conn, "fct_orders"))
    assert entry["owner"] == "analytics-eng"
    assert entry["tags"] == ["gold", "pii-free"]


def test_annotating_an_unknown_table_is_kept_not_dropped(conn):
    """Declaring ownership before the first run is normal — it is how a new
    model gets an owner on the PR that creates it."""
    specs = catalog.parse_annotations(
        {"datasets": [{"name": "not_yet_built", "owner": "team"}]}
    )
    catalog.apply_annotations(conn, specs, sources=["cat.yml"])

    _model(conn, "not_yet_built")
    identity.resolve(conn)

    entry = catalog.get_entry(conn, _entity(conn, "not_yet_built"))
    assert entry["owner"] == "team"


def test_annotations_survive_re_resolution(conn):
    """Entity ids are stable across `identity.resolve()`, and ownership must not
    be collateral damage when a fourth identity shows up."""
    _model(conn, "fct_orders")
    identity.resolve(conn)
    catalog.apply_annotations(
        conn,
        catalog.parse_annotations({"datasets": [{"name": "fct_orders", "owner": "team"}]}),
        sources=["cat.yml"],
    )
    identity.resolve(conn)

    assert catalog.get_entry(conn, _entity(conn, "fct_orders"))["owner"] == "team"


def test_an_annotation_without_a_name_is_refused():
    with pytest.raises(catalog.AnnotationError, match="needs a `name:`"):
        catalog.parse_annotations({"datasets": [{"owner": "team"}]})


# --------------------------------------------------------------------- search


def test_search_finds_a_table_by_name(warehouse):
    conn = warehouse
    found = catalog.search(conn, "orders")
    assert {r["name"] for r in found} >= {"raw_orders", "stg_orders", "fct_orders"}


def test_search_ranks_an_exact_name_first(warehouse):
    conn = warehouse
    assert catalog.search(conn, "fct_orders")[0]["name"] == "fct_orders"


def test_search_finds_a_table_by_its_description(conn):
    """Someone looking for "refunds" should find the table whose description
    mentions them, which is most of what a catalog search is for."""
    _model(conn, "fct_orders")
    identity.resolve(conn)
    catalog.apply_annotations(
        conn,
        catalog.parse_annotations({
            "datasets": [{"name": "fct_orders",
                          "description": "One row per order, post-refunds."}]
        }),
        sources=["cat.yml"],
    )
    assert [r["name"] for r in catalog.search(conn, "refunds")] == ["fct_orders"]


def test_search_finds_a_table_by_owner_or_tag(conn):
    _model(conn, "fct_orders")
    identity.resolve(conn)
    catalog.apply_annotations(
        conn,
        catalog.parse_annotations({
            "datasets": [{"name": "fct_orders", "owner": "analytics-eng",
                          "tags": ["gold"]}]
        }),
        sources=["cat.yml"],
    )
    assert [r["name"] for r in catalog.search(conn, "analytics-eng")] == ["fct_orders"]
    assert [r["name"] for r in catalog.search(conn, "gold")] == ["fct_orders"]


def test_search_finds_a_table_by_a_column_it_contains(conn):
    """"Which tables have `customer_email`?" is the question that turns a catalog
    from documentation into a tool — it is how a GDPR request gets scoped."""
    _model(conn, "dim_customers")
    conn.execute(
        "update datasets set facets = %s where name = '/warehouse/dim_customers'",
        (json.dumps({"schema": {"fields": [{"name": "customer_email", "type": "string"}]}}),),
    )
    identity.resolve(conn)
    catalog.reindex(conn)

    assert [r["name"] for r in catalog.search(conn, "customer_email")] == ["dim_customers"]


def test_search_returns_nothing_rather_than_everything_for_gibberish(warehouse):
    assert catalog.search(warehouse, "zzzznotathing") == []


def test_search_survives_punctuation(warehouse):
    """Users paste fully-qualified names. `analytics.public.fct_orders` must not
    become a syntax error in a tsquery."""
    assert catalog.search(warehouse, "analytics.public.fct_orders & | !") is not None


def test_empty_search_returns_nothing_rather_than_the_whole_warehouse(warehouse):
    assert catalog.search(warehouse, "   ") == []
