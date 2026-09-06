"""Phase 04 surfaces: graph, catalog, search and incidents.

ADR-002 anticipated that the lineage graph would be the thing that finally forced
JavaScript into this UI, and allowed one vendored library for it. It turns out
not to be necessary: a lineage graph is a *layered* DAG, layers come from the hop
distance the traversal already computes, and laying out layered nodes is
arithmetic. The page renders as server-side inline SVG — no build step, no
framework, no vendored asset, no client-side anything.

That is worth stating rather than assuming, because "we need a graph library" is
the kind of thing that gets accepted without checking, and it would have cost the
install story its best property for a page of boxes and lines.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from dataspine import db


def _seed(conn):
    """raw_orders -> stg_orders -> fct_orders, one monitor, one breach."""
    now = datetime(2026, 8, 9, 12, 0, tzinfo=UTC)

    def dataset(name):
        return conn.execute(
            "insert into datasets (namespace, name) values ('file', %s) "
            "on conflict (namespace, name) do update set updated_at = now() returning id",
            (f"/warehouse/{name}",),
        ).fetchone()["id"]

    def model(name, inputs=()):
        job = conn.execute(
            "insert into jobs (namespace, name, integration) values ('t', %s, 'SPARK') "
            "on conflict (namespace, name) do update set name = excluded.name returning id",
            (f"build_{name}",),
        ).fetchone()["id"]
        run = uuid4()
        conn.execute(
            "insert into runs (run_id, job_id, root_run_id, state, started_at, ended_at) "
            "values (%s, %s, %s, 'COMPLETED', %s, %s)",
            (run, job, run, now, now),
        )
        conn.execute(
            "insert into run_datasets (run_id, dataset_id, direction) "
            "values (%s, %s, 'OUTPUT')",
            (run, dataset(name)),
        )
        for upstream in inputs:
            conn.execute(
                "insert into run_datasets (run_id, dataset_id, direction) "
                "values (%s, %s, 'INPUT') on conflict do nothing",
                (run, dataset(upstream)),
            )

    model("raw_orders")
    model("stg_orders", ["raw_orders"])
    model("fct_orders", ["stg_orders"])


@pytest.fixture()
def seeded(api_client):
    from dataspine import catalog, identity, lineage, monitors

    with db.connection() as conn:
        _seed(conn)
        identity.resolve(conn)
        lineage.resolve(conn)
        monitors.apply_specs(
            conn,
            [monitors.MonitorSpec("raw_fresh", "freshness", "dataset", "raw_orders",
                                  {"max_age_minutes": 1}, source="m.yml")],
            sources=["m.yml"],
        )
        catalog.reindex(conn)
        conn.commit()
    return api_client


def _entity_id(name):
    from dataspine import identity

    with db.connection() as conn:
        return identity.find(conn, name)[0]["id"]


# ----------------------------------------------------------------------- API


def test_api_lists_entities(seeded):
    body = seeded.get("/api/v1/entities").json()
    assert {e["name"] for e in body["entities"]} == {
        "raw_orders", "stg_orders", "fct_orders"
    }


def test_api_returns_a_graph(seeded):
    body = seeded.get(f"/api/v1/graph/{_entity_id('stg_orders')}?depth=2").json()
    assert body["focus"]["name"] == "stg_orders"
    assert {n["name"] for n in body["nodes"]} == {
        "raw_orders", "stg_orders", "fct_orders"
    }
    assert len(body["edges"]) == 2


def test_api_search(seeded):
    body = seeded.get("/api/v1/catalog/search", params={"q": "orders"}).json()
    assert len(body["results"]) == 3


def test_api_catalog_entry(seeded):
    body = seeded.get(f"/api/v1/catalog/{_entity_id('fct_orders')}").json()
    assert body["entry"]["name"] == "fct_orders"
    assert body["entry"]["upstream_count"] == 1


def test_api_incidents(seeded):
    seeded.post("/api/v1/monitors/check")
    body = seeded.get("/api/v1/incidents").json()
    assert body["incidents"][0]["cause_monitor"] == "raw_fresh"


def test_lineage_endpoints_require_auth(seeded, monkeypatch):
    from dataspine import auth

    monkeypatch.setenv(auth.TOKEN_ENV, "test:secret")
    assert seeded.get("/api/v1/entities").status_code == 401
    assert seeded.get("/api/v1/catalog/search?q=x").status_code == 401


# ------------------------------------------------------------------------ UI


def test_catalog_page_lists_tables(seeded):
    body = seeded.get("/catalog").text
    assert "fct_orders" in body
    assert "stg_orders" in body


def test_catalog_page_search_narrows(seeded):
    body = seeded.get("/catalog", params={"q": "fct_orders"}).text
    assert "fct_orders" in body
    assert ">raw_orders<" not in body


def test_catalog_entry_page_shows_context(seeded):
    body = seeded.get(f"/catalog/{_entity_id('fct_orders')}").text
    assert "build_fct_orders" in body
    assert "/warehouse/fct_orders" in body


def test_lineage_page_renders_svg_without_javascript(seeded):
    """The ADR-002 claim, asserted rather than assumed.

    A lineage graph is a layered DAG and the layers are the hop distances the
    traversal already computes, so the layout is arithmetic. Vendoring a graph
    library would have cost the install story its best property to draw boxes
    and lines.
    """
    body = seeded.get(f"/lineage/{_entity_id('stg_orders')}").text
    assert "<svg" in body
    assert "raw_orders" in body and "fct_orders" in body
    assert "<script" not in body


def test_lineage_page_respects_depth(seeded):
    shallow = seeded.get(f"/lineage/{_entity_id('fct_orders')}", params={"depth": 1}).text
    deep = seeded.get(f"/lineage/{_entity_id('fct_orders')}", params={"depth": 3}).text
    assert "raw_orders" not in shallow
    assert "raw_orders" in deep


def test_incidents_page_leads_with_the_cause(seeded):
    seeded.post("/api/v1/monitors/check")
    body = seeded.get("/incidents").text
    assert "raw_fresh" in body


def test_an_unknown_entity_is_a_not_found_not_a_crash(seeded):
    assert seeded.get("/lineage/999999").status_code == 200
    assert seeded.get("/catalog/999999").status_code == 200


def test_nav_reaches_the_new_pages(seeded):
    body = seeded.get("/").text
    assert 'href="/catalog"' in body
    assert 'href="/incidents"' in body
