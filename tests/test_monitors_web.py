"""Monitors through the API and the UI, against a simulated pipeline.

The other monitor tests build rows directly. These go through the gateway, so
they also assert that a monitor works against the dataset names the *producers*
actually emit — which is where a target that resolved only to dbt's identity, or
only to Spark's, would quietly fail.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from dataspine.simulate import build_pipeline, shuffle_events

T0 = datetime(2026, 8, 6, 2, 0, 0, tzinfo=UTC)


def _load(client, *, pipelines=3):
    events = []
    for i in range(pipelines):
        events += build_pipeline(fail_model=None, start=T0 + timedelta(days=i))
    client.post("/api/v1/lineage/batch", json=shuffle_events(events, seed=7))


def _apply(client, kind, target, **config):
    """Create a monitor through the same reconciler `dataspine apply` uses."""
    from dataspine import db, monitors

    spec = monitors.MonitorSpec(
        name=f"test_{kind}",
        kind=kind,
        target_kind="dataset" if kind in monitors.DATASET_KINDS else "job",
        target=target,
        config=config,
        source="test.yml",
    )
    with db.connection() as conn:
        monitors.apply_specs(conn, [spec], sources=["test.yml"])
        conn.commit()
    return spec.name


def test_freshness_finds_the_table_under_the_names_producers_really_emit(api_client):
    """The end-to-end version of the identity problem.

    Nothing in this test names a namespace or a full path — it asks about
    `fct_orders`, which is what a human knows the table as.
    """
    _load(api_client)
    name = _apply(api_client, "freshness", "fct_orders", max_age_minutes=60)

    checked = api_client.post("/api/v1/monitors/check", params={"monitor": name}).json()
    assert checked["checked"] == 1
    result = checked["results"][0]

    # The simulated pipelines are days old, so this is a genuine breach rather
    # than an "insufficient_data" that would also have passed a weaker assertion.
    assert result["status"] == "breach"
    assert result["collected"] > 0

    detail = api_client.get(f"/api/v1/monitors/{name}").json()
    assert detail["monitor"]["last_status"] == "breach"
    assert len(detail["points"]) == 3, "one observation per pipeline run"


def test_row_count_reads_sparks_output_statistics(api_client):
    _load(api_client)
    name = _apply(api_client, "row_count", "fct_orders", min=1)
    result = api_client.post(
        "/api/v1/monitors/check", params={"monitor": name}
    ).json()["results"][0]
    assert result["status"] == "ok"
    assert result["value"] > 0


def test_monitor_page_shows_what_the_target_resolved_to(api_client):
    """A loose match is only safe if it is visible."""
    _load(api_client)
    name = _apply(api_client, "freshness", "fct_orders", max_age_minutes=60)
    api_client.post("/api/v1/monitors/check", params={"monitor": name})

    body = api_client.get(f"/monitors/{name}").text
    assert "Resolves to" in body
    assert "fct_orders" in body
    assert "max_age_minutes" in body


def test_breaches_are_visible_from_every_page(api_client):
    """Someone reading a run list should learn a table is stale without
    navigating to a second page to ask."""
    _load(api_client)
    name = _apply(api_client, "freshness", "fct_orders", max_age_minutes=60)
    api_client.post("/api/v1/monitors/check", params={"monitor": name})

    assert 'class="nav-badge"' in api_client.get("/").text
    listing = api_client.get("/monitors")
    assert "In breach" in listing.text
    assert name in listing.text


def test_monitor_list_is_empty_and_calm_before_anything_is_applied(api_client):
    body = api_client.get("/monitors").text
    assert "No monitors defined" in body
    assert "nav-badge" not in body


def test_monitor_endpoints_require_auth(api_client, monkeypatch):
    """Monitor results describe production data, so they sit behind the same bar
    as every other read endpoint rather than getting an exemption for being new."""
    from dataspine import auth

    monkeypatch.setenv(auth.TOKEN_ENV, "test:secret")
    assert api_client.get("/api/v1/monitors").status_code == 401
    assert api_client.post("/api/v1/monitors/check").status_code == 401
    assert api_client.get(
        "/api/v1/monitors", headers={"Authorization": "Bearer secret"}
    ).status_code == 200
