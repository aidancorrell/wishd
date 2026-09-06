"""Prometheus exposition, and the rule about what a missing value means.

A platform selling "you cannot operate what you cannot see" had no way to be
seen: `/health/ingest` is JSON shaped for a web page, so alerting on this
service meant writing a bespoke scraper for it. The one system nobody could put
on a dashboard was this one.

The load-bearing decision is what happens when something cannot be read. A
metrics endpoint that fails with the database has removed the signal at the
moment it is needed, and one that reports 0 for an unreadable gauge is worse
still: on a dashboard "we could not read this" and "this is zero" look identical
and mean opposite things. Unreadable values are omitted, which Prometheus
already interprets correctly as a stale series.
"""

from __future__ import annotations

from dataspine import metrics

QUEUE = {
    "queue_depth": 3,
    "queue_maxsize": 1000,
    "dropped_events": 0,
    "processed_events": 4210,
    "failed_events": 2,
}
HEALTH = {"runs": 362, "jobs": 71, "datasets": 25, "events": 700,
          "unstitched_runs": 0, "placeholder_runs": 0}
STORAGE = {"months_provisioned": 4, "healthy": True}


def _render(**over):
    values = metrics.collect(
        queue_stats=over.get("queue", QUEUE),
        health=over.get("health", HEALTH),
        storage=over.get("storage", STORAGE),
        breaches=over.get("breaches", 1),
        version="0.1.0",
    )
    return metrics.render(values, version="0.1.0")


def test_exposition_is_well_formed():
    text = _render()
    assert text.endswith("\n")
    for name in ("dataspine_events_ingested_total", "dataspine_queue_depth",
                 "dataspine_unstitched_runs", "dataspine_partitions_provisioned"):
        assert f"# HELP {name} " in text
        assert f"# TYPE {name} " in text
        assert any(line.startswith(f"{name} ") for line in text.splitlines())


def test_every_emitted_metric_carries_help_and_type():
    """A dashboard has to be readable by someone who did not write the exporter."""
    lines = [ln for ln in _render().splitlines() if ln and not ln.startswith("#")]
    text = _render()
    for line in lines:
        name = line.split("{")[0].split(" ")[0]
        assert f"# HELP {name} " in text, f"{name} emitted without HELP"


def test_counters_and_gauges_are_typed_correctly():
    text = _render()
    assert "# TYPE dataspine_events_ingested_total counter" in text
    assert "# TYPE dataspine_queue_depth gauge" in text


def test_build_info_carries_the_version_as_a_label():
    assert 'dataspine_build_info{version="0.1.0"} 1' in _render()


def test_an_unreadable_value_is_omitted_not_zeroed():
    """The rule the whole module turns on."""
    text = _render(health=None, storage=None, breaches=None)

    assert "dataspine_runs_total" not in text
    assert "dataspine_partitions_provisioned" not in text
    assert "dataspine_monitor_breaches" not in text
    # Process-local counters survive, because they were readable.
    assert "dataspine_events_ingested_total 4210" in text


def test_a_lapsed_partition_horizon_reads_as_zero_not_missing():
    """Distinct from unreadable: we looked, and it is unhealthy. That must be a
    real 0 so an alert can fire on it."""
    text = _render(storage={"months_provisioned": 1, "healthy": False})
    assert "dataspine_upkeep_healthy 0" in text
    assert "dataspine_partitions_provisioned 1" in text


def test_label_values_are_escaped():
    values = metrics.collect(queue_stats=QUEUE, health=HEALTH, storage=STORAGE,
                             breaches=0, version='0.1.0"; DROP')
    text = metrics.render(values, version='0.1.0"; DROP')
    assert '\\"' in text
    assert text.count("\n") == len([ln for ln in text.splitlines() if ln])


def test_no_content_leaks_into_metrics():
    """Volume, not content: counts are fine, job names and SQL are not — this is
    the endpoint most likely to be exposed without authentication."""
    text = _render()
    for leak in ("select ", "namespace", "job_name", "fct_orders"):
        assert leak not in text.lower()


def test_the_endpoint_serves_over_http(api_client):
    response = api_client.get("/metrics")
    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"]
    assert "dataspine_build_info" in response.text
