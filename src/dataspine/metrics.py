"""Prometheus exposition for the observability tool itself.

There was none. A platform whose entire pitch is "you cannot operate what you
cannot see" shipped with no way to see it: `/health/ingest` returns JSON built
for a human reading a web page, and alerting on it meant writing a scraper for a
bespoke shape. So the one system in the stack nobody could put on a dashboard
was this one.

**Hand-rolled, not `prometheus_client`.** The exposition format is a documented
handful of lines of text, and the alternative is a dependency plus a global
registry that fights the process model — this gateway already keeps its counters
in the ingest queue and its gauges in Postgres, and a registry would mean
maintaining a second copy of both. ADR-001's reasoning, fourth application.

**Reads, never writes.** A scrape must not be able to change what it measures,
must not hold a transaction open, and must not fail the endpoint if the database
is unreachable: a metrics endpoint that goes down with the database removes the
signal exactly when it is needed. Anything unavailable is simply omitted, which
Prometheus already treats as a stale series rather than a zero.
"""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger("dataspine.metrics")

# name -> (type, help). Kept beside the render so a metric cannot be emitted
# without a HELP line, which is what makes a dashboard readable by someone who
# did not write the exporter.
SPEC: dict[str, tuple[str, str]] = {
    "dataspine_events_ingested_total": (
        "counter", "OpenLineage events written since this process started"),
    "dataspine_events_dropped_total": (
        "counter", "Events shed because the ingest queue was full"),
    "dataspine_events_failed_total": (
        "counter", "Events that could not be processed"),
    "dataspine_queue_depth": (
        "gauge", "Events currently waiting in the ingest queue"),
    "dataspine_queue_capacity": (
        "gauge", "Maximum depth of the ingest queue"),
    "dataspine_runs_total": ("gauge", "Runs stored"),
    "dataspine_jobs_total": ("gauge", "Jobs stored"),
    "dataspine_datasets_total": ("gauge", "Datasets stored"),
    "dataspine_events_stored_total": ("gauge", "Events in the archive"),
    "dataspine_unstitched_runs": (
        "gauge", "Runs naming a parent no producer ever sent — a correlation gap"),
    "dataspine_placeholder_runs": (
        "gauge", "Runs created from a parent reference, not yet reported"),
    "dataspine_partitions_provisioned": (
        "gauge", "Month partitions provisioned from now forward; below 2 is a lapse"),
    "dataspine_upkeep_healthy": (
        "gauge", "1 when partition provisioning is ahead of schedule, 0 when lapsed"),
    "dataspine_monitor_breaches": ("gauge", "Monitors currently in breach"),
    # Delivery is the failure nothing else reports. A revoked token, a channel
    # the bot was removed from, a route that stopped matching -- every dashboard
    # stays green and the channel is quiet because nothing is arriving, not
    # because nothing is wrong.
    "dataspine_alerts_undelivered": (
        "gauge", "Monitor alerts that failed to deliver in the last 24h"),
    "dataspine_notifications_undelivered": (
        "gauge", "Run/incident/digest notifications undelivered in the last 24h"),
    "dataspine_build_info": ("gauge", "Always 1; carries the version as a label"),
}


def _line(name: str, value: Any, labels: dict[str, str] | None = None) -> str | None:
    if value is None:
        return None
    try:
        rendered = float(value)
    except (TypeError, ValueError):
        return None
    if labels:
        pairs = ",".join(f'{k}="{_escape(v)}"' for k, v in sorted(labels.items()))
        return f"{name}{{{pairs}}} {rendered:g}"
    return f"{name} {rendered:g}"


def _escape(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


def collect(*, queue_stats: dict[str, Any], health: dict[str, Any] | None,
            storage: dict[str, Any] | None, breaches: int | None,
            version: str, undelivered: dict[str, Any] | None = None) -> dict[str, Any]:
    """Flatten the sources into metric name -> value. Pure, so it is testable."""
    health = health or {}
    storage = storage or {}
    undelivered = undelivered or {}
    return {
        "dataspine_events_ingested_total": queue_stats.get("processed_events"),
        "dataspine_events_dropped_total": queue_stats.get("dropped_events"),
        "dataspine_events_failed_total": queue_stats.get("failed_events"),
        "dataspine_queue_depth": queue_stats.get("queue_depth"),
        "dataspine_queue_capacity": queue_stats.get("queue_maxsize"),
        "dataspine_runs_total": health.get("runs"),
        "dataspine_jobs_total": health.get("jobs"),
        "dataspine_datasets_total": health.get("datasets"),
        "dataspine_events_stored_total": health.get("events"),
        "dataspine_unstitched_runs": health.get("unstitched_runs"),
        "dataspine_placeholder_runs": health.get("placeholder_runs"),
        "dataspine_partitions_provisioned": storage.get("months_provisioned"),
        "dataspine_upkeep_healthy": (
            None if storage.get("healthy") is None else int(bool(storage.get("healthy")))
        ),
        "dataspine_monitor_breaches": breaches,
        "dataspine_alerts_undelivered": undelivered.get("alerts"),
        "dataspine_notifications_undelivered": undelivered.get("notifications"),
        "dataspine_build_info": 1,
    }


def render(values: dict[str, Any], *, version: str) -> str:
    """Prometheus text exposition, version 0.0.4."""
    out: list[str] = []
    for name, (kind, description) in SPEC.items():
        if name not in values:
            continue
        labels = {"version": version} if name == "dataspine_build_info" else None
        line = _line(name, values[name], labels)
        if line is None:
            # Omitted rather than zeroed: "we could not read this" and "this is
            # zero" mean opposite things, and a dashboard cannot tell them apart
            # after the fact. Prometheus handles a missing series correctly.
            continue
        out.append(f"# HELP {name} {description}")
        out.append(f"# TYPE {name} {kind}")
        out.append(line)
    return "\n".join(out) + "\n"
