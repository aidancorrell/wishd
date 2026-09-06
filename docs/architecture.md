# Architecture decisions

wish:d runs as one Python service backed by Postgres. OpenLineage events are archived and
correlated into runs, jobs and datasets; monitors, lineage, incidents and cost analysis query
that shared model. The web UI and API run in the same process.

These stable ADR identifiers are cited in source comments. This document consolidates the
accepted decisions from the original development roadmap and removes superseded phase plans.
See [CLAUDE.md](../CLAUDE.md) for code conventions and [the roadmap](../ROADMAP.md) for priorities.

## ADR-001 — Python service with a small installation footprint

Use Python for the gateway, correlator, collectors and analysis. It fits the data engineering
ecosystem and keeps contributions accessible. Keep optional vendor drivers out of the core
installation. Postgres performs the central correlation queries.

The original proposal included a JVM listener; ADR-004 supersedes that part. A different
language or separate agent needs measured evidence that the existing boundary is inadequate.

## ADR-002 — Server-rendered HTML without a frontend build step

Use Jinja templates, handwritten CSS, native HTML interactions and self-hosted assets.
There is no JavaScript framework, Node build or separate frontend service. Full page loads
and explicit refreshes are accepted tradeoffs. The lineage graph is server-rendered SVG.

A focused vendored library may be considered for a demonstrated interaction need while
preserving the installation contract. See [interface design](design.md) for visual rules.

## ADR-003 — dbt-spark over Thrift is the reference integration

Target dbt-spark over a persistent Thrift server. Local/session mode and EMR Serverless need
separate integration design and validation. Local Thrift captures establish behavior for that
setup; they do not prove a live EMR deployment.

A shared Spark session outlives dbt invocations, so parent propagation alone is insufficient.
Correlation also uses the dbt node identity embedded in SQL comments. Dataset identity depends
on the table format and producer namespaces; preserve declared symlink identities.

## ADR-004 — Read Spark event logs

Parse the event logs Spark already writes instead of shipping a custom JVM Spark listener.
This supports historical import and keeps wish:d code outside the driver. OpenLineage's
producer listener remains responsible for lineage events.

Metrics arrive after logs are collected rather than as live stage telemetry. Event logging
must be enabled, and truncated logs must remain readable. Revisit only for a concrete need
for metrics from in-flight jobs.

## ADR-005 — Partitioned Postgres metric history

Store `metric_points` in ordinary monthly Postgres partitions, using the same maintenance
mechanism as the event archive. No Timescale extension or second storage engine is required.
Retention and partition provisioning remain application responsibilities. Reconsider a separate
engine only when measured query or storage requirements justify the operational cost.

## ADR-006 — Pure Python anomaly detection

Use standard-library seasonal indices, medians and robust deviation bounds for small monitor
series. Avoid a numerical computing stack in the base install. The accepted limits include
one seasonal period at a time and no trend extrapolation. A replacement needs a labelled
corpus demonstrating better detection, with its installation cost evaluated too.

## ADR-007 — Producer column lineage before SQL inference

Prefer OpenLineage `columnLineage` facets. Use SQLGlot on collected SQL when producer lineage
is absent. A SQL-derived edge must not overwrite a facet-derived edge; a facet may supersede
an inference. Store provenance so users can distinguish them.

Decline ambiguous columns and unresolved stars rather than inventing dependencies. Improve
schema context or parser coverage using captures, while retaining that precedence rule.

## ADR-008 — User-initiated agent handoff

Use a configured target's supported deep link when it can carry the briefing; otherwise use
a handoff page with copyable instructions. Keep target behavior isolated because client
capabilities change. This is user-initiated handoff, not server-side agent execution.

Sign handoff keys because briefings expose pipeline metadata and SQL. A key is derived without
an extra persistence table; links do not automatically expire. The direct-link briefing is
built at notification time, while the page builds its briefing when opened. Slack interaction
callbacks acknowledge the button without triggering a second action.

See [agent handoff](agent-handoff.md) for configuration and
[security](../SECURITY.md) for the sharing boundary.
