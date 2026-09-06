# wish:d roadmap

wish:d (**what is happening:data**) correlates pipeline metadata so a team can follow
an Airflow task through dbt and Spark, investigate failures, and understand downstream impact.

The current target is **v1**; package metadata is `1.0.0rc1`. The frontend redesign and
release hardening are merged. Final review and publication are still pending.
Start with the [README](README.md) to run it, or the
[release checklist](docs/releasing.md) for the remaining launch gates.

## Available in the release candidate

- OpenLineage ingestion, parent/child run correlation, run timelines and artifact inspection.
- dbt-core artifact ingestion, dbt Cloud polling/webhooks, and Spark event-log metrics.
- YAML monitors for freshness, volume, schema drift, anomalies and data test failures.
- Table and column lineage, dataset identity resolution, catalog search and grouped incidents.
- Slack notifications and optional agent handoff, PagerDuty and webhook delivery.
- AWS CUR import, observed resource attribution and downstream impact on pull requests.
- A server-rendered interface with light/dark themes and self-hosted assets.
- Source installation and Docker deployment, packaged migrations, authentication,
  request limits, dependency audits and CI package/container checks.

Feature availability does not imply validation on every platform. The
[integration guide](docs/integrations.md) describes configuration and limitations.

## Before v1 publication

- [ ] Verify the combined frontend/backend build: CI, package installation, Docker demo,
  authentication, artifact persistence and manual UI review.
- [ ] Complete hosted-history review and final secret scans; verify private security reporting.
- [ ] Review getting-started instructions from a clean checkout and confirm final names,
  licenses, version and distribution instructions.
- [ ] Obtain the owner's explicit approval before changing visibility, publishing artifacts
  or announcing the release.

The [release checklist](docs/releasing.md) defines these checks. The repository remains
private until approval. No release date or registry availability is promised here.

## Validation priorities

Existing tests use sanitized captures from Airflow 3.0.2, dbt-core, dbt Cloud on Snowflake,
Spark 3.5.7, dbt-spark over Thrift, Iceberg/Delta metadata and AWS CUR. Captures verify
parser and correlation behavior for those inputs; they do not establish every live deployment.

| Priority | Outstanding evidence |
| --- | --- |
| EMR on EC2 | Bootstrap a real cluster and follow Airflow → dbt-spark over Thrift → Spark, including S3 event logs. |
| S3 and Glue | Run the opt-in S3 tests and verify Glue resolves current Iceberg metadata after a commit. |
| Warehouse pollers | Validate each supported poller against a real warehouse, including freshness semantics and restricted permissions. |
| Cost attribution | Reconcile actual EMR spend with observed runs; validate Databricks/Dataproc billing separately. |
| Scale | Measure ingest, query latency and retention under representative sustained workloads before extending capacity claims. |

See [live validation](docs/validation.md) for acceptance criteria and capture hygiene.
These integrations can remain explicitly experimental in v1; do not present unvalidated paths
as production-proven or make release progress depend on speculative cloud claims.

## After v1

Prioritize these with users and reproducible examples; they are candidates, not commitments:

- Improve integration coverage and actionable diagnostics using sanitized real-world captures.
- Improve column-lineage coverage where SQL parsing lacks schema or catalog information.
- Evaluate durable queued ingestion and recovery under process failure. The current async
  queue acknowledges before commit; synchronous ingestion is available today.
- Evaluate richer profiling with explicit scan budgets and dashboard-level downstream impact.
- Consider live Spark stage metrics only when post-run event logs cannot meet a concrete need.
- Define a tenant and user-permission model before adding RBAC or multi-tenancy.
- Consider server-side agent dispatch only with an explicit execution and authorization model;
  current handoff links let the user initiate the work.

## Architecture and contributions

[Architecture decisions](docs/architecture.md) preserve ADR-001 through ADR-008, which are
referenced throughout the code. [Interface design](docs/design.md) defines the UI contract.
Follow [CONTRIBUTING.md](CONTRIBUTING.md) for development and validation expectations.
