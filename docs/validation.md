# Live integration validation

Use this guide to close the evidence gaps in the [roadmap](../ROADMAP.md#validation-priorities).
Unit tests and sanitized captures establish behavior for their inputs. Record live results
separately, including the commit, producer versions, configuration, test outcome and limitations.

## Prepare a disposable environment

Use a dedicated test account or project, scoped credentials and a cost limit. Confirm access
and service availability before creating resources. Keep credentials in the environment or
an approved secret store, and arrange teardown before starting. Never use production data.

Install the relevant optional drivers; AWS support is available via `.[aws]`. Consult
[configuration](configuration.md) and [integrations](integrations.md) for setup.

## Acceptance criteria

| Integration | Evidence required |
| --- | --- |
| S3 | Run `.venv/bin/pytest -m aws -q` with `DATASPINE_TEST_S3_BUCKET` (the test-only setting retains its legacy name) set to a dedicated bucket. Verify object listing and streamed/gzipped reads, and remove test objects. |
| Glue / Iceberg | Poll a table through the Glue catalog, commit another snapshot and poll again. Verify current metadata is resolved rather than a stale file path, with useful errors for missing metadata or denied access. |
| EMR on EC2 | Exercise `deploy/emr/bootstrap.sh`, run Airflow → dbt-spark over Thrift → Spark, ingest S3 event logs and run `wishd resolve`. Verify one rooted tree, expected metrics and namespaces, and explain any unstitched runs. Compare with local Thrift fixtures. |
| Warehouse pollers | Validate each driver/query against its warehouse. Compare stored rows, bytes, schema and freshness with known source changes; distinguish DDL timestamps from data freshness and verify unavailable metrics stay unknown. |
| Cost attribution | Import actual reports with resource identifiers and the required allocation columns. Reconcile totals and attributed/unattributed spend, including EC2, EMR and storage charges. Validate Databricks/Dataproc exports independently. |

For cost validation, configure the intended export and allocation tags before generating test
usage, and verify the delivered columns before relying on attribution. See the
[cost integration guide](integrations.md#cost). A parsed AWS CUR fixture does not by itself
prove cluster-to-run attribution on EMR.

## Captures and reporting

1. Capture raw responses into an **ignored local directory**, never directly into tracked fixtures.
2. Remove tokens, credentials, account identifiers, personal information and private SQL/data.
   Preserve schema and behavior with explicit synthetic replacements.
3. Scan the sanitized files before committing them; follow
   [fixture provenance](../tests/fixtures/README.md) and [CONTRIBUTING.md](../CONTRIBUTING.md).
4. Add a regression for the observed behavior, including any assumption the capture disproved.
5. Update the integration guide and roadmap with the exact validated scope. Record evidence
   in the PR; do not generalize one warehouse or producer version to all supported systems.
6. Tear down test resources and remove raw captures according to the account's data policy.

Live tests remain opt-in. Unvalidated integrations must stay clearly labelled until the
corresponding evidence is available.
