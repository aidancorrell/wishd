# Fixture provenance and sanitation

These fixtures exercise formats captured from Airflow 3.0.2 / OpenLineage provider 2.19.0,
dbt-core / openlineage-dbt 1.52.0, Spark 3.5.7, an Iceberg REST catalog, dbt Cloud on Snowflake,
and AWS Cost and Usage Reports. They preserve producer structure and relevant behavior.
See [live validation](../../docs/validation.md) and the tests named `test_real_*` for validation scope.

They are sanitized test data, not credentials or a supported deployment configuration.
The Spark event log's OpenLineage API token has been replaced with
`fixture-token-not-a-credential`. A historical copy held a real local token; current-file
sanitation alone does not make that history safe to publish. See the release checklist.

`sanitize_dbt_cloud.py` discovers and replaces account identifiers in captures under `local/`.
Before committing any capture, also inspect schema/database/user names, query comments,
connection URLs, headers, tokens, email addresses, cloud account IDs and compiled SQL.
Run Gitleaks without logging raw matches. Do not commit `local/`, newly generated event logs,
or the replacement map used to clean them. Preserve valid JSON and rerun the affected tests.
