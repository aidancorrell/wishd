"""A DAG shaped like the real thing, for validating the correlator.

This mirrors the structure `simulate.py` fakes — an Airflow task that shells out
to dbt-core — so we can diff the events real integrations emit against the
events we invented.

The `dbt_run_marts` task is the whole point. `OPENLINEAGE_PARENT_ID` is the
handoff the roadmap calls the single most fragile link in the chain: whatever
dbt emits next must claim this task instance as its OpenLineage parent, or the
pipeline splits into two unrelated trees and every downstream feature
(root cause, blast radius, cost attribution) silently breaks.

The `lineage_parent_id` macro is provided by the OpenLineage Airflow provider
and returns `<namespace>/<job_name>/<run_id>` for the current task instance —
which is exactly the format `dbt-ol` expects. Using the macro rather than
hand-assembling the string is what makes this a real test: if the provider ever
changes its run-id derivation, this keeps working and a hand-rolled version
would not.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

DBT_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="analytics_daily",
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    schedule="0 2 * * *",
    catchup=False,
    tags=["dataspine", "validation"],
) as dag:
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        # `dbt-ol` wraps dbt and emits OpenLineage events for the invocation and
        # each node. --consume-structured-logs streams node events live rather
        # than parsing run_results.json after the fact, which is the mode that
        # matters on ephemeral EMR nodes.
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt-ol run --consume-structured-logs --profiles-dir . --project-dir ."
        ),
        env={
            "OPENLINEAGE_PARENT_ID": "{{ macros.OpenLineageProviderPlugin.lineage_parent_id(task_instance) }}",
            "OPENLINEAGE_URL": "{{ var.value.get('openlineage_url', 'http://gateway:8080') }}",
            "OPENLINEAGE_API_KEY": "{{ var.value.get('openlineage_api_key', '') }}",
            "OPENLINEAGE_NAMESPACE": "dbt://analytics",
            "DBT_PROFILES_DIR": DBT_DIR,
            "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
        },
        append_env=True,
    )

    @task(task_id="publish_marts")
    def publish_marts() -> None:
        print("[dataspine] publishing marts")

    dbt_run_marts >> publish_marts()
