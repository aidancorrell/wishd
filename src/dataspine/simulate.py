"""A synthetic EMR pipeline, emitted as real OpenLineage events.

You cannot iterate on a correlator by waiting for a nightly EMR run, and you
should not need an AWS account to develop this project. This module produces the
exact event shapes the real integrations emit for the target stack:

    Airflow DAG run  (apache-airflow-providers-openlineage)
      └─ Airflow task run
           └─ dbt invocation        (dbt-ol / --consume-structured-logs)
                └─ dbt model run
                     └─ Spark application   (OpenLineageSparkListener)
                          └─ Spark SQL execution

Two scenarios ship: a clean run, and one where a Spark stage fails and the
failure propagates up every level. Events can be emitted shuffled, because the
one thing guaranteed about production is that they will not arrive in order.

VALIDATED 2026-08-07 against a real Airflow 3.0.2 running
apache-airflow-providers-openlineage 2.19.0 (openlineage-python 1.52.0). The
Airflow half of this file is no longer guesswork:

  - job naming confirmed: `<dag_id>` for the DAG run, `<dag_id>.<task_id>` for
    task runs
  - jobType facet confirmed: {integration: AIRFLOW, jobType: DAG|TASK,
    processingType: BATCH}
  - the parent facet does carry `root`, so the gateway's fast path is the one
    production actually takes
  - run ids are UUIDv7, not v4

Things the real provider sends that this file does not bother reproducing:
`airflow`, `airflowDagRun`, `airflowState`, `ownership` and
`unknownSourceAttribute` facets. They ride through as opaque jsonb, which is
exactly the point of not flattening facets into columns. A verbatim capture
lives in tests/fixtures/ and is asserted against in test_real_airflow.py.

dbt ALSO VALIDATED 2026-08-07 against openlineage-dbt 1.52.0 running
`dbt-ol run --consume-structured-logs` from an Airflow BashOperator. The
original guesses here were wrong and have been corrected:

  guessed                                    actual
  analytics.run                              dbt-run-analytics            (JOB)
  analytics.model.analytics.fct_orders       model.analytics.fct_orders   (MODEL)
  (no equivalent)                            model.analytics.fct_orders.sql.N (SQL)

The `.sql.N` level is new: in structured-logs mode dbt emits one event per SQL
statement executed within a node, which is a level deeper than we modelled.

Also learned the hard way: openlineage-dbt sets the parent facet's `root` to its
own *parent* (the Airflow task), not the true top of the tree. The correlator
therefore treats `root` as a hint only -- see ingest.py.

Spark VALIDATED 2026-08-08 against openlineage-spark 1.52.0 on Spark 3.5.7. The
naming here was wrong and is corrected:

  guessed                                          actual
  dbt_spark_analytics.fct_orders                   dbt_spark_analytics       (APPLICATION)
  <app>.<model>.execute_insert_into_...            <app>.<command>.<db>_<table>  (SQL_JOB)

And, as predicted from the dbt lesson, **openlineage-spark also reports `root` as
its own parent** rather than the true top of the tree. Two independent
integrations get this wrong, which is why the correlator treats `root` as a hint
and resolves the chain itself.

WHERE THIS SIMULATOR KNOWINGLY DIVERGES: it models one Spark application per dbt
model, whereas a real dbt-spark session runs many SQL executions inside a single
long-lived application. The per-model split keeps job names unique and the tree
readable; the correlator does not care either way, since it keys on the parent
facet. Real captures live in tests/fixtures/spark_openlineage_1.52.0.json.
"""

from __future__ import annotations

import os
import random
import time
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

PRODUCER = "https://github.com/aidancorrell/wishd"
SPEC = "https://openlineage.io/spec/2-0-2/OpenLineage.json"
FACET_BASE = "https://openlineage.io/spec/facets"

AIRFLOW_NS = "prod-airflow"
DBT_NS = "dbt://analytics"
SPARK_NS = "spark://emr-j-1A2B3C4D5E6F"
LAKE_NS = "s3://acme-lakehouse"


def uuid7(at: datetime | None = None) -> UUID:
    """UUIDv7 stamped at `at`, matching what real producers emit.

    The spec recommends v7 and the Airflow provider uses it. Generating v4 here
    would leave a realism gap with teeth: v7 sorts by creation time, so any code
    that (wrongly) relies on run_id ordering would pass against the simulator and
    fail in production. Python 3.12 has no uuid7, so: 48-bit ms timestamp, then
    random, with the version and variant bits set.

    `at` exists for the same reason the function does. Seeded history is
    backdated across several days, and stamping every id at *generation* time
    left five days of runs sharing one leading hex run -- something no real
    producer would ever emit, and which made the truncated ids in `dataspine
    runs` collide with each other. Passing the instant the run stands for keeps
    the ids sorting and truncating the way production ids do.
    """
    when = at.timestamp() if at is not None else time.time()
    ms = int(when * 1000) & ((1 << 48) - 1)
    rand = int.from_bytes(os.urandom(10), "big")
    value = (ms << 80) | rand
    value &= ~(0xF000 << 64)          # clear version nibble
    value |= 0x7000 << 64             # version 7
    value &= ~(0xC000 << 48)          # clear variant bits
    value |= 0x8000 << 48             # RFC 4122 variant
    return UUID(int=value)


def _base(schema_url: str) -> dict[str, str]:
    return {"_producer": PRODUCER, "_schemaURL": schema_url}


def _job_type(integration: str, job_type: str, processing: str = "BATCH") -> dict[str, Any]:
    return {
        "jobType": {
            **_base(f"{FACET_BASE}/2-0-4/JobTypeJobFacet.json"),
            "processingType": processing,
            "integration": integration,
            "jobType": job_type,
        }
    }


def _parent(
    parent_run: UUID,
    parent_ns: str,
    parent_name: str,
    root_run: UUID,
    root_ns: str,
    root_name: str,
) -> dict[str, Any]:
    """ParentRunFacet, including the `root` block that newer integrations emit.

    We include `root` on purpose: it lets the gateway resolve a deep run to its
    top-level DAG run in one hop instead of walking the chain, which matters
    when the intermediate levels have not arrived yet.
    """
    return {
        "parent": {
            **_base(f"{FACET_BASE}/1-2-0/ParentRunFacet.json"),
            "run": {"runId": str(parent_run)},
            "job": {"namespace": parent_ns, "name": parent_name},
            "root": {
                "run": {"runId": str(root_run)},
                "job": {"namespace": root_ns, "name": root_name},
            },
        }
    }


def _processing_engine(name: str, version: str, adapter: str) -> dict[str, Any]:
    return {
        "processing_engine": {
            **_base(f"{FACET_BASE}/1-1-1/ProcessingEngineRunFacet.json"),
            "name": name,
            "version": version,
            "openlineageAdapterVersion": adapter,
        }
    }


def _schema(fields: list[tuple[str, str]]) -> dict[str, Any]:
    return {
        "schema": {
            **_base(f"{FACET_BASE}/1-2-0/SchemaDatasetFacet.json"),
            "fields": [{"name": n, "type": t} for n, t in fields],
        }
    }


def _output_stats(rows: int, size: int) -> dict[str, Any]:
    return {
        "outputStatistics": {
            **_base(f"{FACET_BASE}/1-0-2/OutputStatisticsOutputDatasetFacet.json"),
            "rowCount": rows,
            "size": size,
        }
    }


def _event(
    event_type: str,
    when: datetime,
    run_id: UUID,
    namespace: str,
    name: str,
    run_facets: dict[str, Any] | None = None,
    job_facets: dict[str, Any] | None = None,
    inputs: list[dict[str, Any]] | None = None,
    outputs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "eventTime": when.isoformat(),
        "producer": PRODUCER,
        "schemaURL": f"{SPEC}#/$defs/RunEvent",
        "eventType": event_type,
        "run": {"runId": str(run_id), "facets": run_facets or {}},
        "job": {"namespace": namespace, "name": name, "facets": job_facets or {}},
        "inputs": inputs or [],
        "outputs": outputs or [],
    }


# ------------------------------------------------------------------- scenarios

STG_ORDERS = f"{LAKE_NS}|warehouse/staging/stg_orders"
STG_CUSTOMERS = f"{LAKE_NS}|warehouse/staging/stg_customers"


def _dataset(ref: str, facets: dict[str, Any] | None = None, **extra: Any) -> dict[str, Any]:
    ns, name = ref.split("|", 1)
    return {"namespace": ns, "name": name, "facets": facets or {}, **extra}


def build_pipeline(
    *,
    dag_id: str = "analytics_daily",
    task_id: str = "dbt_run_marts",
    models: tuple[str, ...] = ("fct_orders", "fct_order_items", "dim_customers"),
    fail_model: str | None = None,
    start: datetime | None = None,
) -> list[dict[str, Any]]:
    """One full DAG run, as the ~40 events the real integrations would emit.

    If `fail_model` is set, that model's Spark execution fails and the failure
    propagates up to the DAG run -- which is the interesting case, because it is
    the one where you currently have to go read EMR logs by hand.
    """
    start = start or datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=45)
    t = start
    events: list[dict[str, Any]] = []

    dag_run = uuid7(t)
    task_run = uuid7(t)
    dbt_run = uuid7(t)

    dag_name = dag_id
    task_name = f"{dag_id}.{task_id}"
    dbt_name = "dbt-run-analytics"

    nominal = {
        "nominalTime": {
            **_base(f"{FACET_BASE}/1-0-1/NominalTimeRunFacet.json"),
            "nominalStartTime": start.replace(hour=2, minute=0, second=0).isoformat(),
        }
    }

    airflow_engine = _processing_engine("Airflow", "3.0.2", "2.19.0")
    # The real provider derives these from DAG tags and DAG owner.
    airflow_job_extras = {
        "ownership": {
            **_base(f"{FACET_BASE}/1-0-0/OwnershipJobFacet.json"),
            "owners": [{"name": "airflow"}],
        },
        "tags": {
            **_base(f"{FACET_BASE}/1-0-0/TagsJobFacet.json"),
            "tags": [{"key": "dataspine", "value": "dataspine", "source": "AIRFLOW"}],
        },
    }

    # --- DAG start ----------------------------------------------------------
    events.append(
        _event("START", t, dag_run, AIRFLOW_NS, dag_name,
               run_facets={**nominal, **airflow_engine},
               job_facets={**_job_type("AIRFLOW", "DAG"), **airflow_job_extras})
    )
    t += timedelta(seconds=2)

    # --- task start ---------------------------------------------------------
    task_parent = _parent(dag_run, AIRFLOW_NS, dag_name, dag_run, AIRFLOW_NS, dag_name)
    events.append(
        _event("START", t, task_run, AIRFLOW_NS, task_name,
               run_facets={**task_parent, **airflow_engine},
               job_facets={**_job_type("AIRFLOW", "TASK"), **airflow_job_extras})
    )
    t += timedelta(seconds=5)

    # --- dbt invocation -----------------------------------------------------
    # The Airflow task exports OPENLINEAGE_PARENT_* into the environment, which
    # is how dbt claims the task as its parent. Get this wrong and the two
    # halves of the pipeline become two unrelated trees -- this env handoff is
    # the single most fragile link in the chain.
    dbt_parent = _parent(task_run, AIRFLOW_NS, task_name, dag_run, AIRFLOW_NS, dag_name)
    events.append(
        _event("START", t, dbt_run, DBT_NS, dbt_name,
               run_facets={**dbt_parent, **_processing_engine("dbt", "1.9.1", "1.27.0")},
               job_facets=_job_type("DBT", "JOB"))
    )
    t += timedelta(seconds=3)

    failed = False
    for model in models:
        model_run = uuid7(t)
        spark_app_run = uuid7(t)
        spark_sql_run = uuid7(t)
        model_name = f"model.analytics.{model}"
        spark_app_name = f"dbt_spark_analytics.{model}"
        # Real pattern: <appName>.<lowercased command>.<db>_<table>
        spark_sql_name = (
            f"dbt_spark_analytics.execute_insert_into_hadoop_fs_relation_command"
            f".warehouse_{model}"
        )
        this_fails = model == fail_model

        model_parent = _parent(dbt_run, DBT_NS, dbt_name, dag_run, AIRFLOW_NS, dag_name)
        sql = (
            f"insert overwrite table analytics.{model}\n"
            f"select o.order_id, o.customer_id, c.segment, o.order_total\n"
            f"from analytics.stg_orders o\n"
            f"join analytics.stg_customers c on c.customer_id = o.customer_id"
        )
        model_job_facets = {
            **_job_type("DBT", "MODEL"),
            "sql": {**_base(f"{FACET_BASE}/1-1-0/SQLJobFacet.json"), "query": sql},
        }

        events.append(
            _event("START", t, model_run, DBT_NS, model_name,
                   run_facets=model_parent, job_facets=model_job_facets)
        )
        t += timedelta(seconds=1)

        # --- Spark application, launched by dbt against the EMR thrift server
        spark_parent = _parent(model_run, DBT_NS, model_name, dag_run, AIRFLOW_NS, dag_name)
        spark_engine = _processing_engine("spark", "3.5.1", "1.27.0")
        events.append(
            _event("START", t, spark_app_run, SPARK_NS, spark_app_name,
                   run_facets={**spark_parent, **spark_engine},
                   job_facets=_job_type("SPARK", "APPLICATION"))
        )
        t += timedelta(seconds=2)

        # --- Spark SQL execution: the level that carries real dataset lineage
        sql_parent = _parent(
            spark_app_run, SPARK_NS, spark_app_name, dag_run, AIRFLOW_NS, dag_name
        )
        inputs = [
            _dataset(STG_ORDERS, _schema([("order_id", "bigint"),
                                          ("customer_id", "bigint"),
                                          ("order_total", "decimal(18,2)")])),
            _dataset(STG_CUSTOMERS, _schema([("customer_id", "bigint"),
                                             ("segment", "string")])),
        ]
        # The Spark integration carries the query text for SQL executions, so the
        # facet appears at this level as well as on the dbt model above it.
        spark_sql_facets = {
            **_job_type("SPARK", "SQL_JOB"),
            "sql": {**_base(f"{FACET_BASE}/1-1-0/SQLJobFacet.json"), "query": sql},
        }
        events.append(
            _event("START", t, spark_sql_run, SPARK_NS, spark_sql_name,
                   run_facets={**sql_parent, **spark_engine},
                   job_facets=spark_sql_facets,
                   inputs=inputs)
        )
        duration = timedelta(seconds=random.randint(40, 400))
        t += duration

        if this_fails:
            failed = True
            err = {
                "errorMessage": {
                    **_base(f"{FACET_BASE}/1-0-1/ErrorMessageRunFacet.json"),
                    "message": (
                        "org.apache.spark.SparkException: Job aborted due to stage failure: "
                        "Task 137 in stage 12.0 failed 4 times, most recent failure: "
                        "ExecutorLostFailure (executor 9 exited caused by one of the running "
                        "tasks) Reason: Container killed by YARN for exceeding physical memory "
                        "limits. 11.4 GB of 11.0 GB physical memory used."
                    ),
                    "programmingLanguage": "SCALA",
                    "stackTrace": (
                        "at org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages"
                        "(DAGScheduler.scala:2856)\n"
                        "\tat org.apache.spark.sql.execution.datasources."
                        "InsertIntoHadoopFsRelationCommand.run(InsertIntoHadoopFsRelation"
                        "Command.scala:188)"
                    ),
                }
            }
            events.append(_event("FAIL", t, spark_sql_run, SPARK_NS, spark_sql_name,
                                 run_facets={**sql_parent, **err}, inputs=inputs))
            events.append(_event("FAIL", t + timedelta(seconds=1), spark_app_run,
                                 SPARK_NS, spark_app_name, run_facets=spark_parent))
            events.append(_event("FAIL", t + timedelta(seconds=2), model_run,
                                 DBT_NS, model_name,
                                 run_facets={**model_parent, **err}))
            t += timedelta(seconds=3)
            break

        rows = random.randint(800_000, 2_000_000)
        outputs = [
            _dataset(
                f"{LAKE_NS}|warehouse/marts/{model}",
                _schema([("order_id", "bigint"), ("customer_id", "bigint"),
                         ("segment", "string"), ("order_total", "decimal(18,2)")]),
                outputFacets=_output_stats(rows, rows * 32),
            )
        ]
        events.append(_event("COMPLETE", t, spark_sql_run, SPARK_NS, spark_sql_name,
                             run_facets=sql_parent, inputs=inputs, outputs=outputs))
        events.append(_event("COMPLETE", t + timedelta(seconds=1), spark_app_run,
                             SPARK_NS, spark_app_name, run_facets=spark_parent))
        events.append(_event("COMPLETE", t + timedelta(seconds=2), model_run,
                             DBT_NS, model_name, run_facets=model_parent,
                             outputs=outputs))
        t += timedelta(seconds=4)

    final = "FAIL" if failed else "COMPLETE"
    events.append(_event(final, t, dbt_run, DBT_NS, dbt_name, run_facets=dbt_parent))
    t += timedelta(seconds=2)
    events.append(_event(final, t, task_run, AIRFLOW_NS, task_name, run_facets=task_parent))
    t += timedelta(seconds=1)
    events.append(_event(final, t, dag_run, AIRFLOW_NS, dag_name, run_facets=nominal))

    return events


def shuffle_events(events: list[dict[str, Any]], seed: int | None = None) -> list[dict[str, Any]]:
    """Deliver events in a deliberately hostile order.

    Real producers buffer, retry and race. If the run tree only assembles when
    events arrive in causal order, it does not work.
    """
    rng = random.Random(seed)
    out = list(events)
    rng.shuffle(out)
    return out


def truncate_at(events: list[dict[str, Any]], cutoff: datetime) -> list[dict[str, Any]]:
    """Only the events a producer would have sent by `cutoff`.

    This is how you get a pipeline that is genuinely *in flight* rather than one
    that is finished and relabelled: drop every event that has not happened yet,
    and the runs whose START arrived but whose COMPLETE did not are left RUNNING,
    exactly as they would be if you looked at a real pipeline mid-execution.

    It matters that this is a truncation and not a special "running" scenario.
    A hand-built running fixture encodes whatever we *think* an in-flight tree
    looks like; a truncated real one encodes what the event sequence actually
    produces -- including the awkward part, which is that a parent often reports
    COMPLETE before its children do, so mid-flight trees are not simply "the top
    N levels done".
    """
    return [e for e in events if datetime.fromisoformat(e["eventTime"]) <= cutoff]
