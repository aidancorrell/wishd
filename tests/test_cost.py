"""Cost: ingesting the bill, and attributing it down to a dbt model.

The target output the roadmap set:

    "fct_orders cost $34 to build last night, up 4x from last week, because a
     broadcast join spilled across 180 tasks."

Getting there is three divisions, and each one is a place to be wrong quietly:

  **Bill -> cluster.** AWS Cost and Usage Report rows carry `resourceTags/...`,
  not cluster ids, so the tags recorded by the lifecycle sync are the join key.

  **Cluster -> application.** A cluster's hourly cost is split across the
  applications that ran on it *by core-seconds*, which come free from the Spark
  event log. Splitting evenly would charge a five-minute job the same as an
  eight-hour one.

  **Application -> model.** The run tree already says which dbt model spawned
  which Spark application, so this division is a join the spine paid for in
  Phase 00.

The tests lean hard on two rules. **Idle cluster time is nobody's cost** — a
cluster that sat empty for six hours has real spend that no model caused, and
silently spreading it across whatever ran that day would make every model's cost
depend on how idle the cluster happened to be. And **an unpriced run says so**
rather than reporting zero; a cost of $0 and a cost we do not know look identical
on a dashboard and mean opposite things.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from dataspine import cost, resources

NOW = datetime(2026, 8, 9, 12, 0, tzinfo=UTC)
CLUSTER = "j-2ABCDEFGHIJKL"


# ------------------------------------------------------------------- fixtures


def _cluster(conn, cluster_id=CLUSTER, *, tags=None, started=None, ended=None):
    resources.store_cluster(
        conn,
        resources.ClusterSpec(
            cluster_id=cluster_id,
            name="analytics-emr",
            started_at=started or NOW - timedelta(hours=6),
            ended_at=ended,
            tags=tags or {"team": "analytics"},
            instance_groups=[
                {"role": "CORE", "instance_type": "r5.2xlarge", "count": 4,
                 "market": "SPOT"},
            ],
        ),
    )
    return cluster_id


def _cur_row(*, start, cost_usd, tags=None, service="AmazonEMR", resource=""):
    """A CUR line item, in the documented column layout."""
    row = {
        "identity/LineItemId": uuid4().hex,
        "lineItem/ProductCode": service,
        "lineItem/UsageStartDate": start.isoformat(),
        "lineItem/UsageEndDate": (start + timedelta(hours=1)).isoformat(),
        "lineItem/UnblendedCost": str(cost_usd),
        "lineItem/ResourceId": resource,
    }
    for key, value in (tags or {"team": "analytics"}).items():
        row[f"resourceTags/user:{key}"] = value
    return row


def _application(conn, app_id, *, start, seconds, cores=8, cluster_id=CLUSTER, run_id=None):
    conn.execute(
        """
        insert into spark_apps (app_id, run_id, app_name, started_at, ended_at,
                                duration_ms, metrics, cluster_id)
        values (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            app_id,
            run_id,
            app_id,
            start,
            start + timedelta(seconds=seconds),
            seconds * 1000,
            json.dumps({"core_seconds": seconds * cores, "disk_spilled_bytes": 0}),
            cluster_id,
        ),
    )


# ------------------------------------------------------------ CUR ingestion


def test_cur_rows_attach_to_a_cluster_by_tag(conn):
    _cluster(conn, tags={"team": "analytics"})
    written = cost.import_cur(
        conn,
        [_cur_row(start=NOW - timedelta(hours=2), cost_usd=12.50)],
        tag_key="team",
    )

    assert written == 1
    rows = conn.execute("select * from cost_line_items").fetchall()
    assert rows[0]["cluster_id"] == CLUSTER
    assert float(rows[0]["cost_usd"]) == pytest.approx(12.50)


def test_a_cur_row_matching_no_cluster_is_kept_unattributed(conn):
    """Spend we cannot attribute is still spend, and dropping it would make the
    totals quietly disagree with the AWS console — which is how a cost feature
    loses its credibility in one meeting."""
    cost.import_cur(
        conn, [_cur_row(start=NOW, cost_usd=99.0, tags={"team": "nobody"})], tag_key="team"
    )
    row = conn.execute("select * from cost_line_items").fetchone()
    assert row["cluster_id"] is None
    assert float(row["cost_usd"]) == pytest.approx(99.0)


def test_reimporting_the_same_report_does_not_double_the_bill(conn):
    """CUR files are restated through the month; the same hour arrives repeatedly."""
    _cluster(conn)
    rows = [_cur_row(start=NOW - timedelta(hours=2), cost_usd=12.50)]
    for _ in range(5):
        cost.import_cur(conn, rows, tag_key="team")

    total = conn.execute("select sum(cost_usd) as t from cost_line_items").fetchone()["t"]
    assert float(total) == pytest.approx(12.50)


def test_a_restated_row_updates_rather_than_adds(conn):
    """AWS revises costs as reservations and credits are applied. The later value
    is the true one."""
    _cluster(conn)
    row = _cur_row(start=NOW - timedelta(hours=2), cost_usd=12.50)
    cost.import_cur(conn, [row], tag_key="team")
    cost.import_cur(conn, [{**row, "lineItem/UnblendedCost": "9.00"}], tag_key="team")

    total = conn.execute("select sum(cost_usd) as t from cost_line_items").fetchone()["t"]
    assert float(total) == pytest.approx(9.00)


def test_cur_can_be_read_from_a_csv_file(tmp_path, conn):
    _cluster(conn)
    path = tmp_path / "cur.csv"
    path.write_text(
        "identity/LineItemId,lineItem/ProductCode,lineItem/UsageStartDate,"
        "lineItem/UsageEndDate,lineItem/UnblendedCost,resourceTags/user:team\n"
        f"abc,AmazonEMR,{(NOW - timedelta(hours=1)).isoformat()},"
        f"{NOW.isoformat()},7.25,analytics\n"
    )
    assert cost.import_cur_file(conn, path, tag_key="team") == 1


def test_a_malformed_cur_row_is_skipped_not_fatal(conn):
    """A month of billing must not be lost to one unparseable line."""
    _cluster(conn)
    good = _cur_row(start=NOW - timedelta(hours=1), cost_usd=5.0)
    bad = {"identity/LineItemId": "x", "lineItem/UnblendedCost": "not-a-number"}
    assert cost.import_cur(conn, [bad, good], tag_key="team") == 1


# ------------------------------------------------------------- attribution


def test_cluster_cost_splits_across_applications_by_core_seconds(conn):
    """Splitting evenly would charge a five-minute job the same as an eight-hour
    one, which is the entire question this feature exists to answer."""
    _cluster(conn)
    hour = NOW - timedelta(hours=2)
    cost.import_cur(conn, [_cur_row(start=hour, cost_usd=100.0)], tag_key="team")

    _application(conn, "app-big", start=hour + timedelta(minutes=5), seconds=1800, cores=8)
    _application(conn, "app-small", start=hour + timedelta(minutes=5), seconds=600, cores=8)

    cost.attribute(conn)
    costs = {
        r["app_id"]: float(r["cost_usd"])
        for r in conn.execute("select app_id, cost_usd from application_costs").fetchall()
    }

    assert costs["app-big"] == pytest.approx(75.0, rel=0.01)
    assert costs["app-small"] == pytest.approx(25.0, rel=0.01)


def test_idle_cluster_time_is_not_charged_to_anybody(conn):
    """A cluster that sat empty has real spend that no model caused.

    Spreading it silently would make every model's cost depend on how idle the
    cluster happened to be that day — a number that moves for reasons no dbt
    author can act on.
    """
    _cluster(conn)
    busy = NOW - timedelta(hours=2)
    idle = NOW - timedelta(hours=5)
    cost.import_cur(conn, [_cur_row(start=busy, cost_usd=100.0)], tag_key="team")
    cost.import_cur(conn, [_cur_row(start=idle, cost_usd=100.0)], tag_key="team")
    _application(conn, "app-1", start=busy + timedelta(minutes=5), seconds=600)

    cost.attribute(conn)
    attributed = conn.execute(
        "select coalesce(sum(cost_usd), 0) as t from application_costs"
    ).fetchone()["t"]
    summary = cost.cluster_summary(conn, CLUSTER)

    assert float(attributed) == pytest.approx(100.0)
    assert float(summary["idle_cost_usd"]) == pytest.approx(100.0)


def test_an_application_spanning_two_hours_is_charged_from_both(conn):
    _cluster(conn)
    first = NOW - timedelta(hours=3)
    cost.import_cur(conn, [_cur_row(start=first, cost_usd=60.0)], tag_key="team")
    cost.import_cur(
        conn, [_cur_row(start=first + timedelta(hours=1), cost_usd=60.0)], tag_key="team"
    )
    _application(conn, "app-1", start=first + timedelta(minutes=30), seconds=3600)

    cost.attribute(conn)
    total = conn.execute("select sum(cost_usd) as t from application_costs").fetchone()["t"]
    assert float(total) == pytest.approx(120.0)


def test_an_application_with_no_cost_data_is_unpriced_not_free(conn):
    """A cost of $0 and a cost we do not know look identical on a dashboard and
    mean opposite things."""
    _cluster(conn)
    _application(conn, "app-1", start=NOW - timedelta(hours=1), seconds=600)

    cost.attribute(conn)
    assert conn.execute("select count(*) as n from application_costs").fetchone()["n"] == 0
    assert cost.for_application(conn, "app-1") is None


def test_attribution_is_idempotent(conn):
    _cluster(conn)
    hour = NOW - timedelta(hours=2)
    cost.import_cur(conn, [_cur_row(start=hour, cost_usd=50.0)], tag_key="team")
    _application(conn, "app-1", start=hour + timedelta(minutes=5), seconds=600)

    for _ in range(4):
        cost.attribute(conn)
    total = conn.execute("select sum(cost_usd) as t from application_costs").fetchone()["t"]
    assert float(total) == pytest.approx(50.0)


# ------------------------------------------------------------ cost per model


def _pipeline(conn, *, model="fct_orders", app_id="app-1", at=None, seconds=600):
    """An Airflow -> dbt -> Spark tree, as the correlator assembles it."""
    at = at or NOW - timedelta(hours=2)
    dbt_job = conn.execute(
        "insert into jobs (namespace, name, integration) values ('t', %s, 'DBT') "
        "on conflict (namespace, name) do update set name = excluded.name returning id",
        (f"model.analytics.{model}",),
    ).fetchone()["id"]
    spark_job = conn.execute(
        "insert into jobs (namespace, name, integration) values ('t', %s, 'SPARK') "
        "on conflict (namespace, name) do update set name = excluded.name returning id",
        (f"spark.{model}",),
    ).fetchone()["id"]

    root = uuid4()
    conn.execute(
        "insert into runs (run_id, job_id, root_run_id, state, started_at, ended_at) "
        "values (%s, %s, %s, 'COMPLETED', %s, %s)",
        (root, dbt_job, root, at, at + timedelta(seconds=seconds)),
    )
    child = uuid4()
    conn.execute(
        "insert into runs (run_id, job_id, parent_run_id, root_run_id, state, "
        "started_at, ended_at) values (%s, %s, %s, %s, 'COMPLETED', %s, %s)",
        (child, spark_job, root, root, at, at + timedelta(seconds=seconds)),
    )
    _application(conn, app_id, start=at, seconds=seconds, run_id=child)
    return root, child


def test_cost_reaches_the_dbt_model_through_the_run_tree(conn):
    """The payoff, and the reason Phase 00 came first: nothing in a Spark event
    log or an AWS bill knows what a dbt model is."""
    _cluster(conn)
    hour = NOW - timedelta(hours=2)
    cost.import_cur(conn, [_cur_row(start=hour, cost_usd=34.0)], tag_key="team")
    root, _ = _pipeline(conn, at=hour + timedelta(minutes=5))

    cost.attribute(conn)
    run_cost = cost.for_run(conn, root)

    assert run_cost is not None
    assert float(run_cost["cost_usd"]) == pytest.approx(34.0)


def test_a_run_with_several_spark_applications_sums_them(conn):
    _cluster(conn)
    hour = NOW - timedelta(hours=2)
    cost.import_cur(conn, [_cur_row(start=hour, cost_usd=100.0)], tag_key="team")

    root, child = _pipeline(conn, app_id="app-1", at=hour + timedelta(minutes=5))
    _application(conn, "app-2", start=hour + timedelta(minutes=5), seconds=600,
                 run_id=child)

    cost.attribute(conn)
    assert float(cost.for_run(conn, root)["cost_usd"]) == pytest.approx(100.0)


def test_cost_per_model_aggregates_across_runs(conn):
    _cluster(conn)
    for index, hour in enumerate([NOW - timedelta(hours=5), NOW - timedelta(hours=3)]):
        cost.import_cur(conn, [_cur_row(start=hour, cost_usd=20.0)], tag_key="team")
        _pipeline(conn, app_id=f"app-{index}", at=hour + timedelta(minutes=5))

    cost.attribute(conn)
    by_job = {r["job_name"]: float(r["cost_usd"]) for r in cost.by_job(conn)}
    assert by_job["model.analytics.fct_orders"] == pytest.approx(40.0)


def test_the_headline_sentence_is_answerable(conn):
    """The roadmap's target output, end to end.

    "fct_orders cost $34 to build last night" — the number, attached to the model
    name a dbt author would recognise, derived from an AWS bill that never heard
    of dbt.
    """
    _cluster(conn)
    hour = NOW - timedelta(hours=2)
    cost.import_cur(conn, [_cur_row(start=hour, cost_usd=34.0)], tag_key="team")
    _pipeline(conn, at=hour + timedelta(minutes=5))

    cost.attribute(conn)
    summary = cost.by_job(conn)[0]

    assert summary["job_name"] == "model.analytics.fct_orders"
    assert float(summary["cost_usd"]) == pytest.approx(34.0)
    assert summary["runs"] == 1


# --------------------------------------------------------- query attribution


def _real_shape(conn, *, at, models, app_id="app-1", seconds=600):
    """The tree shape the *real* captures produce.

    This matters and is easy to get wrong. The Spark application is parented to
    the Airflow task and sits *beside* the dbt models, not beneath them — one
    long-lived application (a thrift session) serves many models. The models'
    own Spark work appears as SQL-execution runs underneath each model.

    So application cost cannot simply roll up to "the parent job": that lands on
    the Airflow task and never reaches a model. It has to be split across the
    models by the query work each one did — the roadmap's third division.
    """
    def job(name, integration, job_type):
        return conn.execute(
            "insert into jobs (namespace, name, integration, job_type) "
            "values ('t', %s, %s, %s) "
            "on conflict (namespace, name) do update set job_type = excluded.job_type "
            "returning id",
            (name, integration, job_type),
        ).fetchone()["id"]

    def run(job_id, parent, root, start, length):
        run_id = uuid4()
        conn.execute(
            "insert into runs (run_id, job_id, parent_run_id, root_run_id, state, "
            "started_at, ended_at) values (%s,%s,%s,%s,'COMPLETED',%s,%s)",
            (run_id, job_id, parent, root or run_id, start, start + timedelta(seconds=length)),
        )
        return run_id

    dag = run(job("analytics_daily", "AIRFLOW", "DAG"), None, None, at, seconds)
    task = run(job("analytics_daily.dbt_run_marts", "AIRFLOW", "TASK"), dag, dag, at, seconds)

    # The Spark application: a sibling of the models, parented to the task.
    app_run = run(job("dbt_spark_analytics", "SPARK", "APPLICATION"), task, dag, at, seconds)
    _application(conn, app_id, start=at, seconds=seconds, run_id=app_run)

    for name, share in models.items():
        model_run = run(job(f"model.analytics.{name}", "DBT", "MODEL"), task, dag, at,
                        int(seconds * share))
        run(job(f"dbt_spark_analytics.{name}", "SPARK", "SQL_JOB"), model_run, dag, at,
            int(seconds * share))
    return dag


def test_application_cost_is_split_across_the_models_that_used_it(conn):
    """One Spark application, three dbt models, split by the query work each did.

    Without this the whole bill lands on the Airflow task and "cost per model" —
    the roadmap's actual target — is unanswerable.
    """
    _cluster(conn)
    hour = NOW - timedelta(hours=2)
    cost.import_cur(conn, [_cur_row(start=hour, cost_usd=100.0)], tag_key="team")
    _real_shape(conn, at=hour + timedelta(minutes=5),
                models={"fct_orders": 0.5, "stg_orders": 0.3, "dim_customers": 0.2})

    cost.attribute(conn)
    by_model = {r["job_name"]: float(r["cost_usd"]) for r in cost.by_job(conn)}

    assert by_model["model.analytics.fct_orders"] == pytest.approx(50.0, rel=0.02)
    assert by_model["model.analytics.stg_orders"] == pytest.approx(30.0, rel=0.02)
    assert by_model["model.analytics.dim_customers"] == pytest.approx(20.0, rel=0.02)


def test_the_split_conserves_the_total(conn):
    """Attribution must not create or destroy money."""
    _cluster(conn)
    hour = NOW - timedelta(hours=2)
    cost.import_cur(conn, [_cur_row(start=hour, cost_usd=100.0)], tag_key="team")
    _real_shape(conn, at=hour + timedelta(minutes=5),
                models={"a": 0.5, "b": 0.5})

    cost.attribute(conn)
    total = sum(float(r["cost_usd"]) for r in cost.by_job(conn))
    assert total == pytest.approx(100.0, rel=0.01)


def test_an_application_with_no_models_still_reports_its_cost(conn):
    """A bare spark-submit has no dbt models under it. Its cost must not vanish
    just because there is nothing to split it across."""
    _cluster(conn)
    hour = NOW - timedelta(hours=2)
    cost.import_cur(conn, [_cur_row(start=hour, cost_usd=40.0)], tag_key="team")
    _pipeline(conn, at=hour + timedelta(minutes=5))

    cost.attribute(conn)
    total = sum(float(r["cost_usd"]) for r in cost.by_job(conn))
    assert total == pytest.approx(40.0)
