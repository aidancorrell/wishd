"""Cost monitors, other platforms, and the pull-request blast radius.

Three things that all reuse machinery already built, which is the point:

  **Cost monitors** are a monitor kind. Spend regression alerting needs no new
  detector — `mode: anomaly` was built in Phase 03 and a cost series is just
  another numeric series. "Up 4x from last week" falls out of the seasonal
  baseline for free.

  **Databricks and Dataproc** are the same three divisions as EMR with a
  different bill format. Their usage rows normalise into `cost_line_items`, so
  attribution never learns there is more than one cloud.

  **The PR comment** is the lineage graph, addressed to someone about to merge.
  It is the one place in the product where the answer arrives before the damage
  rather than after it.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from dataspine import checks, cost, identity, lineage, monitors, pr, resources

NOW = datetime(2026, 8, 9, 12, 0, tzinfo=UTC)
CLUSTER = "j-2ABCDEFGHIJKL"


def _priced_run(conn, *, job="model.analytics.fct_orders", usd, at, app_id=None):
    """A dbt run with a Spark application under it, already priced."""
    app_id = app_id or f"app-{uuid4().hex[:8]}"
    job_id = conn.execute(
        "insert into jobs (namespace, name, integration) values ('t', %s, 'DBT') "
        "on conflict (namespace, name) do update set name = excluded.name returning id",
        (job,),
    ).fetchone()["id"]
    spark_job = conn.execute(
        "insert into jobs (namespace, name, integration) values ('t', %s, 'SPARK') "
        "on conflict (namespace, name) do update set name = excluded.name returning id",
        (f"spark.{job}",),
    ).fetchone()["id"]

    root = uuid4()
    conn.execute(
        "insert into runs (run_id, job_id, root_run_id, state, started_at, ended_at) "
        "values (%s, %s, %s, 'COMPLETED', %s, %s)",
        (root, job_id, root, at, at + timedelta(minutes=10)),
    )
    child = uuid4()
    conn.execute(
        "insert into runs (run_id, job_id, parent_run_id, root_run_id, state, "
        "started_at, ended_at) values (%s, %s, %s, %s, 'COMPLETED', %s, %s)",
        (child, spark_job, root, root, at, at + timedelta(minutes=10)),
    )
    conn.execute(
        "insert into spark_apps (app_id, run_id, started_at, ended_at, metrics) "
        "values (%s, %s, %s, %s, %s)",
        (app_id, child, at, at + timedelta(minutes=10),
         json.dumps({"core_seconds": 600})),
    )
    conn.execute(
        "insert into application_costs (app_id, cost_usd, core_seconds) values (%s, %s, %s)",
        (app_id, usd, 600),
    )
    return root


# ------------------------------------------------------------- cost monitors


def _monitor(conn, name, job, config, *, mode="threshold"):
    monitors.apply_specs(
        conn,
        [monitors.MonitorSpec(name, "cost_per_run", "job", job, config,
                              source=f"{name}.yml", mode=mode)],
        sources=[f"{name}.yml"],
    )
    return monitors.get_monitor(conn, name)


def test_a_cost_monitor_breaches_on_an_expensive_run(conn):
    _priced_run(conn, usd=140.0, at=NOW - timedelta(hours=1))
    monitor = _monitor(conn, "orders_cost", "model.analytics.fct_orders", {"max": 50})

    result = checks.evaluate(conn, monitor, now=NOW)
    assert result["status"] == "breach"
    assert result["value"] == pytest.approx(140.0)


def test_a_cost_monitor_is_quiet_within_budget(conn):
    _priced_run(conn, usd=12.0, at=NOW - timedelta(hours=1))
    monitor = _monitor(conn, "orders_cost", "model.analytics.fct_orders", {"max": 50})
    assert checks.evaluate(conn, monitor, now=NOW)["status"] == "ok"


def test_an_unpriced_run_is_insufficient_data_not_a_pass(conn):
    """Cost attribution needs a bill. Before one is imported, a cost monitor must
    not report a clean bill of health it has no basis for."""
    job_id = conn.execute(
        "insert into jobs (namespace, name, integration) "
        "values ('t','model.analytics.fct_orders','DBT') returning id"
    ).fetchone()["id"]
    run = uuid4()
    conn.execute(
        "insert into runs (run_id, job_id, root_run_id, state, started_at, ended_at) "
        "values (%s, %s, %s, 'COMPLETED', %s, %s)",
        (run, job_id, run, NOW - timedelta(hours=1), NOW),
    )
    monitor = _monitor(conn, "orders_cost", "model.analytics.fct_orders", {"max": 50})
    assert checks.evaluate(conn, monitor, now=NOW)["status"] == "insufficient_data"


def test_spend_regression_needs_no_new_detector(conn):
    """"Up 4x from last week" is the Phase 03 anomaly detector over a cost series.

    Building a second, cost-specific regression detector would have been a
    parallel thing to maintain that answers the same question worse.
    """
    for day in range(10):
        _priced_run(conn, usd=10.0 + (day % 2), at=NOW - timedelta(days=10 - day))
    _priced_run(conn, usd=140.0, at=NOW - timedelta(minutes=30))

    monitor = _monitor(conn, "orders_spend", "model.analytics.fct_orders", {},
                       mode="anomaly")
    result = checks.evaluate(conn, monitor, now=NOW)

    assert result["status"] == "breach"
    assert "140" in result["message"]


def test_cost_per_run_spec_needs_a_bound_in_threshold_mode():
    with pytest.raises(monitors.SpecError, match="needs `min`, `max`, or both"):
        monitors.parse_spec(
            {"monitors": [{"name": "c", "kind": "cost_per_run", "job": "j"}]}
        )


def test_cost_per_run_supports_anomaly_mode():
    specs = monitors.parse_spec(
        {"monitors": [{"name": "c", "kind": "cost_per_run", "job": "j",
                       "mode": "anomaly"}]}
    )
    assert specs[0].mode == "anomaly"


# ------------------------------------------------------- other cloud billing


def test_databricks_usage_rows_normalise_into_the_same_table(conn):
    """`system.billing.usage` gives quantity and list price separately.

    Normalising here rather than teaching attribution about DBUs means the three
    divisions never learn there is more than one cloud.
    """
    resources.store_cluster(
        conn,
        resources.ClusterSpec(cluster_id="0806-cluster", platform="databricks",
                              started_at=NOW - timedelta(hours=6),
                              tags={"team": "analytics"}),
    )
    written = cost.import_usage(
        conn,
        platform="databricks",
        rows=[{
            "record_id": "r1",
            "usage_start_time": (NOW - timedelta(hours=2)).isoformat(),
            "usage_end_time": (NOW - timedelta(hours=1)).isoformat(),
            "usage_quantity": 10,
            "list_price": 0.55,
            "custom_tags": {"team": "analytics"},
        }],
        tag_key="team",
    )

    assert written == 1
    row = conn.execute("select * from cost_line_items").fetchone()
    assert float(row["cost_usd"]) == pytest.approx(5.5)
    assert row["cluster_id"] == "0806-cluster"


def test_dataproc_billing_export_rows_normalise_too(conn):
    """GCP's BigQuery billing export uses `cost` and a `labels` array."""
    resources.store_cluster(
        conn,
        resources.ClusterSpec(cluster_id="dp-1", platform="dataproc",
                              started_at=NOW - timedelta(hours=6),
                              tags={"team": "analytics"}),
    )
    written = cost.import_usage(
        conn,
        platform="dataproc",
        rows=[{
            "record_id": "g1",
            "usage_start_time": (NOW - timedelta(hours=2)).isoformat(),
            "usage_end_time": (NOW - timedelta(hours=1)).isoformat(),
            "cost": 3.25,
            "labels": [{"key": "team", "value": "analytics"}],
        }],
        tag_key="team",
    )
    assert written == 1
    assert float(
        conn.execute("select cost_usd from cost_line_items").fetchone()["cost_usd"]
    ) == pytest.approx(3.25)


def test_normalised_usage_attributes_exactly_like_emr(conn):
    """The proof that normalising was the right seam: attribution is untouched."""
    resources.store_cluster(
        conn,
        resources.ClusterSpec(cluster_id="0806-cluster", platform="databricks",
                              started_at=NOW - timedelta(hours=6),
                              tags={"team": "analytics"}),
    )
    hour = NOW - timedelta(hours=2)
    cost.import_usage(
        conn, platform="databricks",
        rows=[{"record_id": "r1", "usage_start_time": hour.isoformat(),
               "usage_end_time": (hour + timedelta(hours=1)).isoformat(),
               "usage_quantity": 100, "list_price": 1.0,
               "custom_tags": {"team": "analytics"}}],
        tag_key="team",
    )
    conn.execute(
        "insert into spark_apps (app_id, started_at, ended_at, metrics, cluster_id) "
        "values ('a1', %s, %s, %s, '0806-cluster')",
        (hour + timedelta(minutes=5), hour + timedelta(minutes=35),
         json.dumps({"core_seconds": 900})),
    )

    cost.attribute(conn)
    assert float(
        conn.execute("select cost_usd from application_costs").fetchone()["cost_usd"]
    ) == pytest.approx(100.0)


# ------------------------------------------------------ pull-request comment


def _graph(conn):
    """raw -> stg_orders -> fct_orders -> report_daily."""
    def dataset(name):
        return conn.execute(
            "insert into datasets (namespace, name) values ('file', %s) "
            "on conflict (namespace, name) do update set updated_at = now() returning id",
            (f"/warehouse/{name}",),
        ).fetchone()["id"]

    def model(name, inputs=()):
        job = conn.execute(
            "insert into jobs (namespace, name, integration) values ('t', %s, 'SPARK') "
            "on conflict (namespace, name) do update set name = excluded.name returning id",
            (f"build_{name}",),
        ).fetchone()["id"]
        run = uuid4()
        conn.execute(
            "insert into runs (run_id, job_id, root_run_id, state, started_at, ended_at) "
            "values (%s, %s, %s, 'COMPLETED', %s, %s)",
            (run, job, run, NOW, NOW),
        )
        conn.execute(
            "insert into run_datasets (run_id, dataset_id, direction) values (%s,%s,'OUTPUT')",
            (run, dataset(name)),
        )
        for upstream in inputs:
            conn.execute(
                "insert into run_datasets (run_id, dataset_id, direction) "
                "values (%s,%s,'INPUT') on conflict do nothing",
                (run, dataset(upstream)),
            )

    model("raw_orders")
    model("stg_orders", ["raw_orders"])
    model("fct_orders", ["stg_orders"])
    model("report_daily", ["fct_orders"])
    identity.resolve(conn)
    lineage.resolve(conn)


def test_changed_model_paths_resolve_to_tables(conn):
    """dbt PRs change files, not tables. `models/marts/fct_orders.sql` is the
    input a CI job actually has."""
    _graph(conn)
    impact = pr.impact(conn, ["models/marts/fct_orders.sql"])
    assert impact[0]["model"] == "fct_orders"


def test_the_comment_names_the_downstream_blast_radius(conn):
    _graph(conn)
    body = pr.comment(conn, ["models/marts/stg_orders.sql"])

    assert "stg_orders" in body
    assert "fct_orders" in body
    assert "report_daily" in body
    assert "raw_orders" not in body, "upstream is not blast radius"


def test_the_comment_says_so_when_nothing_is_downstream(conn):
    """A leaf model is the common case, and silence would read as a broken job."""
    _graph(conn)
    body = pr.comment(conn, ["models/marts/report_daily.sql"])
    assert "report_daily" in body
    assert "nothing downstream" in body.lower()


def test_an_unknown_model_is_reported_as_unknown(conn):
    """A brand-new model has no lineage yet. Saying "no impact" would be a
    reassurance we have not earned."""
    _graph(conn)
    body = pr.comment(conn, ["models/marts/brand_new.sql"])
    assert "brand_new" in body
    assert "no lineage" in body.lower()


def test_the_comment_carries_cost_when_it_is_known(conn):
    """"This model costs $34 a night and four things depend on it" is a better
    merge decision than either half alone."""
    _graph(conn)
    _priced_run(conn, job="model.analytics.stg_orders", usd=34.0,
                at=NOW - timedelta(hours=2))
    body = pr.comment(conn, ["models/marts/stg_orders.sql"])
    assert "34" in body


def test_the_comment_is_markdown_and_stable(conn):
    _graph(conn)
    body = pr.comment(conn, ["models/marts/stg_orders.sql"])
    assert body.startswith("###")
    assert body == pr.comment(conn, ["models/marts/stg_orders.sql"])


def test_no_changed_models_produces_no_comment(conn):
    """A PR touching only YAML must not post an empty box on every push."""
    _graph(conn)
    assert pr.comment(conn, ["README.md", "dbt_project.yml"]) == ""


def test_the_action_endpoint_returns_a_rendered_comment(api_client):
    """The Action runs on a CI runner with no dataspine install, so rendering
    happens server-side rather than shipping lineage logic into a workflow."""
    from dataspine import db

    with db.connection() as conn:
        _graph(conn)
        conn.commit()

    body = api_client.post(
        "/api/v1/pr/impact", json={"paths": ["models/marts/stg_orders.sql"], "depth": 3}
    ).json()

    assert body["comment"].startswith("### wish:d")
    assert "fct_orders" in body["comment"]
    assert body["impact"][0]["model"] == "stg_orders"


def test_the_action_endpoint_requires_auth(api_client, monkeypatch):
    from dataspine import auth

    monkeypatch.setenv(auth.TOKEN_ENV, "test:secret")
    assert api_client.post("/api/v1/pr/impact", json={"paths": []}).status_code == 401


def test_the_cost_page_reports_idle_separately(api_client):
    """Idle is the largest line most teams can act on, so it gets its own column
    rather than being folded into the models' numbers."""
    from dataspine import db

    with db.connection() as conn:
        resources.store_cluster(
            conn,
            resources.ClusterSpec(cluster_id=CLUSTER, name="analytics-emr",
                                  started_at=NOW - timedelta(hours=6),
                                  tags={"team": "analytics"}),
        )
        conn.execute(
            "insert into cost_line_items (line_item_id, cluster_id, period_start, "
            "period_end, cost_usd) values ('x', %s, %s, %s, 80)",
            (CLUSTER, NOW - timedelta(hours=5), NOW - timedelta(hours=4)),
        )
        conn.commit()

    body = api_client.get("/costs").text
    assert "analytics-emr" in body
    assert "80.00" in body
    assert "Idle" in body


def test_the_cost_page_is_calm_before_any_bill_is_imported(api_client):
    body = api_client.get("/costs").text
    assert "Nothing priced yet" in body


def test_nav_reaches_the_cost_page(api_client):
    assert 'href="/costs"' in api_client.get("/").text
