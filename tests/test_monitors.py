"""Monitor definitions, reconciliation and evaluation.

These are integration tests against a real Postgres for the same reason the
correlator's are: the observation half of a monitor is a query over the run tree,
and testing it against anything else would be testing a different program.

The most important assertions here are the negative ones. A monitoring tool is
judged on what it *doesn't* say -- an empty table that has never run must not
alert, an additive schema change must not page anyone, and a monitor evaluated
twelve times against a job that ran once must hold one observation, not twelve.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from dataspine import checks, monitors

NOW = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)


# ------------------------------------------------------------------- fixtures


def _job(conn, name, *, integration="DBT", job_type="MODEL") -> int:
    return conn.execute(
        """
        insert into jobs (namespace, name, integration, job_type)
        values ('test', %s, %s, %s)
        on conflict (namespace, name) do update set name = excluded.name
        returning id
        """,
        (name, integration, job_type),
    ).fetchone()["id"]


def _schema_facet(columns: dict[str, str]) -> dict:
    """A SchemaDatasetFacet in the shape openlineage-spark 1.52.0 really sends."""
    return {"fields": [{"name": n, "type": t} for n, t in columns.items()]}


def _dataset(conn, namespace, name) -> int:
    return conn.execute(
        """
        insert into datasets (namespace, name, facets)
        values (%s, %s, '{}'::jsonb)
        on conflict (namespace, name) do update set updated_at = now()
        returning id
        """,
        (namespace, name),
    ).fetchone()["id"]


def _run(conn, job_id, *, started, ended=None, state="COMPLETED", root=None, facets=None):
    run_id = uuid4()
    conn.execute(
        """
        insert into runs (run_id, job_id, root_run_id, state, started_at, ended_at, facets)
        values (%s, %s, %s, %s, %s, %s, %s)
        """,
        (run_id, job_id, root or run_id, state, started, ended or started,
         json.dumps(facets or {})),
    )
    return run_id


def _write(conn, run_id, dataset_id, *, rows=None, size=None, columns=None):
    """One OUTPUT write edge.

    `columns` lands on the edge rather than on the dataset, which is what ingest
    does: the schema as of *this* write is the only form drift detection can use.
    """
    facets = {"schema": _schema_facet(columns)} if columns else {}
    conn.execute(
        """
        insert into run_datasets (run_id, dataset_id, direction, facets, row_count, size_bytes)
        values (%s, %s, 'OUTPUT', %s, %s, %s)
        """,
        (run_id, dataset_id, json.dumps(facets), rows, size),
    )


def _monitor(conn, name, kind, target, config, *, target_kind="dataset"):
    spec = monitors.MonitorSpec(
        name=name, kind=kind, target_kind=target_kind, target=target,
        config=config, source="test.yml",
    )
    monitors.apply_specs(conn, [spec], sources=["test.yml"])
    return monitors.get_monitor(conn, name)


# ---------------------------------------------------------------- spec parsing


def test_valid_spec_parses():
    specs = monitors.parse_spec(
        {
            "monitors": [
                {"name": "orders_fresh", "kind": "freshness", "dataset": "fct_orders",
                 "max_age_minutes": 90},
                {"name": "orders_volume", "kind": "row_count", "dataset": "fct_orders",
                 "min": 1000, "schedule": "daily"},
            ]
        }
    )
    assert [s.name for s in specs] == ["orders_fresh", "orders_volume"]
    assert specs[0].config == {"max_age_minutes": 90}
    assert specs[0].target_kind == "dataset"
    assert specs[1].schedule == "daily"


@pytest.mark.parametrize(
    "entry, expected",
    [
        ({"kind": "freshness", "dataset": "x", "max_age_minutes": 5}, "has no `name`"),
        ({"name": "a", "kind": "nonsense", "dataset": "x"}, "expected one of"),
        ({"name": "a", "kind": "freshness", "max_age_minutes": 5}, "needs a `dataset:`"),
        ({"name": "a", "kind": "freshness", "dataset": "x"}, "needs `max_age_minutes`"),
        ({"name": "a", "kind": "row_count", "dataset": "x"}, "needs `min`, `max`, or both"),
        ({"name": "a", "kind": "job_duration", "job": "j", "max_seconds": "soon"},
         "must be a number"),
        ({"name": "a", "kind": "row_count", "dataset": "x", "min": 10, "max": 1},
         "greater than `max`"),
        ({"name": "a", "kind": "freshness", "dataset": "x", "max_age_minute": 5},
         "unknown key"),
    ],
)
def test_invalid_specs_are_refused(entry, expected):
    """Config validation is strict, unlike the ingest path.

    The asymmetry is deliberate: a malformed event is data we would otherwise
    lose, so we keep it; a malformed monitor has produced nothing yet, and quietly
    accepting it yields a monitor that never fires. `max_age_minute` (singular) is
    the case that matters most -- a typo'd threshold key is invisible at runtime.
    """
    with pytest.raises(monitors.SpecError, match=expected):
        monitors.parse_spec({"monitors": [entry]})


def test_duplicate_names_are_refused():
    entry = {"name": "dup", "kind": "freshness", "dataset": "x", "max_age_minutes": 5}
    with pytest.raises(monitors.SpecError, match="duplicate"):
        monitors.parse_spec({"monitors": [entry, dict(entry)]})


def test_load_specs_from_directory_skips_foreign_yaml(tmp_path):
    """A monitors/ directory sits inside a dbt project full of other YAML."""
    (tmp_path / "monitors.yml").write_text(
        "monitors:\n"
        "  - name: a\n    kind: freshness\n    dataset: fct_orders\n    max_age_minutes: 60\n"
    )
    (tmp_path / "dbt_project.yml").write_text("name: analytics\nversion: '1.0'\n")
    specs = monitors.load_specs(tmp_path)
    assert [s.name for s in specs] == ["a"]


# --------------------------------------------------------------- reconciliation


def test_apply_creates_updates_and_disables(conn):
    first = monitors.MonitorSpec(
        name="a", kind="freshness", target_kind="dataset", target="fct_orders",
        config={"max_age_minutes": 60}, source="m.yml",
    )
    second = monitors.MonitorSpec(
        name="b", kind="freshness", target_kind="dataset", target="dim_users",
        config={"max_age_minutes": 60}, source="m.yml",
    )
    outcome = monitors.apply_specs(conn, [first, second], sources=["m.yml"])
    assert outcome["created"] == ["a", "b"]

    # Re-applying unchanged specs must be a no-op, so `apply` is safe in CI.
    outcome = monitors.apply_specs(conn, [first, second], sources=["m.yml"])
    assert outcome["unchanged"] == ["a", "b"]

    first.config = {"max_age_minutes": 30}
    outcome = monitors.apply_specs(conn, [first], sources=["m.yml"])
    assert outcome["updated"] == ["a"]
    # `b` vanished from the file: disabled, not deleted, so its history survives
    # a bad merge.
    assert outcome["disabled"] == ["b"]
    assert monitors.get_monitor(conn, "b")["enabled"] is False


def test_apply_is_scoped_to_the_files_it_was_given(conn):
    """Applying one file must never disable monitors defined in another."""
    monitors.apply_specs(
        conn,
        [monitors.MonitorSpec("a", "freshness", "dataset", "x", {"max_age_minutes": 60},
                              source="one.yml")],
        sources=["one.yml"],
    )
    monitors.apply_specs(
        conn,
        [monitors.MonitorSpec("b", "freshness", "dataset", "y", {"max_age_minutes": 60},
                              source="two.yml")],
        sources=["two.yml"],
    )
    assert monitors.get_monitor(conn, "a")["enabled"] is True
    assert monitors.get_monitor(conn, "b")["enabled"] is True


def test_prune_deletes_and_takes_history_with_it(conn):
    spec = monitors.MonitorSpec("a", "freshness", "dataset", "x", {"max_age_minutes": 60},
                                source="m.yml")
    monitors.apply_specs(conn, [spec], sources=["m.yml"])
    monitor = monitors.get_monitor(conn, "a")
    checks.store_points(conn, monitor["id"], [checks.Point(NOW, "s", 1.0)])

    monitors.apply_specs(conn, [], sources=["m.yml"], prune=True)
    assert monitors.get_monitor(conn, "a") is None
    assert conn.execute("select count(*) as n from metric_points").fetchone()["n"] == 0


# ---------------------------------------------------------- target resolution


def test_one_table_resolves_across_producer_identities(conn):
    """The captures' most consequential surprise, pinned.

    Real openlineage-dbt and openlineage-spark name the same physical table
    completely differently, in the same pipeline. A monitor that resolved to only
    one of them would silently lose either freshness or volume, because dbt sends
    no outputStatistics and Spark sends no dbt-shaped name.
    """
    _dataset(conn, "postgres://postgres:5432", "dataspine.analytics_marts.fct_orders")
    _dataset(conn, "file", "/tmp/warehouse/fct_orders")
    _dataset(conn, "file", "/tmp/warehouse/stg_orders")

    matched = checks.resolve_datasets(conn, "fct_orders")
    assert sorted(m["name"] for m in matched) == [
        "/tmp/warehouse/fct_orders",
        "dataspine.analytics_marts.fct_orders",
    ]


def test_namespace_pins_an_ambiguous_target(conn):
    _dataset(conn, "prod", "warehouse.fct_orders")
    _dataset(conn, "dev", "warehouse.fct_orders")
    matched = checks.resolve_datasets(conn, "fct_orders", namespace="prod")
    assert [m["namespace"] for m in matched] == ["prod"]


def test_exact_job_match_beats_substring(conn):
    """`dbt-run-analytics` is a prefix of `dbt-run-analytics_marts`; a monitor on
    the first must not quietly start watching the second."""
    _job(conn, "dbt-run-analytics")
    _job(conn, "dbt-run-analytics_marts")
    assert [j["name"] for j in checks.resolve_jobs(conn, "dbt-run-analytics")] == [
        "dbt-run-analytics"
    ]
    assert len(checks.resolve_jobs(conn, "analytics")) == 2


# ------------------------------------------------------------------- freshness


def test_freshness_breaches_when_stale_and_says_when_it_last_ran(conn):
    job = _job(conn, "fct_orders")
    dataset = _dataset(conn, "file", "/warehouse/fct_orders")
    run = _run(conn, job, started=NOW - timedelta(hours=6), ended=NOW - timedelta(hours=6))
    _write(conn, run, dataset, rows=100)

    monitor = _monitor(conn, "fresh", "freshness", "fct_orders", {"max_age_minutes": 90})
    result = checks.evaluate(conn, monitor, now=NOW)

    assert result["status"] == "breach"
    assert "6h00m ago" in result["message"]
    assert "limit is 1h30m" in result["message"]


def test_freshness_is_quiet_when_the_table_has_never_been_written(conn):
    """A monitor on a table that has not run yet must not alert.

    This is the single most important negative case in the suite. If a new
    installation's first experience is a wall of red for tables that simply have
    no history, every alert the tool ever sends is discounted.
    """
    _dataset(conn, "file", "/warehouse/fct_orders")
    monitor = _monitor(conn, "fresh", "freshness", "fct_orders", {"max_age_minutes": 90})
    result = checks.evaluate(conn, monitor, now=NOW)
    assert result["status"] == "insufficient_data"


def test_freshness_records_the_gap_between_writes_not_the_age(conn):
    """History is inter-arrival time; staleness is computed at judgement time.

    Storing "how old is it right now" would make the history a record of our
    polling interval rather than of the pipeline.
    """
    job = _job(conn, "fct_orders")
    dataset = _dataset(conn, "file", "/warehouse/fct_orders")
    for hours in (5, 4, 3):
        run = _run(conn, job, started=NOW - timedelta(hours=hours),
                   ended=NOW - timedelta(hours=hours))
        _write(conn, run, dataset, rows=100)

    monitor = _monitor(conn, "fresh", "freshness", "fct_orders", {"max_age_minutes": 300})
    checks.evaluate(conn, monitor, now=NOW)

    points = monitors.recent_points(conn, monitor["id"])
    assert [p["value"] for p in points] == [60.0, 60.0, None]  # newest first; first has no gap


def test_two_producers_reporting_one_write_are_one_observation(conn):
    """The run tree is what tells "reported twice" from "written twice".

    dbt and Spark both report the nightly build of fct_orders, seconds apart,
    under different dataset identities. Counted as two writes, every inter-arrival
    gap halves and a freshness baseline learns this table updates twice a minute.
    """
    dbt_job = _job(conn, "model.analytics.fct_orders")
    spark_job = _job(conn, "spark.fct_orders", integration="SPARK")
    dbt_ds = _dataset(conn, "postgres://db:5432", "analytics.fct_orders")
    spark_ds = _dataset(conn, "file", "/warehouse/fct_orders")

    for hours in (2, 1):
        root = _run(conn, dbt_job, started=NOW - timedelta(hours=hours),
                    ended=NOW - timedelta(hours=hours))
        _write(conn, root, dbt_ds, rows=None)
        child = _run(conn, spark_job, started=NOW - timedelta(hours=hours),
                     ended=NOW - timedelta(hours=hours, seconds=-20), root=root)
        _write(conn, child, spark_ds, rows=500)

    monitor = _monitor(conn, "fresh", "freshness", "fct_orders", {"max_age_minutes": 300})
    checks.evaluate(conn, monitor, now=NOW)

    points = monitors.recent_points(conn, monitor["id"])
    assert len(points) == 2, "one logical write per pipeline execution, not one per producer"
    assert points[0]["value"] == pytest.approx(60.0, abs=1)


# ------------------------------------------------------------------ row counts


def test_row_count_breaches_below_the_floor_with_the_comparison(conn):
    job = _job(conn, "fct_orders")
    dataset = _dataset(conn, "file", "/warehouse/fct_orders")
    for hours, rows in ((2, 10_000), (1, 800)):
        run = _run(conn, job, started=NOW - timedelta(hours=hours),
                   ended=NOW - timedelta(hours=hours))
        _write(conn, run, dataset, rows=rows)

    monitor = _monitor(conn, "vol", "row_count", "fct_orders", {"min": 1000})
    result = checks.evaluate(conn, monitor, now=NOW)

    assert result["status"] == "breach"
    assert "800 rows" in result["message"]
    assert "12.5× fewer" in result["message"]


def test_row_count_ignores_failed_runs(conn):
    """A failed run's row count is whatever it wrote before dying. Feeding that to
    a volume monitor produces an alert about the failure you already knew about."""
    job = _job(conn, "fct_orders")
    dataset = _dataset(conn, "file", "/warehouse/fct_orders")
    ok = _run(conn, job, started=NOW - timedelta(hours=2), ended=NOW - timedelta(hours=2))
    _write(conn, ok, dataset, rows=10_000)
    bad = _run(conn, job, started=NOW - timedelta(hours=1), ended=NOW - timedelta(hours=1),
               state="FAILED")
    _write(conn, bad, dataset, rows=3)

    monitor = _monitor(conn, "vol", "row_count", "fct_orders", {"min": 1000})
    result = checks.evaluate(conn, monitor, now=NOW)
    assert result["status"] == "ok"
    assert result["value"] == 10_000


def test_row_counts_are_not_summed_across_the_tree(conn):
    """One physical write reported at two levels is not a doubling.

    `tree_datasets` learned this the hard way; a volume monitor is where the bug
    would actually page someone.
    """
    dbt_job = _job(conn, "model.analytics.fct_orders")
    spark_job = _job(conn, "spark.fct_orders", integration="SPARK")
    dbt_ds = _dataset(conn, "postgres://db:5432", "analytics.fct_orders")
    spark_ds = _dataset(conn, "file", "/warehouse/fct_orders")

    root = _run(conn, dbt_job, started=NOW - timedelta(hours=1), ended=NOW - timedelta(hours=1))
    _write(conn, root, dbt_ds, rows=1_600_000)
    child = _run(conn, spark_job, started=NOW - timedelta(hours=1),
                 ended=NOW - timedelta(hours=1), root=root)
    _write(conn, child, spark_ds, rows=1_600_000)

    monitor = _monitor(conn, "vol", "row_count", "fct_orders", {"max": 2_000_000})
    result = checks.evaluate(conn, monitor, now=NOW)
    assert result["status"] == "ok"
    assert result["value"] == 1_600_000


# ---------------------------------------------------------------- schema drift


def _write_with_schema(conn, job, dataset, columns, *, at):
    run = _run(conn, job, started=at, ended=at)
    _write(conn, run, dataset, rows=10, columns=columns)


def test_schema_drift_breaches_on_a_removed_column(conn):
    job = _job(conn, "fct_orders")
    dataset = _dataset(conn, "file", "/warehouse/fct_orders")
    _write_with_schema(conn, job, dataset, {"order_id": "integer", "segment": "string"},
                       at=NOW - timedelta(hours=2))
    _write_with_schema(conn, job, dataset, {"order_id": "integer"}, at=NOW - timedelta(hours=1))

    monitor = _monitor(conn, "schema", "schema_drift", "fct_orders", {})
    result = checks.evaluate(conn, monitor, now=NOW)

    assert result["status"] == "breach"
    assert "removed segment" in result["message"]


def test_schema_drift_sees_history_because_the_schema_is_on_the_write_edge(conn):
    """The reason ingest snapshots the schema per write.

    `datasets.facets` is a running merge that only ever holds the current schema.
    Comparing two historical writes through it compares today's columns against
    themselves, and no drift is ever detectable -- so the drift monitor would
    report a permanent, silent all-clear. The per-edge copy is what makes the
    comparison real, and a `dataspine replay` gives it retroactively.
    """
    job = _job(conn, "fct_orders")
    dataset = _dataset(conn, "file", "/warehouse/fct_orders")
    _write_with_schema(conn, job, dataset, {"order_id": "integer", "segment": "string"},
                       at=NOW - timedelta(hours=2))
    _write_with_schema(conn, job, dataset, {"order_id": "integer"}, at=NOW - timedelta(hours=1))

    monitor = _monitor(conn, "schema", "schema_drift", "fct_orders", {})
    checks.evaluate(conn, monitor, now=NOW)
    points = monitors.recent_points(conn, monitor["id"])

    assert [sorted(p["context"]["columns"]) for p in points] == [
        ["order_id"],
        ["order_id", "segment"],
    ], "each write must carry the columns it actually wrote"


def test_schema_drift_does_not_page_on_an_added_column(conn):
    """Adding a column is the most common change in a healthy dbt project. A
    monitor that pages on every additive migration gets muted in week two, taking
    the removals with it."""
    job = _job(conn, "fct_orders")
    dataset = _dataset(conn, "file", "/warehouse/fct_orders")
    _write_with_schema(conn, job, dataset, {"order_id": "integer"}, at=NOW - timedelta(hours=2))
    _write_with_schema(conn, job, dataset, {"order_id": "integer", "customer_id": "integer"},
                       at=NOW - timedelta(hours=1))

    monitor = _monitor(conn, "schema", "schema_drift", "fct_orders", {})
    result = checks.evaluate(conn, monitor, now=NOW)

    assert result["status"] == "ok"
    assert "added customer_id" in result["message"]


def test_schema_drift_is_silent_on_a_dbt_only_stack(conn):
    """openlineage-dbt sends no schema facet at all — verified in the captures.

    Collecting zero points there is correct, and it must read as
    "insufficient_data" rather than as a passing check, or a dbt-only user would
    believe they have schema coverage they do not have.
    """
    job = _job(conn, "fct_orders")
    dataset = _dataset(conn, "postgres://db:5432", "analytics.fct_orders")  # no schema facet
    run = _run(conn, job, started=NOW - timedelta(hours=1), ended=NOW - timedelta(hours=1))
    _write(conn, run, dataset, rows=10)

    monitor = _monitor(conn, "schema", "schema_drift", "fct_orders", {})
    result = checks.evaluate(conn, monitor, now=NOW)
    assert result["status"] == "insufficient_data"


# ---------------------------------------------------------------- job SLOs


def test_job_duration_breaches_and_reports_the_typical(conn):
    job = _job(conn, "dbt-run-analytics")
    for hours, seconds in ((4, 300), (3, 310), (2, 295), (1, 3600)):
        _run(conn, job, started=NOW - timedelta(hours=hours),
             ended=NOW - timedelta(hours=hours, seconds=-seconds))

    monitor = _monitor(conn, "dur", "job_duration", "dbt-run-analytics",
                       {"max_seconds": 900}, target_kind="job")
    result = checks.evaluate(conn, monitor, now=NOW)
    assert result["status"] == "breach"
    assert "3,600s" in result["message"]
    assert "typically 310s" in result["message"]


def test_failure_rate_is_measured_over_a_window(conn):
    """One failure means different things for a five-minute job and a nightly one.
    Only a rate over a stated window means the same for both."""
    job = _job(conn, "dbt-run-analytics")
    for hours in range(1, 11):
        _run(conn, job, started=NOW - timedelta(hours=hours),
             ended=NOW - timedelta(hours=hours), state="FAILED" if hours <= 3 else "COMPLETED")

    monitor = _monitor(conn, "fail", "job_failure_rate", "dbt-run-analytics",
                       {"max_rate": 0.2, "window_hours": 24}, target_kind="job")
    result = checks.evaluate(conn, monitor, now=NOW)
    assert result["status"] == "breach"
    assert "3/10 runs failed in 24h (30%)" in result["message"]


def test_failure_rate_with_no_runs_in_the_window_is_not_a_breach(conn):
    job = _job(conn, "dbt-run-analytics")
    _run(conn, job, started=NOW - timedelta(days=30), ended=NOW - timedelta(days=30),
         state="FAILED")
    monitor = _monitor(conn, "fail", "job_failure_rate", "dbt-run-analytics",
                       {"max_rate": 0.2, "window_hours": 24}, target_kind="job")
    result = checks.evaluate(conn, monitor, now=NOW)
    assert result["status"] == "insufficient_data"


def test_queue_delay_comes_from_stored_airflow_facets(conn):
    """The Phase 01 decision that avoided an OTel collector (D2), now as an SLO."""
    job = _job(conn, "nightly.load", integration="AIRFLOW", job_type="TASK")
    started = NOW - timedelta(hours=1)
    _run(
        conn, job, started=started, ended=started + timedelta(minutes=5),
        facets={
            "airflowDagRun": {
                "dagRun": {
                    "run_after": (started - timedelta(minutes=40)).isoformat(),
                    "start_date": started.isoformat(),
                }
            }
        },
    )
    monitor = _monitor(conn, "queue", "queue_delay", "nightly.load",
                       {"max_seconds": 600}, target_kind="job")
    result = checks.evaluate(conn, monitor, now=NOW)
    assert result["status"] == "breach"
    assert "2,400s" in result["message"]


# ------------------------------------------------------ idempotency & transitions


def test_repeated_evaluation_does_not_multiply_history(conn):
    """`observed_at` is when the run happened, not when we looked.

    Without that, a monitor polled hourly against a nightly job would hold 24
    copies of one number, and every future baseline would be measuring our polling
    interval instead of the pipeline.
    """
    job = _job(conn, "dbt-run-analytics")
    _run(conn, job, started=NOW - timedelta(hours=1), ended=NOW - timedelta(minutes=55))
    monitor = _monitor(conn, "dur", "job_duration", "dbt-run-analytics",
                       {"max_seconds": 900}, target_kind="job")

    for _ in range(12):
        checks.evaluate(conn, monitor, now=NOW)

    assert len(monitors.recent_points(conn, monitor["id"])) == 1


def test_narrow_collection_window_does_not_erase_a_computed_gap(conn):
    """A re-collect may correct an observation; it must never destroy one.

    Freshness stores the gap since the previous write, which a window starting
    after that write cannot compute. Without the coalesce in store_points, the
    routine hourly check would null out a correct gap every time it ran.
    """
    job = _job(conn, "fct_orders")
    dataset = _dataset(conn, "file", "/warehouse/fct_orders")
    for days in (5, 4):
        run = _run(conn, job, started=NOW - timedelta(days=days), ended=NOW - timedelta(days=days))
        _write(conn, run, dataset, rows=100)

    monitor = _monitor(conn, "fresh", "freshness", "fct_orders", {"max_age_minutes": 10_000})
    checks.evaluate(conn, monitor, since=None, now=NOW)
    assert monitors.recent_points(conn, monitor["id"])[0]["value"] == pytest.approx(1440.0)

    # The hourly check, which only looks back a day.
    checks.evaluate(conn, monitor, since=NOW - timedelta(days=4, hours=1), now=NOW)
    assert monitors.recent_points(conn, monitor["id"])[0]["value"] == pytest.approx(1440.0)


def test_only_status_changes_are_marked_as_transitions(conn):
    """Alerting fires on transitions, so this is what makes a table broken since
    02:00 one alert rather than one per hour."""
    job = _job(conn, "fct_orders")
    dataset = _dataset(conn, "file", "/warehouse/fct_orders")
    run = _run(conn, job, started=NOW - timedelta(hours=6), ended=NOW - timedelta(hours=6))
    _write(conn, run, dataset, rows=100)
    monitor = _monitor(conn, "fresh", "freshness", "fct_orders", {"max_age_minutes": 90})

    first = checks.evaluate(conn, monitor, now=NOW)
    assert first["transitioned"] is True

    monitor = monitors.get_monitor(conn, "fresh")
    second = checks.evaluate(conn, monitor, now=NOW + timedelta(hours=1))
    assert second["status"] == "breach"
    assert second["transitioned"] is False


def test_a_broken_monitor_does_not_stop_the_sweep(conn):
    """One monitor's failure is its own status, not an outage of the check run."""
    job = _job(conn, "dbt-run-analytics")
    _run(conn, job, started=NOW - timedelta(hours=1), ended=NOW - timedelta(minutes=55))
    _monitor(conn, "good", "job_duration", "dbt-run-analytics", {"max_seconds": 900},
             target_kind="job")
    conn.execute(
        "insert into monitors (name, kind, target_kind, target, config) "
        "values ('broken', 'job_duration', 'job', 'dbt-run-analytics', '{}'::jsonb)"
    )

    results = {r["monitor"]: r["status"] for r in checks.check_all(conn, now=NOW)}
    assert results["good"] == "ok"
    assert results["broken"] == "error"


def test_open_breaches_lists_what_is_broken_now(conn):
    job = _job(conn, "fct_orders")
    dataset = _dataset(conn, "file", "/warehouse/fct_orders")
    run = _run(conn, job, started=NOW - timedelta(hours=6), ended=NOW - timedelta(hours=6))
    _write(conn, run, dataset, rows=100)
    monitor = _monitor(conn, "fresh", "freshness", "fct_orders", {"max_age_minutes": 90})
    checks.evaluate(conn, monitor, now=NOW)

    breaches = monitors.open_breaches(conn)
    assert [b["name"] for b in breaches] == ["fresh"]
    assert "limit is 1h30m" in breaches[0]["message"]
