"""The remaining monitor kinds: custom SQL, Spark spill, and job retries.

Two of these needed no new collection either — the Spark metrics have been in
`spark_apps` since Phase 02, and Airflow has been sending attempt numbers since
Phase 00. They were stored and unread.

The retry monitor carries a finding worth stating loudly, because it is the sort
of thing that silently produces a monitor that never fires: **the Airflow facet
field called `retries` is the configured maximum, not the number of attempts.**
The actual count is `taskInstance.try_number`. Reading the obvious-looking field
would give every task the same constant, forever.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from dataspine import checks, monitors

NOW = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)


def _job(conn, name) -> int:
    return conn.execute(
        "insert into jobs (namespace, name, integration) values ('t', %s, 'AIRFLOW') "
        "on conflict (namespace, name) do update set name = excluded.name returning id",
        (name,),
    ).fetchone()["id"]


def _run(conn, job_id, *, started, facets=None, state="COMPLETED"):
    run_id = uuid4()
    conn.execute(
        "insert into runs (run_id, job_id, root_run_id, state, started_at, ended_at, facets) "
        "values (%s, %s, %s, %s, %s, %s, %s)",
        (run_id, job_id, run_id, state, started, started, json.dumps(facets or {})),
    )
    return run_id


def _monitor(conn, name, kind, target, config, *, target_kind="job"):
    monitors.apply_specs(
        conn,
        [monitors.MonitorSpec(name, kind, target_kind, target, config, source="t.yml")],
        sources=["t.yml"],
    )
    return monitors.get_monitor(conn, name)


# ------------------------------------------------------------------- retries


def _try_facets(try_number: int, max_retries: int = 3):
    """The shape Airflow 3.0.2 + provider 2.19.0 really sends."""
    return {
        "airflow": {
            "taskInstance": {"try_number": try_number},
            "task": {"retries": max_retries},
        }
    }


def test_retries_are_counted_from_try_number_not_from_the_retries_field(conn):
    """The distinction that makes this monitor work at all.

    `task.retries` is the configured ceiling and is identical on every run. A
    monitor reading it would report a constant 3 forever and never fire.
    """
    job = _job(conn, "nightly.load")
    _run(conn, job, started=NOW - timedelta(hours=2), facets=_try_facets(1))
    _run(conn, job, started=NOW - timedelta(hours=1), facets=_try_facets(3))

    monitor = _monitor(conn, "retries", "job_retries", "nightly.load", {"max": 1})
    result = checks.evaluate(conn, monitor, now=NOW)

    assert result["status"] == "breach"
    assert result["value"] == 2, "try_number 3 is two retries, not three attempts"


def test_a_first_attempt_is_zero_retries(conn):
    job = _job(conn, "nightly.load")
    _run(conn, job, started=NOW - timedelta(hours=1), facets=_try_facets(1))
    monitor = _monitor(conn, "retries", "job_retries", "nightly.load", {"max": 1})
    result = checks.evaluate(conn, monitor, now=NOW)
    assert result["status"] == "ok"
    assert result["value"] == 0


def test_runs_without_the_facet_are_skipped_not_counted_as_zero(conn):
    """Spark and dbt send no attempt number. Treating a missing facet as "zero
    retries" would report a clean record for jobs we cannot see."""
    job = _job(conn, "spark.thing")
    _run(conn, job, started=NOW - timedelta(hours=1), facets={})
    monitor = _monitor(conn, "retries", "job_retries", "spark.thing", {"max": 1})
    assert checks.evaluate(conn, monitor, now=NOW)["status"] == "insufficient_data"


# --------------------------------------------------------------------- spill


def _spark_app(conn, run_id, *, disk=0, memory=0, app_id=None):
    conn.execute(
        "insert into spark_apps (app_id, run_id, metrics) values (%s, %s, %s)",
        (
            app_id or f"app-{uuid4()}",
            run_id,
            json.dumps({"disk_spilled_bytes": disk, "memory_spilled_bytes": memory}),
        ),
    )


def test_spill_reads_metrics_stored_since_phase_02(conn):
    job = _job(conn, "dbt_spark_analytics")
    run = _run(conn, job, started=NOW - timedelta(hours=1))
    _spark_app(conn, run, disk=8 * 1024**3, memory=2 * 1024**3)

    monitor = _monitor(conn, "spill", "spark_spill", "dbt_spark_analytics", {"max_gb": 4})
    result = checks.evaluate(conn, monitor, now=NOW)

    assert result["status"] == "breach"
    assert "10.0GB" in result["message"]


def test_a_job_that_does_not_spill_is_silent(conn):
    job = _job(conn, "dbt_spark_analytics")
    run = _run(conn, job, started=NOW - timedelta(hours=1))
    _spark_app(conn, run, disk=0, memory=0)
    monitor = _monitor(conn, "spill", "spark_spill", "dbt_spark_analytics", {"max_gb": 4})
    assert checks.evaluate(conn, monitor, now=NOW)["status"] == "ok"


def test_spill_finds_metrics_attached_anywhere_in_the_run_tree(conn):
    """The event log is joined to the Spark run, but the job a human names is the
    dbt model or the Airflow task above it."""
    parent_job = _job(conn, "analytics_daily.dbt_run_marts")
    child_job = _job(conn, "dbt_spark_analytics.fct_orders")
    parent = _run(conn, parent_job, started=NOW - timedelta(hours=1))
    child_id = uuid4()
    conn.execute(
        "insert into runs (run_id, job_id, parent_run_id, root_run_id, state, "
        "started_at, ended_at) values (%s, %s, %s, %s, 'COMPLETED', %s, %s)",
        (child_id, child_job, parent, parent, NOW - timedelta(hours=1),
         NOW - timedelta(hours=1)),
    )
    _spark_app(conn, child_id, disk=9 * 1024**3)

    monitor = _monitor(conn, "spill", "spark_spill", "analytics_daily.dbt_run_marts",
                       {"max_gb": 4})
    assert checks.evaluate(conn, monitor, now=NOW)["status"] == "breach"


# ---------------------------------------------------------------- custom SQL


def test_custom_sql_returns_one_number(conn):
    conn.execute("create temporary table widgets (id int, status text)")
    conn.execute("insert into widgets values (1,'ok'),(2,'bad'),(3,'bad')")

    monitor = _monitor(
        conn, "bad_widgets", "custom_sql",
        "select count(*) from widgets where status = 'bad'",
        {"max": 1}, target_kind="query",
    )
    result = checks.evaluate(conn, monitor, now=NOW)
    assert result["status"] == "breach"
    assert result["value"] == 2


def test_custom_sql_within_bounds_is_quiet(conn):
    conn.execute("create temporary table widgets (id int)")
    conn.execute("insert into widgets values (1)")
    monitor = _monitor(conn, "widgets", "custom_sql", "select count(*) from widgets",
                       {"min": 1}, target_kind="query")
    assert checks.evaluate(conn, monitor, now=NOW)["status"] == "ok"


def test_custom_sql_cannot_write(conn):
    """A monitor is a question, not a migration.

    The query runs in a read-only transaction, so a mistake — or a malicious PR
    against the monitors file — cannot mutate the warehouse it is watching.
    """
    conn.execute("create temporary table widgets (id int)")
    monitor = _monitor(conn, "sneaky", "custom_sql", "insert into widgets values (99)",
                       {"max": 1}, target_kind="query")
    result = checks.evaluate(conn, monitor, now=NOW)

    assert result["status"] == "error"
    assert conn.execute("select count(*) as n from widgets").fetchone()["n"] == 0


def test_custom_sql_returning_no_rows_is_insufficient_not_a_breach(conn):
    conn.execute("create temporary table widgets (id int)")
    monitor = _monitor(conn, "empty", "custom_sql",
                       "select id from widgets where id = 999",
                       {"max": 1}, target_kind="query")
    assert checks.evaluate(conn, monitor, now=NOW)["status"] == "insufficient_data"


def test_custom_sql_returning_a_non_number_is_an_error_not_a_breach(conn):
    """"Broken monitor" and "broken data" need different readers."""
    monitor = _monitor(conn, "wrong", "custom_sql", "select 'banana'",
                       {"max": 1}, target_kind="query")
    result = checks.evaluate(conn, monitor, now=NOW)
    assert result["status"] == "error"
    assert "number" in result["message"].lower()


def test_custom_sql_is_bounded_by_a_statement_timeout(conn):
    """A runaway monitor query must not hold a connection open indefinitely and
    starve the ingest pool."""
    monitor = _monitor(conn, "slow", "custom_sql", "select pg_sleep(5), 1",
                       {"max": 1, "timeout_seconds": 1}, target_kind="query")
    result = checks.evaluate(conn, monitor, now=NOW)
    assert result["status"] == "error"


# -------------------------------------------------------------------- specs


def test_custom_sql_needs_a_query_and_a_bound():
    with pytest.raises(monitors.SpecError, match="needs a `query:`"):
        monitors.parse_spec({"monitors": [{"name": "q", "kind": "custom_sql", "max": 1}]})
    with pytest.raises(monitors.SpecError, match="needs `min`, `max`, or both"):
        monitors.parse_spec(
            {"monitors": [{"name": "q", "kind": "custom_sql", "query": "select 1"}]}
        )


def test_spill_and_retries_parse_from_yaml():
    specs = monitors.parse_spec(
        {
            "monitors": [
                {"name": "s", "kind": "spark_spill", "job": "j", "max_gb": 4},
                {"name": "r", "kind": "job_retries", "job": "j", "max": 2},
            ]
        }
    )
    assert [s.kind for s in specs] == ["spark_spill", "job_retries"]
    assert specs[0].target_kind == "job"


def test_cost_per_run_arrived_with_the_cost_model():
    """Phase 03 deliberately refused this kind, because a cost monitor without a
    cost model would be inventing numbers and a wrong cost figure is worse than
    none. Phase 05 built the model, so the refusal is now the wrong behaviour."""
    specs = monitors.parse_spec(
        {"monitors": [{"name": "c", "kind": "cost_per_run", "job": "j", "max": 10}]}
    )
    assert specs[0].kind == "cost_per_run"
    assert specs[0].target_kind == "job"
