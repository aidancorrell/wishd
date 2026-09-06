"""Column profiling, column monitors, and native DQ results.

This is the first collection in the project that costs the user money. Everything
before it reads metadata an engine maintains anyway; profiling reads the data. So
the budget is not a nicety bolted on afterwards — it is the feature. A profiler
without one is how a monitoring tool ends up as a line item on a warehouse bill
and then gets removed.

The native-DQ half is the opposite of a feature: it is a decision *not* to
compete. Snowflake DMFs and Databricks DQ rules already run inside the warehouse,
closer to the data and paid for. Reimplementing them would be asking a team to
run two systems that disagree. Reading their results is strictly better.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from dataspine import checks, dq, monitors, profile

NOW = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)


@pytest.fixture()
def widgets(conn):
    conn.execute(
        "create table widgets (id int, email text, score numeric, region text)"
    )
    conn.execute(
        """
        insert into widgets values
          (1, 'a@x.com',  10.0, 'eu'),
          (2, 'b@x.com',  20.0, 'us'),
          (3, null,        0.0, 'eu'),
          (4, 'd@x.com', -5.0,  'us'),
          (5, 'd@x.com',  30.0, null)
        """
    )
    return "widgets"


# ------------------------------------------------------------------- profiling


def test_profile_computes_the_documented_statistics(conn, widgets):
    profiles = {p.column: p for p in profile.profile_table(conn, widgets)}

    email = profiles["email"]
    assert email.null_rate == pytest.approx(0.2)
    assert email.distinct_count == 3
    assert email.uniqueness == pytest.approx(3 / 4)  # distinct over non-null

    score = profiles["score"]
    assert score.min == pytest.approx(-5.0)
    assert score.max == pytest.approx(30.0)
    assert score.mean == pytest.approx(11.0)
    assert score.sum == pytest.approx(55.0)
    assert score.zero_rate == pytest.approx(0.2)
    assert score.negative_rate == pytest.approx(0.2)


def test_numeric_statistics_are_absent_for_text_columns(conn, widgets):
    """A mean over an email address is not a number anyone wants defended."""
    profiles = {p.column: p for p in profile.profile_table(conn, widgets)}
    assert profiles["email"].mean is None
    assert profiles["email"].negative_rate is None


def test_profiling_is_opt_in(conn, widgets):
    """Nothing profiles unless asked. The default install never reads a data
    value, which is the promise the collection layer is built on."""
    assert profile.enabled_for(conn, widgets) is False


def test_the_scan_budget_samples_rather_than_reading_everything(conn):
    """The budget is the feature, not a nicety.

    Above the budget the profiler switches to a sample and *says* it did, because
    a statistic from a 1% sample and one from a full table must not be presented
    as the same claim.
    """
    conn.execute("create table big (id int, score numeric)")
    conn.execute("insert into big select g, g from generate_series(1, 5000) g")

    full = profile.profile_table(conn, "big", max_rows=10_000)[0]
    sampled = profile.profile_table(conn, "big", max_rows=100)[0]

    assert full.sampled is False
    assert sampled.sampled is True
    assert sampled.scanned_rows <= 5000


def test_a_budget_of_zero_refuses_to_scan(conn, widgets):
    """An explicit "never scan" has to be expressible, and has to be obeyed."""
    assert profile.profile_table(conn, widgets, max_rows=0) == []


def test_profiles_are_stored_as_observations(conn, widgets):
    profiles = profile.profile_table(conn, widgets)
    dataset_id = conn.execute(
        "insert into datasets (namespace, name) values ('pg://t', 'widgets') returning id"
    ).fetchone()["id"]
    written = profile.store_profiles(conn, dataset_id, profiles, observed_at=NOW)

    assert written == 4
    rows = conn.execute("select * from column_profiles order by column_name").fetchall()
    assert [r["column_name"] for r in rows] == ["email", "id", "region", "score"]


def test_reprofiling_the_same_moment_is_idempotent(conn, widgets):
    dataset_id = conn.execute(
        "insert into datasets (namespace, name) values ('pg://t', 'widgets') returning id"
    ).fetchone()["id"]
    for _ in range(5):
        profile.store_profiles(
            conn, dataset_id, profile.profile_table(conn, widgets), observed_at=NOW
        )
    count = conn.execute("select count(*) as n from column_profiles").fetchone()["n"]
    assert count == 4


# -------------------------------------------------------------- column monitors


def _column_monitor(conn, name, column, metric, config):
    monitors.apply_specs(
        conn,
        [monitors.MonitorSpec(
            name, "column_stats", "dataset", "widgets",
            {**config, "column": column, "metric": metric}, source="t.yml",
        )],
        sources=["t.yml"],
    )
    return monitors.get_monitor(conn, name)


def _profiled(conn, *, null_rate, at):
    dataset_id = conn.execute(
        "insert into datasets (namespace, name) values ('pg://t','widgets') "
        "on conflict (namespace, name) do update set updated_at = now() returning id"
    ).fetchone()["id"]
    profile.store_profiles(
        conn,
        dataset_id,
        [profile.ColumnProfile(column="email", null_rate=null_rate, row_count=100)],
        observed_at=at,
    )


def test_a_null_rate_monitor_reads_stored_profiles(conn):
    _profiled(conn, null_rate=0.4, at=NOW - timedelta(hours=1))
    monitor = _column_monitor(conn, "email_nulls", "email", "null_rate", {"max": 0.1})
    result = checks.evaluate(conn, monitor, now=NOW)

    assert result["status"] == "breach"
    assert result["value"] == pytest.approx(0.4)


def test_a_column_monitor_with_no_profile_is_quiet(conn):
    """Profiling is opt-in, so the common case is a column monitor with nothing to
    read. It must not read as a passing check."""
    monitor = _column_monitor(conn, "email_nulls", "email", "null_rate", {"max": 0.1})
    assert checks.evaluate(conn, monitor, now=NOW)["status"] == "insufficient_data"


def test_column_monitors_need_a_column_and_a_metric():
    with pytest.raises(monitors.SpecError, match="needs a `column:`"):
        monitors.parse_spec(
            {"monitors": [{"name": "c", "kind": "column_stats", "dataset": "d", "max": 1}]}
        )
    with pytest.raises(monitors.SpecError, match="metric"):
        monitors.parse_spec(
            {"monitors": [{"name": "c", "kind": "column_stats", "dataset": "d",
                           "column": "email", "metric": "vibes", "max": 1}]}
        )


@pytest.mark.parametrize(
    "metric",
    ["null_rate", "uniqueness", "cardinality", "min", "max", "mean", "sum", "stddev",
     "zero_rate", "negative_rate"],
)
def test_every_documented_column_metric_is_supported(metric):
    specs = monitors.parse_spec(
        {"monitors": [{"name": "c", "kind": "column_stats", "dataset": "d",
                       "column": "score", "metric": metric, "max": 1}]}
    )
    assert specs[0].config["metric"] == metric


def test_column_monitors_support_learned_baselines(conn):
    """A null rate that creeps from 1% to 4% breaches no sane static threshold and
    is exactly the kind of decay worth catching."""
    for day, rate in enumerate([0.01, 0.011, 0.009, 0.012, 0.01, 0.008, 0.011, 0.4]):
        _profiled(conn, null_rate=rate, at=NOW - timedelta(days=8 - day))

    monitors.apply_specs(
        conn,
        [monitors.MonitorSpec(
            "creep", "column_stats", "dataset", "widgets",
            {"column": "email", "metric": "null_rate"}, source="t.yml", mode="anomaly",
        )],
        sources=["t.yml"],
    )
    result = checks.evaluate(conn, monitors.get_monitor(conn, "creep"), now=NOW)
    assert result["status"] == "breach"


# ------------------------------------------------------------------ native DQ


def test_native_dq_results_are_ingested_as_a_signal(conn):
    """Snowflake DMFs and Databricks DQ rules run inside the warehouse, closer to
    the data and already paid for. We read them rather than compete."""
    written = dq.import_results(
        conn,
        source="snowflake",
        rows=[
            {
                "table": "ANALYTICS.PUBLIC.FCT_ORDERS",
                "check": "NULL_COUNT(order_id)",
                "status": "fail",
                "value": 12,
                "measured_at": NOW - timedelta(hours=1),
            },
            {
                "table": "ANALYTICS.PUBLIC.FCT_ORDERS",
                "check": "UNIQUE_COUNT(order_id)",
                "status": "pass",
                "value": 1000,
                "measured_at": NOW - timedelta(hours=1),
            },
        ],
    )
    assert written == 2

    failing = dq.failing(conn)
    assert len(failing) == 1
    assert failing[0]["check_name"] == "NULL_COUNT(order_id)"


def test_dq_import_is_idempotent(conn):
    row = {
        "table": "T", "check": "C", "status": "fail", "value": 1,
        "measured_at": NOW,
    }
    for _ in range(4):
        dq.import_results(conn, source="databricks", rows=[row])
    count = conn.execute("select count(*) as n from external_checks").fetchone()["n"]
    assert count == 1


def test_dq_results_attach_to_the_dataset_they_name(conn):
    conn.execute("insert into datasets (namespace, name) values ('sf://a','ANALYTICS.FCT')")
    dq.import_results(
        conn, source="snowflake",
        rows=[{"table": "ANALYTICS.FCT", "check": "C", "status": "fail", "value": 1,
               "measured_at": NOW}],
    )
    row = conn.execute("select * from external_checks").fetchone()
    assert row["dataset_id"] is not None


def test_an_unmatched_dq_result_is_still_kept(conn):
    """A check on a table we have never seen is still evidence. Dropping it would
    hide exactly the coverage gap worth knowing about."""
    dq.import_results(
        conn, source="snowflake",
        rows=[{"table": "NOT.KNOWN", "check": "C", "status": "fail", "value": 1,
               "measured_at": NOW}],
    )
    row = conn.execute("select * from external_checks").fetchone()
    assert row["dataset_id"] is None
    assert row["table_name"] == "NOT.KNOWN"


def test_dq_endpoint_accepts_a_post(api_client):
    response = api_client.post(
        "/api/v1/dq/snowflake",
        json={"results": [
            {"table": "T", "check": "C", "status": "fail", "value": 3,
             "measured_at": NOW.isoformat()}
        ]},
    )
    assert response.status_code == 200
    assert response.json()["imported"] == 1
    assert api_client.get("/api/v1/dq/failing").json()["failing"][0]["check_name"] == "C"


def test_fractional_metrics_are_not_rounded_into_nonsense(conn):
    """A null rate of 0.979 against a ceiling of 0.5 must not read as
    "1, above the ceiling of 0".

    Integer formatting is right for row counts and wrong for rates, and the same
    judgement path serves both. Found by running a real column monitor.
    """
    _profiled(conn, null_rate=0.979, at=NOW - timedelta(hours=1))
    monitor = _column_monitor(conn, "nulls", "email", "null_rate", {"max": 0.5})
    result = checks.evaluate(conn, monitor, now=NOW)

    assert "0.979" in result["message"]
    assert "0.5" in result["message"]
    assert " 1 " not in result["message"]
