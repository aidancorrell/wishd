"""Resource-seconds, and the cluster inventory that prices them.

Cost attribution needs a denominator: how much compute did *this* application
actually hold, over what interval. Everything else in Phase 05 divides by it.

The pleasing part is that the denominator is free. The real Spark event log
carries `SparkListenerExecutorAdded` with a timestamp and a core count, and
`SparkListenerExecutorRemoved` when one goes away — so core-seconds are a sum
over intervals of data we already parse. This is ADR-004 paying off a second
time: reading what Spark already writes, rather than instrumenting anything.

AWS is needed only for the *price*. The cluster inventory (instance types,
counts, Spot vs On-Demand) is what turns core-seconds into dollars, and it is
the one part that cannot be derived from an event log.

**Validation status.** Everything computed from event logs is tested against the
real captured Spark 3.5.7 log. The EMR inventory mapper is built from boto3's
documented `describe_cluster` / `list_instance_groups` response shapes and has
**never been run against a real cluster** — the same honest category as the
bootstrap action (D5) and the warehouse pollers (D9).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from dataspine import resources, sparklog

EVENTLOG = Path(__file__).parent / "fixtures" / "spark_eventlog_3.5.7.jsonl"
NOW = datetime(2026, 8, 9, 12, 0, tzinfo=UTC)


# ------------------------------------------------------- resource accounting


def test_core_seconds_come_from_the_real_event_log():
    """The denominator, from a capture rather than a fixture we invented."""
    summary = sparklog.parse_event_log(EVENTLOG)

    assert summary.core_seconds > 0
    assert summary.executor_seconds > 0
    # The capture is local mode: one executor ("driver") with 2 cores, held for
    # the life of the application.
    assert summary.core_seconds == pytest.approx(
        summary.executor_seconds * 2, rel=0.01
    )


def _log(tmp_path, events):
    path = tmp_path / "eventlog.jsonl"
    path.write_text("\n".join(json.dumps(e) for e in events))
    return path


def _base(start_ms, end_ms):
    return [
        {"Event": "SparkListenerApplicationStart", "App Name": "app", "App ID": "app-1",
         "Timestamp": start_ms},
        {"Event": "SparkListenerApplicationEnd", "Timestamp": end_ms},
    ]


def test_an_executor_that_leaves_early_stops_accruing(tmp_path):
    """Dynamic allocation releases executors mid-run. Charging for the whole
    application would overstate every cost on an autoscaling cluster — which is
    every EMR cluster anyone actually runs."""
    start = 1_000_000
    events = _base(start, start + 100_000) + [
        {"Event": "SparkListenerExecutorAdded", "Timestamp": start,
         "Executor ID": "1", "Executor Info": {"Total Cores": 4}},
        {"Event": "SparkListenerExecutorRemoved", "Timestamp": start + 20_000,
         "Executor ID": "1"},
    ]
    summary = sparklog.parse_event_log(_log(tmp_path, events))

    assert summary.executor_seconds == pytest.approx(20.0)
    assert summary.core_seconds == pytest.approx(80.0)


def test_an_executor_still_running_at_the_end_is_charged_to_the_end(tmp_path):
    start = 1_000_000
    events = _base(start, start + 60_000) + [
        {"Event": "SparkListenerExecutorAdded", "Timestamp": start,
         "Executor ID": "1", "Executor Info": {"Total Cores": 2}},
    ]
    summary = sparklog.parse_event_log(_log(tmp_path, events))
    assert summary.executor_seconds == pytest.approx(60.0)


def test_executors_added_at_different_times_accrue_separately(tmp_path):
    """Autoscaling ramps up. A flat executor_count x duration would charge the
    late arrivals for time they did not exist."""
    start = 1_000_000
    events = _base(start, start + 100_000) + [
        {"Event": "SparkListenerExecutorAdded", "Timestamp": start,
         "Executor ID": "1", "Executor Info": {"Total Cores": 2}},
        {"Event": "SparkListenerExecutorAdded", "Timestamp": start + 50_000,
         "Executor ID": "2", "Executor Info": {"Total Cores": 2}},
    ]
    summary = sparklog.parse_event_log(_log(tmp_path, events))

    assert summary.executor_seconds == pytest.approx(150.0)
    assert summary.core_seconds == pytest.approx(300.0)


def test_a_truncated_log_still_yields_resource_seconds(tmp_path):
    """A cluster killed by YARN never writes ApplicationEnd — and those runs are
    exactly the expensive ones worth pricing."""
    start = 1_000_000
    events = [
        {"Event": "SparkListenerApplicationStart", "App Name": "app", "App ID": "app-1",
         "Timestamp": start},
        {"Event": "SparkListenerExecutorAdded", "Timestamp": start,
         "Executor ID": "1", "Executor Info": {"Total Cores": 2}},
        {"Event": "SparkListenerTaskEnd", "Task Info": {"Finish Time": start + 30_000}},
    ]
    summary = sparklog.parse_event_log(_log(tmp_path, events))

    assert summary.truncated is True
    assert summary.core_seconds > 0


def test_resource_seconds_reach_the_stored_metrics(conn, tmp_path):
    from dataspine import spark_metrics

    start = 1_000_000
    events = _base(start, start + 10_000) + [
        {"Event": "SparkListenerExecutorAdded", "Timestamp": start,
         "Executor ID": "1", "Executor Info": {"Total Cores": 2}},
    ]
    summary = sparklog.parse_event_log(_log(tmp_path, events))
    spark_metrics.store(conn, summary, source_uri="test")

    row = conn.execute("select metrics from spark_apps").fetchone()
    assert row["metrics"]["core_seconds"] == pytest.approx(20.0)


# -------------------------------------------------------- cluster inventory


def _cluster(conn, cluster_id="j-2ABCDEFGHIJKL", **kwargs):
    spec = resources.ClusterSpec(
        cluster_id=cluster_id,
        name=kwargs.get("name", "analytics-emr"),
        platform=kwargs.get("platform", "emr"),
        started_at=kwargs.get("started_at", NOW - timedelta(hours=4)),
        ended_at=kwargs.get("ended_at"),
        tags=kwargs.get("tags", {"team": "analytics"}),
        instance_groups=kwargs.get("instance_groups", [
            {"role": "MASTER", "instance_type": "m5.xlarge", "count": 1,
             "market": "ON_DEMAND"},
            {"role": "CORE", "instance_type": "r5.2xlarge", "count": 4,
             "market": "SPOT"},
        ]),
    )
    resources.store_cluster(conn, spec)
    return cluster_id


def test_a_cluster_and_its_instance_groups_are_stored(conn):
    _cluster(conn)
    cluster = resources.get_cluster(conn, "j-2ABCDEFGHIJKL")

    assert cluster["name"] == "analytics-emr"
    assert len(cluster["instance_groups"]) == 2
    spot = next(g for g in cluster["instance_groups"] if g["market"] == "SPOT")
    assert spot["instance_type"] == "r5.2xlarge"
    assert spot["count"] == 4


def test_storing_a_cluster_again_updates_rather_than_duplicates(conn):
    """Lifecycle sync runs on a schedule against a cluster that is still alive:
    its instance groups scale up and down between polls."""
    _cluster(conn)
    _cluster(conn, instance_groups=[
        {"role": "CORE", "instance_type": "r5.2xlarge", "count": 12, "market": "SPOT"},
    ])
    cluster = resources.get_cluster(conn, "j-2ABCDEFGHIJKL")

    assert len(cluster["instance_groups"]) == 1
    assert cluster["instance_groups"][0]["count"] == 12


def test_tags_are_what_the_cost_report_will_join_on(conn):
    """CUR rows carry `resourceTags/...`, not cluster ids, so the tags are the
    join key and storing them is not optional."""
    _cluster(conn, tags={"team": "analytics", "env": "prod"})
    assert resources.get_cluster(conn, "j-2ABCDEFGHIJKL")["tags"]["env"] == "prod"


def test_spark_applications_link_to_the_cluster_that_ran_them(conn, tmp_path):
    from dataspine import spark_metrics

    _cluster(conn)
    start = int((NOW - timedelta(hours=2)).timestamp() * 1000)
    events = _base(start, start + 60_000) + [
        {"Event": "SparkListenerExecutorAdded", "Timestamp": start,
         "Executor ID": "1", "Executor Info": {"Total Cores": 2}},
    ]
    summary = sparklog.parse_event_log(_log(tmp_path, events))
    spark_metrics.store(conn, summary, source_uri="s3://logs/j-2ABCDEFGHIJKL/app-1")

    linked = resources.link_applications(conn)
    assert linked == 1
    row = conn.execute("select cluster_id from spark_apps").fetchone()
    assert row["cluster_id"] == "j-2ABCDEFGHIJKL"


def test_an_application_outside_the_cluster_window_is_not_linked(conn, tmp_path):
    """A log path can name a cluster whose lifetime does not contain the run —
    reused ids, or a backfill of logs from a cluster long gone. Linking anyway
    would attribute cost to the wrong week."""
    from dataspine import spark_metrics

    _cluster(conn, started_at=NOW - timedelta(hours=1), ended_at=NOW)
    start = int((NOW - timedelta(days=30)).timestamp() * 1000)
    summary = sparklog.parse_event_log(_log(tmp_path, _base(start, start + 1000)))
    spark_metrics.store(conn, summary, source_uri="s3://logs/j-2ABCDEFGHIJKL/app-1")

    assert resources.link_applications(conn) == 0


# --------------------------------------------------------------- EMR mapper


def test_emr_describe_responses_map_to_a_cluster_spec():
    """Shapes taken from boto3's documented EMR responses.

    Honest about what this proves: the mapping is right *if* the documented shape
    is right. It is not evidence that anyone has run this against real EMR — see
    D4/D5.
    """
    described = {
        "Cluster": {
            "Id": "j-2ABCD",
            "Name": "analytics-emr",
            "Status": {
                "Timeline": {
                    "CreationDateTime": NOW - timedelta(hours=3),
                    "EndDateTime": NOW - timedelta(hours=1),
                }
            },
            "Tags": [{"Key": "team", "Value": "analytics"}],
        }
    }
    groups = {
        "InstanceGroups": [
            {"InstanceGroupType": "MASTER", "InstanceType": "m5.xlarge",
             "RunningInstanceCount": 1, "Market": "ON_DEMAND"},
            {"InstanceGroupType": "CORE", "InstanceType": "r5.2xlarge",
             "RunningInstanceCount": 8, "Market": "SPOT"},
        ]
    }
    spec = resources.cluster_from_emr(described, groups)

    assert spec.cluster_id == "j-2ABCD"
    assert spec.tags == {"team": "analytics"}
    assert spec.ended_at is not None
    assert {g["market"] for g in spec.instance_groups} == {"ON_DEMAND", "SPOT"}


def test_a_running_cluster_has_no_end_time():
    """`EndDateTime` is absent while a cluster is alive, and inventing one would
    stop it accruing cost the moment we first looked at it."""
    spec = resources.cluster_from_emr(
        {"Cluster": {"Id": "j-1", "Name": "x",
                     "Status": {"Timeline": {"CreationDateTime": NOW}}, "Tags": []}},
        {"InstanceGroups": []},
    )
    assert spec.ended_at is None


def test_instance_fleets_map_as_well_as_instance_groups():
    """EMR clusters use one or the other, and a fleet-based cluster is the common
    shape for Spot-heavy analytics."""
    spec = resources.cluster_from_emr(
        {"Cluster": {"Id": "j-1", "Name": "x",
                     "Status": {"Timeline": {"CreationDateTime": NOW}}, "Tags": []}},
        {"InstanceFleets": [
            {"InstanceFleetType": "CORE",
             "ProvisionedSpotCapacity": 16,
             "ProvisionedOnDemandCapacity": 4,
             "InstanceTypeSpecifications": [{"InstanceType": "r5.2xlarge"}]},
        ]},
    )
    markets = {g["market"]: g for g in spec.instance_groups}
    assert markets["SPOT"]["count"] == 16
    assert markets["ON_DEMAND"]["count"] == 4
