"""Spark event-log parsing.

Phase 02's goal is fleet-wide Spark job history with lineage attached. The
obvious route is a custom JVM SparkListener; this takes the event log instead
(ADR-004), because Spark already writes exactly these metrics to durable storage
without us putting any code inside someone's driver.

Assertions here are mostly **derived independently from the raw log** rather than
hard-coded from the parser's own output. Pinning numbers the parser produced
would only prove it is consistent with itself; recomputing them from the JSON
proves it is consistent with Spark.

Fixture: a real event log from Spark 3.5.7 running the dbt-shaped job in
dev/spark/job.py, captured 2026-08-08.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dataspine import sparklog

FIXTURE = Path(__file__).parent / "fixtures" / "spark_eventlog_3.5.7.jsonl"


@pytest.fixture()
def raw_events() -> list[dict]:
    return [json.loads(line) for line in FIXTURE.read_text().splitlines() if line.strip()]


@pytest.fixture()
def summary():
    return sparklog.parse_event_log(FIXTURE)


# ------------------------------------------------------------------ identity


def test_application_identity(summary, raw_events):
    """app_id is the join key to OpenLineage's spark_applicationDetails, which
    is what lets metrics land on the same run as the lineage."""
    start = next(e for e in raw_events if e["Event"] == "SparkListenerApplicationStart")
    assert summary.app_id == start["App ID"]
    assert summary.app_name == start["App Name"]
    assert summary.start_time_ms == start["Timestamp"]


def test_application_duration(summary, raw_events):
    end = next(e for e in raw_events if e["Event"] == "SparkListenerApplicationEnd")
    assert summary.end_time_ms == end["Timestamp"]
    assert summary.duration_ms == end["Timestamp"] - summary.start_time_ms
    assert summary.duration_ms > 0


# -------------------------------------------------------------------- totals


def test_task_count_matches_the_log(summary, raw_events):
    expected = sum(1 for e in raw_events if e["Event"] == "SparkListenerTaskEnd")
    assert summary.task_count == expected
    assert summary.task_count > 0


def test_totals_are_the_sum_of_task_metrics(summary, raw_events):
    """Independently recomputed from the raw log, so this proves agreement with
    Spark rather than agreement with ourselves."""
    tasks = [e for e in raw_events if e["Event"] == "SparkListenerTaskEnd"]

    def total(*path):
        out = 0
        for t in tasks:
            node = t.get("Task Metrics") or {}
            for key in path:
                node = (node or {}).get(key) if isinstance(node, dict) else None
            out += node or 0
        return out

    assert summary.gc_time_ms == total("JVM GC Time")
    assert summary.input_bytes == total("Input Metrics", "Bytes Read")
    assert summary.input_records == total("Input Metrics", "Records Read")
    assert summary.output_records == total("Output Metrics", "Records Written")
    assert summary.shuffle_write_bytes == total("Shuffle Write Metrics", "Shuffle Bytes Written")
    assert summary.memory_spilled_bytes == total("Memory Bytes Spilled")
    assert summary.disk_spilled_bytes == total("Disk Bytes Spilled")


def test_executor_inventory(summary, raw_events):
    added = [e for e in raw_events if e["Event"] == "SparkListenerExecutorAdded"]
    assert summary.executor_count == len(added)
    assert summary.total_cores == sum(e["Executor Info"]["Total Cores"] for e in added)


# -------------------------------------------------------------------- stages


def test_stages_are_captured(summary, raw_events):
    completed = [e for e in raw_events if e["Event"] == "SparkListenerStageCompleted"]
    assert len(summary.stages) == len(completed)
    for stage in summary.stages:
        assert stage.stage_id is not None
        assert stage.name
        assert stage.task_count >= 0


def test_stage_task_totals_reconcile_with_the_app(summary):
    """Every task belongs to exactly one stage, so the parts must sum to the
    whole. Catches tasks being dropped or double-counted."""
    assert sum(s.task_count for s in summary.stages) == summary.task_count


def test_failed_tasks_are_counted_separately(summary, raw_events):
    failed = sum(
        1
        for e in raw_events
        if e["Event"] == "SparkListenerTaskEnd"
        and (e.get("Task End Reason") or {}).get("Reason") != "Success"
    )
    assert summary.failed_task_count == failed


# ---------------------------------------------------------------------- skew


def test_skew_ratio_is_reported_per_stage(summary):
    """max/median task duration. The number that says 'one task did all the
    work', which is the most common reason a Spark stage is inexplicably slow.
    """
    for stage in summary.stages:
        if stage.task_count >= 2:
            assert stage.skew_ratio is not None
            assert stage.skew_ratio >= 1.0
        else:
            # A single task cannot be skewed relative to anything.
            assert stage.skew_ratio is None


def test_skew_is_computed_correctly_on_a_synthetic_stage():
    """The real fixture is a tiny job with even tasks, so it cannot exercise the
    interesting case. Construct one that can."""
    stage = sparklog.StageSummary(stage_id=1, name="synthetic")
    stage.task_durations = [10, 10, 10, 100]
    assert stage.skew_ratio == pytest.approx(10.0)


# ------------------------------------------------------------------ robustness


def test_truncated_log_still_parses(tmp_path):
    """An event log from a killed cluster is routinely truncated mid-line. That
    is exactly the run someone needs to look at, so a partial parse must return
    what it has rather than raising."""
    lines = FIXTURE.read_text().splitlines()
    partial = tmp_path / "truncated"
    partial.write_text("\n".join(lines[: len(lines) // 2]) + '\n{"Event": "SparkListen')

    summary = sparklog.parse_event_log(partial)
    assert summary.app_id
    assert summary.task_count >= 0
    assert summary.truncated is True


def test_unparseable_lines_are_skipped_not_fatal(tmp_path):
    log = tmp_path / "noisy"
    log.write_text('{"Event": "SparkListenerApplicationStart", "App ID": "app-1", '
                   '"App Name": "x", "Timestamp": 1}\n'
                   "not json at all\n"
                   '{"Event": "SparkListenerApplicationEnd", "Timestamp": 5}\n')
    summary = sparklog.parse_event_log(log)
    assert summary.app_id == "app-1"
    assert summary.duration_ms == 4


def test_empty_log_does_not_raise(tmp_path):
    empty = tmp_path / "empty"
    empty.write_text("")
    summary = sparklog.parse_event_log(empty)
    assert summary.app_id is None
    assert summary.task_count == 0


def test_summary_is_json_serialisable(summary):
    """It gets stored as jsonb, so it has to survive a round trip."""
    payload = summary.to_dict()
    assert json.loads(json.dumps(payload))["app_id"] == summary.app_id
    assert "stages" in payload
