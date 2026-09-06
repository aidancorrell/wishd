"""Performance heuristics over Spark metrics.

Metrics alone do not help anyone. "Shuffle write 4.2GB" is a number; "this stage
spilled to disk because it shuffled 4.2GB through executors with 1GB of memory"
is an answer. These turn the former into the latter.

Design rules, learned from every tool that cries wolf:

  * **Silence is the default.** A healthy job produces zero findings. A tool that
    always shows three warnings trains people to ignore all three.
  * **Every finding carries its evidence.** The numbers that triggered it are in
    the message, so someone can disagree with the threshold.
  * **Thresholds are conservative.** A false positive costs trust; a missed
    finding costs one investigation that would have happened anyway.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dataspine import heuristics, sparklog

EVENTLOG = Path(__file__).parent / "fixtures" / "spark_eventlog_3.5.7.jsonl"


def metrics(**overrides):
    """A healthy baseline; override one dimension per test."""
    base = {
        "task_count": 100,
        "failed_task_count": 0,
        "executor_count": 4,
        "total_cores": 16,
        "duration_ms": 60_000,
        "run_time_ms": 200_000,
        "gc_time_ms": 2_000,
        "cpu_time_ms": 180_000,
        "input_bytes": 10 * 1024**3,
        "input_records": 1_000_000,
        "output_bytes": 5 * 1024**3,
        "output_records": 900_000,
        "shuffle_read_bytes": 0,
        "shuffle_write_bytes": 0,
        "memory_spilled_bytes": 0,
        "disk_spilled_bytes": 0,
        "peak_memory_bytes": 0,
        "truncated": False,
        "stages": [],
    }
    base.update(overrides)
    return base


def stage(**overrides):
    base = {
        "stage_id": 1,
        "name": "stage",
        "task_count": 100,
        "failed_task_count": 0,
        "duration_ms": 30_000,
        "skew_ratio": 1.2,
        "input_bytes": 1024**3,
        "output_bytes": 1024**3,
        "shuffle_read_bytes": 0,
        "shuffle_write_bytes": 0,
        "memory_spilled_bytes": 0,
        "disk_spilled_bytes": 0,
        "gc_time_ms": 100,
        "cpu_time_ms": 1000,
        "peak_memory_bytes": 0,
    }
    base.update(overrides)
    return base


def codes(findings):
    return {f["code"] for f in findings}


# ------------------------------------------------------------------- silence


def test_a_healthy_job_produces_no_findings():
    """The most important test here. A tool that always has something to say
    teaches people to stop reading it."""
    assert heuristics.analyse(metrics(stages=[stage()])) == []


def test_the_real_fixture_is_clean():
    """Our dev job is small and healthy; it must not trip anything. If it does,
    a threshold is wrong."""
    summary = sparklog.parse_event_log(EVENTLOG).to_dict()
    findings = heuristics.analyse(summary)
    assert findings == [], f"healthy real job produced findings: {findings}"


def test_analyse_tolerates_missing_and_junk_input():
    for payload in ({}, None, {"stages": None}, {"stages": [{}]}, {"task_count": "x"}):
        assert isinstance(heuristics.analyse(payload), list)


# --------------------------------------------------------------------- skew


def test_task_skew_is_detected():
    findings = heuristics.analyse(metrics(stages=[stage(skew_ratio=12.0)]))
    assert "task_skew" in codes(findings)
    detail = next(f for f in findings if f["code"] == "task_skew")["detail"]
    assert "12" in detail, "the triggering number must be in the message"


def test_mild_skew_is_ignored():
    assert heuristics.analyse(metrics(stages=[stage(skew_ratio=2.0)])) == []


def test_skew_needs_enough_tasks_to_mean_anything():
    """Three tasks where one is slower is noise, not skew."""
    assert heuristics.analyse(metrics(stages=[stage(task_count=3, skew_ratio=20.0)])) == []


# -------------------------------------------------------------------- spill


def test_disk_spill_is_flagged():
    findings = heuristics.analyse(
        metrics(disk_spilled_bytes=4 * 1024**3, stages=[stage(disk_spilled_bytes=4 * 1024**3)])
    )
    assert "disk_spill" in codes(findings)


def test_spill_severity_scales_with_size():
    small = heuristics.analyse(metrics(disk_spilled_bytes=200 * 1024**2, stages=[stage()]))
    huge = heuristics.analyse(metrics(disk_spilled_bytes=200 * 1024**3, stages=[stage()]))
    assert next(f for f in small if f["code"] == "disk_spill")["severity"] == "warning"
    assert next(f for f in huge if f["code"] == "disk_spill")["severity"] == "critical"


def test_trivial_spill_is_not_worth_mentioning():
    assert heuristics.analyse(metrics(disk_spilled_bytes=1024, stages=[stage()])) == []


# ----------------------------------------------------------------------- gc


def test_excessive_gc_is_flagged():
    findings = heuristics.analyse(metrics(gc_time_ms=80_000, run_time_ms=200_000))
    assert "high_gc" in codes(findings)


def test_normal_gc_is_ignored():
    assert heuristics.analyse(metrics(gc_time_ms=4_000, run_time_ms=200_000)) == []


# ------------------------------------------------------- over-provisioning


def test_over_provisioned_executors_are_flagged():
    """16 cores for 3 tasks means most of the cluster was idle and being paid
    for -- the finding that turns into money in Phase 05."""
    findings = heuristics.analyse(
        metrics(total_cores=64, task_count=3, stages=[stage(task_count=3)])
    )
    assert "over_provisioned" in codes(findings)


def test_well_matched_parallelism_is_ignored():
    assert heuristics.analyse(metrics(total_cores=16, task_count=100, stages=[stage()])) == []


# --------------------------------------------------------------- small files


def test_small_output_files_are_flagged():
    """Many tasks each writing a sliver is the classic small-file explosion:
    cheap now, expensive for every reader afterwards."""
    findings = heuristics.analyse(
        metrics(task_count=2000, output_bytes=20 * 1024**2, stages=[stage(task_count=2000)])
    )
    assert "small_files" in codes(findings)


def test_few_large_files_are_fine():
    assert (
        heuristics.analyse(
            metrics(task_count=8, output_bytes=8 * 1024**3, stages=[stage(task_count=8)])
        )
        == []
    )


# ------------------------------------------------------------------ failures


def test_failed_tasks_are_surfaced():
    findings = heuristics.analyse(metrics(failed_task_count=7))
    assert "failed_tasks" in codes(findings)
    assert next(f for f in findings if f["code"] == "failed_tasks")["severity"] == "warning"


# ------------------------------------------------------------------- shape


def test_findings_have_a_consistent_shape():
    findings = heuristics.analyse(
        metrics(
            disk_spilled_bytes=10 * 1024**3,
            gc_time_ms=90_000,
            stages=[stage(skew_ratio=30.0, disk_spilled_bytes=10 * 1024**3)],
        )
    )
    assert findings
    for f in findings:
        assert set(f) >= {"code", "severity", "title", "detail"}
        assert f["severity"] in ("info", "warning", "critical")
        assert f["detail"], "every finding must carry its evidence"
    assert json.loads(json.dumps(findings))


def test_findings_are_ordered_most_severe_first():
    findings = heuristics.analyse(
        metrics(
            failed_task_count=1,
            disk_spilled_bytes=500 * 1024**3,
            stages=[stage(disk_spilled_bytes=500 * 1024**3)],
        )
    )
    severities = [f["severity"] for f in findings]
    rank = {"critical": 0, "warning": 1, "info": 2}
    assert severities == sorted(severities, key=lambda s: rank[s])


@pytest.mark.parametrize("value,expected", [(0, "0B"), (1536, "1.5KB"), (5 * 1024**3, "5.0GB")])
def test_byte_formatting(value, expected):
    assert heuristics.format_bytes(value) == expected
