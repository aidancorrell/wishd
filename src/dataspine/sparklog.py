"""Spark event-log parsing.

Spark already writes every metric we want -- per-task CPU, GC, shuffle, spill,
input/output, peak memory -- as JSON lines to durable storage. Reading that is
strictly better than shipping a custom JVM listener into someone's driver
(ADR-004):

  * no code of ours runs inside a production Spark driver, so we cannot be the
    reason a job dies
  * it works retroactively, on runs that happened before dataspine existed
  * it survives an unclean shutdown, which is exactly the run worth inspecting
  * a killed cluster leaves a truncated log, and a truncated log still parses

The parser is deliberately forgiving. Event logs from real clusters are
truncated, contain events from Spark versions we have never seen, and carry
vendor-specific extras. Anything unrecognised is skipped; anything malformed is
counted and skipped. Returning partial data beats raising on the one run someone
actually needs.
"""

from __future__ import annotations

import gzip
import json
import logging
import statistics
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import s3

log = logging.getLogger("dataspine.sparklog")


@dataclass
class StageSummary:
    stage_id: int
    name: str
    attempt: int = 0
    task_count: int = 0
    failed_task_count: int = 0
    submitted_ms: int | None = None
    completed_ms: int | None = None
    task_durations: list[int] = field(default_factory=list)
    input_bytes: int = 0
    output_bytes: int = 0
    shuffle_read_bytes: int = 0
    shuffle_write_bytes: int = 0
    memory_spilled_bytes: int = 0
    disk_spilled_bytes: int = 0
    gc_time_ms: int = 0
    cpu_time_ms: int = 0
    peak_memory_bytes: int = 0

    @property
    def duration_ms(self) -> int | None:
        if self.submitted_ms is None or self.completed_ms is None:
            return None
        return self.completed_ms - self.submitted_ms

    @property
    def skew_ratio(self) -> float | None:
        """Slowest task over the median task.

        The single most useful number for "why is this stage slow": a ratio near
        1 means the work was spread evenly and the stage is simply big, while a
        ratio of 10 means one partition did everything and no amount of extra
        executors will help.
        """
        if len(self.task_durations) < 2:
            return None
        median = statistics.median(self.task_durations)
        if median <= 0:
            return None
        return max(self.task_durations) / median

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage_id": self.stage_id,
            "name": self.name,
            "attempt": self.attempt,
            "task_count": self.task_count,
            "failed_task_count": self.failed_task_count,
            "duration_ms": self.duration_ms,
            "skew_ratio": self.skew_ratio,
            "input_bytes": self.input_bytes,
            "output_bytes": self.output_bytes,
            "shuffle_read_bytes": self.shuffle_read_bytes,
            "shuffle_write_bytes": self.shuffle_write_bytes,
            "memory_spilled_bytes": self.memory_spilled_bytes,
            "disk_spilled_bytes": self.disk_spilled_bytes,
            "gc_time_ms": self.gc_time_ms,
            "cpu_time_ms": self.cpu_time_ms,
            "peak_memory_bytes": self.peak_memory_bytes,
        }


@dataclass
class SparkAppSummary:
    app_id: str | None = None
    app_name: str | None = None
    user: str | None = None
    start_time_ms: int | None = None
    end_time_ms: int | None = None
    executor_count: int = 0
    total_cores: int = 0
    task_count: int = 0
    failed_task_count: int = 0
    input_bytes: int = 0
    input_records: int = 0
    output_bytes: int = 0
    output_records: int = 0
    shuffle_read_bytes: int = 0
    shuffle_write_bytes: int = 0
    memory_spilled_bytes: int = 0
    disk_spilled_bytes: int = 0
    gc_time_ms: int = 0
    cpu_time_ms: int = 0
    run_time_ms: int = 0
    peak_memory_bytes: int = 0
    stages: list[StageSummary] = field(default_factory=list)
    truncated: bool = False
    skipped_lines: int = 0

    # Executor lifetimes: {executor id: [added_ms, removed_ms or None]}. The raw
    # intervals rather than a running total, because an executor still alive at
    # the end has to be charged to whenever the application actually stopped --
    # which is not known until the log has been read to its end (or run out).
    executor_spans: dict[str, list[Any]] = field(default_factory=dict)
    executor_cores: dict[str, int] = field(default_factory=dict)

    # The newest timestamp anywhere in the log. Only load-bearing for a
    # truncated log, where it is the best available answer to "when did this
    # actually stop" -- and a killed cluster is precisely the run whose cost
    # someone wants explained.
    last_event_ms: int | None = None

    @property
    def duration_ms(self) -> int | None:
        if self.start_time_ms is None or self.end_time_ms is None:
            return None
        return self.end_time_ms - self.start_time_ms

    @property
    def _closing_ms(self) -> int | None:
        """When an executor still running at the end stopped accruing.

        `end_time_ms` when the application ended cleanly. A cluster killed by
        YARN never writes ApplicationEnd, and those runs are exactly the
        expensive ones worth pricing -- so fall back to the last timestamp the
        log did manage to record.
        """
        return self.end_time_ms or self.last_event_ms or self.start_time_ms

    @property
    def executor_seconds(self) -> float:
        """Executor-seconds held by this application.

        Summed per executor over its own interval, never `executor_count x
        duration`. Dynamic allocation means executors arrive late and leave
        early, and a flat product charges the late arrivals for time they did
        not exist -- on an autoscaling cluster, which is every EMR cluster
        anyone actually runs, that is wrong for most applications.
        """
        closing = self._closing_ms
        if closing is None:
            return 0.0
        total = 0
        for added, removed in self.executor_spans.values():
            end = removed if removed is not None else closing
            total += max(int(end) - int(added), 0)
        return total / 1000

    @property
    def core_seconds(self) -> float:
        """Core-seconds: the denominator every cost attribution divides by."""
        closing = self._closing_ms
        if closing is None:
            return 0.0
        total = 0
        for executor_id, (added, removed) in self.executor_spans.items():
            end = removed if removed is not None else closing
            cores = self.executor_cores.get(executor_id, 0)
            total += max(int(end) - int(added), 0) * cores
        return total / 1000

    def to_dict(self) -> dict[str, Any]:
        return {
            "app_id": self.app_id,
            "app_name": self.app_name,
            "user": self.user,
            "start_time_ms": self.start_time_ms,
            "end_time_ms": self.end_time_ms,
            "duration_ms": self.duration_ms,
            "executor_count": self.executor_count,
            "total_cores": self.total_cores,
            "executor_seconds": round(self.executor_seconds, 3),
            "core_seconds": round(self.core_seconds, 3),
            "task_count": self.task_count,
            "failed_task_count": self.failed_task_count,
            "input_bytes": self.input_bytes,
            "input_records": self.input_records,
            "output_bytes": self.output_bytes,
            "output_records": self.output_records,
            "shuffle_read_bytes": self.shuffle_read_bytes,
            "shuffle_write_bytes": self.shuffle_write_bytes,
            "memory_spilled_bytes": self.memory_spilled_bytes,
            "disk_spilled_bytes": self.disk_spilled_bytes,
            "gc_time_ms": self.gc_time_ms,
            "cpu_time_ms": self.cpu_time_ms,
            "run_time_ms": self.run_time_ms,
            "peak_memory_bytes": self.peak_memory_bytes,
            "truncated": self.truncated,
            "skipped_lines": self.skipped_lines,
            "stages": [s.to_dict() for s in self.stages],
        }


def _num(source: Any, *path: str) -> int:
    node = source
    for key in path:
        if not isinstance(node, dict):
            return 0
        node = node.get(key)
    return node if isinstance(node, int | float) else 0


def _open(path: str | Path, *, s3_client: Any = None):
    # EMR commonly gzips rolled event logs, and commonly leaves them on S3
    # rather than on a disk we can see. Both are ordinary inputs here.
    if s3.is_s3_uri(path):
        return s3.open_text(str(path), s3=s3_client)
    if str(path).endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return open(path, encoding="utf-8", errors="replace")


def parse_event_log(path: str | Path, *, s3_client: Any = None) -> SparkAppSummary:
    """Parse a Spark event log into a summary. Never raises on bad content.

    `path` is a local path or an `s3://` URI — EMR writes event logs straight to
    S3, so reading them there is the normal case, not the exotic one.
    """
    if not s3.is_s3_uri(path):
        path = Path(path)
    summary = SparkAppSummary()
    stages: dict[tuple[int, int], StageSummary] = {}
    # Tasks reference their stage, so we key stage state by (id, attempt) --
    # a retried stage is a different attempt and must not merge with the first.
    task_stage: dict[int, tuple[int, int]] = {}

    with _open(path, s3_client=s3_client) as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                # A partial final line means the writer was killed mid-flush,
                # which is normal for a cluster that died. Note it and move on.
                summary.skipped_lines += 1
                summary.truncated = True
                continue
            if not isinstance(event, dict):
                summary.skipped_lines += 1
                continue
            _apply(summary, stages, task_stage, event)

    summary.stages = sorted(stages.values(), key=lambda s: (s.stage_id, s.attempt))

    # A log that started an application and never ended one is truncated too,
    # even when every line parsed. A cluster killed by YARN writes perfectly
    # valid JSON right up to the moment it stops -- and those are exactly the
    # runs whose cost someone wants explained, so they must not be silently
    # presented as complete.
    if summary.start_time_ms is not None and summary.end_time_ms is None:
        summary.truncated = True

    return summary


_TIME_KEYS = ("Timestamp", "Completion Time", "Submission Time", "Finish Time")


def _observe_time(summary: SparkAppSummary, event: dict[str, Any]) -> None:
    """Track the newest timestamp seen, wherever it appears.

    Different events carry the time under different keys, and a truncated log
    stops at whichever one happened to be last -- so this looks in the nested
    info blocks too rather than only at the top level.
    """
    candidates = [event.get(key) for key in _TIME_KEYS]
    for block in ("Task Info", "Stage Info"):
        info = event.get(block)
        if isinstance(info, dict):
            candidates += [info.get(key) for key in _TIME_KEYS]
    for value in candidates:
        if isinstance(value, int) and value > (summary.last_event_ms or 0):
            summary.last_event_ms = value


def _apply(
    summary: SparkAppSummary,
    stages: dict[tuple[int, int], StageSummary],
    task_stage: dict[int, tuple[int, int]],
    event: dict[str, Any],
) -> None:
    kind = event.get("Event")

    _observe_time(summary, event)

    if kind == "SparkListenerApplicationStart":
        summary.app_id = event.get("App ID")
        summary.app_name = event.get("App Name")
        summary.user = event.get("User")
        summary.start_time_ms = event.get("Timestamp")

    elif kind == "SparkListenerApplicationEnd":
        summary.end_time_ms = event.get("Timestamp")

    elif kind == "SparkListenerExecutorAdded":
        summary.executor_count += 1
        cores = _num(event, "Executor Info", "Total Cores")
        summary.total_cores += cores
        executor_id = str(event.get("Executor ID"))
        summary.executor_spans[executor_id] = [event.get("Timestamp") or 0, None]
        summary.executor_cores[executor_id] = cores

    elif kind == "SparkListenerExecutorRemoved":
        span = summary.executor_spans.get(str(event.get("Executor ID")))
        if span is not None:
            span[1] = event.get("Timestamp")

    elif kind == "SparkListenerStageSubmitted":
        info = event.get("Stage Info") or {}
        key = (_num(info, "Stage ID"), _num(info, "Stage Attempt ID"))
        stage = stages.setdefault(
            key,
            StageSummary(stage_id=key[0], name=info.get("Stage Name") or "", attempt=key[1]),
        )
        stage.submitted_ms = info.get("Submission Time") or stage.submitted_ms

    elif kind == "SparkListenerStageCompleted":
        info = event.get("Stage Info") or {}
        key = (_num(info, "Stage ID"), _num(info, "Stage Attempt ID"))
        stage = stages.setdefault(
            key,
            StageSummary(stage_id=key[0], name=info.get("Stage Name") or "", attempt=key[1]),
        )
        stage.name = stage.name or (info.get("Stage Name") or "")
        stage.completed_ms = info.get("Completion Time") or stage.completed_ms
        if stage.submitted_ms is None:
            stage.submitted_ms = info.get("Submission Time")

    elif kind == "SparkListenerTaskStart":
        info = event.get("Task Info") or {}
        task_stage[_num(info, "Task ID")] = (
            _num(event, "Stage ID"),
            _num(event, "Stage Attempt ID"),
        )

    elif kind == "SparkListenerTaskEnd":
        _apply_task_end(summary, stages, event)


def _apply_task_end(
    summary: SparkAppSummary,
    stages: dict[tuple[int, int], StageSummary],
    event: dict[str, Any],
) -> None:
    key = (_num(event, "Stage ID"), _num(event, "Stage Attempt ID"))
    stage = stages.setdefault(key, StageSummary(stage_id=key[0], name="", attempt=key[1]))

    metrics = event.get("Task Metrics") or {}
    info = event.get("Task Info") or {}
    reason = (event.get("Task End Reason") or {}).get("Reason")
    failed = reason is not None and reason != "Success"

    summary.task_count += 1
    stage.task_count += 1
    if failed:
        summary.failed_task_count += 1
        stage.failed_task_count += 1

    duration = _num(metrics, "Executor Run Time")
    if duration:
        stage.task_durations.append(int(duration))

    pairs = [
        ("gc_time_ms", _num(metrics, "JVM GC Time")),
        ("cpu_time_ms", _num(metrics, "Executor CPU Time") // 1_000_000),  # ns -> ms
        ("run_time_ms", duration),
        ("input_bytes", _num(metrics, "Input Metrics", "Bytes Read")),
        ("input_records", _num(metrics, "Input Metrics", "Records Read")),
        ("output_bytes", _num(metrics, "Output Metrics", "Bytes Written")),
        ("output_records", _num(metrics, "Output Metrics", "Records Written")),
        ("memory_spilled_bytes", _num(metrics, "Memory Bytes Spilled")),
        ("disk_spilled_bytes", _num(metrics, "Disk Bytes Spilled")),
    ]
    shuffle_read = _num(metrics, "Shuffle Read Metrics", "Local Bytes Read") + _num(
        metrics, "Shuffle Read Metrics", "Remote Bytes Read"
    )
    shuffle_write = _num(metrics, "Shuffle Write Metrics", "Shuffle Bytes Written")
    pairs.append(("shuffle_read_bytes", shuffle_read))
    pairs.append(("shuffle_write_bytes", shuffle_write))

    for attr, value in pairs:
        setattr(summary, attr, getattr(summary, attr) + int(value))

    for attr, value in pairs:
        if hasattr(stage, attr):
            setattr(stage, attr, getattr(stage, attr) + int(value))

    peak = int(_num(metrics, "Peak Execution Memory"))
    summary.peak_memory_bytes = max(summary.peak_memory_bytes, peak)
    stage.peak_memory_bytes = max(stage.peak_memory_bytes, peak)

    # `info` is unused beyond failure detection today, but launch/finish times
    # here are what a future queue-delay-per-task metric would use.
    _ = info
