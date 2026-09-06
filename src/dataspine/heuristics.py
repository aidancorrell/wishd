"""Turning Spark metrics into findings.

"Shuffle write 4.2GB" is a number. "This stage spilled 4.2GB to disk because one
partition held most of the data" is an answer, and the answer is the product.

Three rules govern everything here:

  **Silence is the default.** A healthy job produces zero findings. Tools that
  always have three warnings teach people to ignore all three, and then the one
  that mattered goes unread too.

  **Every finding carries its evidence.** The triggering numbers go in the
  message so a reader can disagree with our threshold rather than having to
  trust it.

  **Thresholds are conservative.** A false positive costs credibility, which is
  hard to win back. A missed finding costs one investigation that would have
  happened anyway.

Thresholds are deliberately module-level constants: they are opinions, not
truths, and someone will want to argue with them.
"""

from __future__ import annotations

from typing import Any

# --- thresholds -------------------------------------------------------------
SKEW_RATIO = 5.0          # slowest task vs median before it is worth saying
SKEW_MIN_TASKS = 10       # below this, "skew" is just small-sample noise
SPILL_MIN_BYTES = 100 * 1024**2      # ignore trivial spill
SPILL_CRITICAL_BYTES = 50 * 1024**3  # this much means the job is misconfigured
GC_FRACTION = 0.2         # GC as a share of executor run time
GC_MIN_MS = 10_000        # ...and enough absolute time to be worth acting on
OVER_PROVISION_RATIO = 4.0   # cores per task before the cluster is mostly idle
OVER_PROVISION_MIN_CORES = 8
SMALL_FILE_BYTES = 32 * 1024**2  # average output per writing task
SMALL_FILE_MIN_TASKS = 100

SEVERITY_RANK = {"critical": 0, "warning": 1, "info": 2}


def format_bytes(value: Any) -> str:
    try:
        size = float(value or 0)
    except (TypeError, ValueError):
        return "0B"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.0f}{unit}" if unit == "B" else f"{size:.1f}{unit}"
        size /= 1024
    return f"{size:.1f}TB"


def _num(source: Any, key: str) -> float:
    if not isinstance(source, dict):
        return 0.0
    value = source.get(key)
    return float(value) if isinstance(value, int | float) else 0.0


def _finding(code: str, severity: str, title: str, detail: str) -> dict[str, Any]:
    return {"code": code, "severity": severity, "title": title, "detail": detail}


def analyse(metrics: Any) -> list[dict[str, Any]]:
    """Findings for one Spark application. Never raises."""
    if not isinstance(metrics, dict):
        return []

    stages = metrics.get("stages")
    stages = [s for s in stages if isinstance(s, dict)] if isinstance(stages, list) else []

    findings: list[dict[str, Any]] = []
    findings += _skew(stages)
    findings += _spill(metrics)
    findings += _gc(metrics)
    findings += _over_provisioned(metrics)
    findings += _small_files(metrics)
    findings += _failed_tasks(metrics)

    findings.sort(key=lambda f: SEVERITY_RANK.get(f["severity"], 3))
    return findings


def _skew(stages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    worst = None
    for stage in stages:
        ratio = stage.get("skew_ratio")
        if not isinstance(ratio, int | float):
            continue
        # Below SKEW_MIN_TASKS the median is computed from too few samples for
        # the ratio to mean anything -- three tasks where one is slow is noise.
        if _num(stage, "task_count") < SKEW_MIN_TASKS or ratio < SKEW_RATIO:
            continue
        if worst is None or ratio > worst[0]:
            worst = (ratio, stage)

    if worst is None:
        return []
    ratio, stage = worst
    return [
        _finding(
            "task_skew",
            "critical" if ratio >= SKEW_RATIO * 3 else "warning",
            "Task skew",
            f"Stage {stage.get('stage_id')} had one task {ratio:.1f}x slower than the median "
            f"across {int(_num(stage, 'task_count'))} tasks. Adding executors will not help "
            f"while one partition holds most of the data.",
        )
    ]


def _spill(metrics: dict[str, Any]) -> list[dict[str, Any]]:
    disk = _num(metrics, "disk_spilled_bytes")
    memory = _num(metrics, "memory_spilled_bytes")
    total = disk + memory
    if total < SPILL_MIN_BYTES:
        return []
    return [
        _finding(
            "disk_spill",
            "critical" if total >= SPILL_CRITICAL_BYTES else "warning",
            "Spilled to disk",
            f"{format_bytes(total)} spilled ({format_bytes(disk)} to disk, "
            f"{format_bytes(memory)} from memory). Executors ran out of memory and fell back "
            f"to disk, which is typically the slowest part of a job that spills.",
        )
    ]


def _gc(metrics: dict[str, Any]) -> list[dict[str, Any]]:
    gc = _num(metrics, "gc_time_ms")
    run = _num(metrics, "run_time_ms")
    if gc < GC_MIN_MS or run <= 0 or gc / run < GC_FRACTION:
        return []
    return [
        _finding(
            "high_gc",
            "warning",
            "High GC time",
            f"{gc / run:.0%} of executor run time went to garbage collection "
            f"({gc / 1000:.0f}s of {run / 1000:.0f}s). Usually means executors are "
            f"undersized for the partition size.",
        )
    ]


def _over_provisioned(metrics: dict[str, Any]) -> list[dict[str, Any]]:
    cores = _num(metrics, "total_cores")
    tasks = _num(metrics, "task_count")
    if cores < OVER_PROVISION_MIN_CORES or tasks <= 0:
        return []
    ratio = cores / tasks
    if ratio < OVER_PROVISION_RATIO:
        return []
    return [
        _finding(
            "over_provisioned",
            "info",
            "Over-provisioned",
            f"{int(cores)} cores for {int(tasks)} tasks ({ratio:.1f} cores per task). Most of "
            f"the cluster sat idle and was still paid for.",
        )
    ]


def _small_files(metrics: dict[str, Any]) -> list[dict[str, Any]]:
    tasks = _num(metrics, "task_count")
    output = _num(metrics, "output_bytes")
    if tasks < SMALL_FILE_MIN_TASKS or output <= 0:
        return []
    average = output / tasks
    if average >= SMALL_FILE_BYTES:
        return []
    return [
        _finding(
            "small_files",
            "warning",
            "Small output files",
            f"{int(tasks)} tasks wrote {format_bytes(output)}, averaging "
            f"{format_bytes(average)} each. Small files are cheap to write and expensive for "
            f"every reader afterwards; consider coalescing before the write.",
        )
    ]


def _failed_tasks(metrics: dict[str, Any]) -> list[dict[str, Any]]:
    failed = _num(metrics, "failed_task_count")
    if failed <= 0:
        return []
    total = _num(metrics, "task_count")
    return [
        _finding(
            "failed_tasks",
            "warning",
            "Failed tasks",
            f"{int(failed)} of {int(total)} tasks failed and were retried. The job succeeded, "
            f"but retries cost time and often precede an outright failure later.",
        )
    ]
