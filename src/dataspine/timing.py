"""Was this run late, and whose fault was it?

Two numbers, both derived from facets we already store:

  **queue delay** — `run_after` to `start_date`. Time the run spent eligible but
  not started. Grows when a pool is saturated, slots are exhausted, or the
  scheduler is behind. Nothing to do with how fast the work itself is.

  **lateness** — `nominalStartTime` to the actual start. The 02:00 load did not
  happen at 02:00. This is the number a stakeholder actually feels.

Separating them matters: "the nightly run finished late" has completely
different fixes depending on whether it waited an hour for a slot or ran an hour
slower, and a single duration cannot tell you which.

This is deliberately computed from stored facets rather than from an OTLP
pipeline. See the module docstring in tests/test_timing.py for why.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


def _parse(value: Any) -> datetime | None:
    """Parse an ISO timestamp from a producer, tolerating anything else."""
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed


def _dig(source: Any, *path: str) -> Any:
    current = source
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def queue_delay_seconds(run_facets: dict[str, Any] | None) -> float | None:
    """Seconds between becoming eligible and actually starting."""
    facets = run_facets if isinstance(run_facets, dict) else {}
    dag_run = _dig(facets, "airflowDagRun", "dagRun") or _dig(facets, "airflow", "dagRun")
    eligible = _parse(_dig(dag_run, "run_after"))
    started = _parse(_dig(dag_run, "start_date"))
    if eligible is None or started is None:
        return None
    delta = (started - eligible).total_seconds()
    # Negative means clock skew between scheduler and worker, not a negative
    # wait. Reporting "-4s queued" would read as a bug in us.
    return delta if delta >= 0 else None


def lateness_seconds(
    run_facets: dict[str, Any] | None, started_at: datetime | None
) -> float | None:
    """Seconds between the scheduled window and the actual start."""
    facets = run_facets if isinstance(run_facets, dict) else {}
    nominal = _parse(_dig(facets, "nominalTime", "nominalStartTime"))
    if nominal is None or started_at is None:
        return None
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=UTC)
    delta = (started_at - nominal).total_seconds()
    # Early is not late. Only report the direction anyone cares about.
    return delta if delta > 0 else None


def run_timing(
    run_facets: dict[str, Any] | None, started_at: datetime | None
) -> dict[str, float | None]:
    """Both numbers for one run. Never raises."""
    return {
        "queue_delay_seconds": queue_delay_seconds(run_facets),
        "lateness_seconds": lateness_seconds(run_facets, started_at),
    }


def humanize(seconds: float | None) -> str | None:
    """Compact duration. Units are chosen so the magnitude reads at a glance --
    '1h05m' says more at a glance than '3900s'."""
    if seconds is None:
        return None
    total = int(seconds)
    if total < 60:
        return f"{total}s"
    minutes, secs = divmod(total, 60)
    if minutes < 60:
        return f"{minutes}m{secs:02d}s"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h{minutes:02d}m"
    days, hours = divmod(hours, 24)
    return f"{days}d{hours}h"
