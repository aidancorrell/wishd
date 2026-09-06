"""Robust anomaly detection over metric history.

Static thresholds work, and every one of them is a number somebody invented. This
module is what turns "row count below 1000" into "row count unlike the last three
Tuesdays" -- which is the difference between a monitor a team maintains and one
they set once and stop trusting.

Three properties, in the order they matter:

  **Robust, not merely statistical.** Everything is medians and MAD, never means
  and standard deviation. The training data always contains the incidents we are
  trying to detect; one 10x outage moves a mean enough to hide the next one, and
  moves a median almost not at all. Confirmed anomalies are excluded from the
  baseline outright, because otherwise every incident makes the detector blinder.

  **Seasonal.** Weekend volume is a fifth of weekday volume on most real
  pipelines. A detector without a seasonal term reports an incident every
  Saturday, and a team that gets paged every Saturday turns it off by March.

  **Quiet.** It refuses to arm without enough history *and* enough elapsed time,
  it will not divide by a zero MAD, and it requires a relative deviation as well
  as a statistical one. Silence is the default here exactly as in `heuristics.py`.

No numpy, no scipy, no statsmodels -- see ADR-006. The series are a few hundred
points long and the arithmetic below is the whole algorithm.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from .checks import Point

# --- arming ------------------------------------------------------------------
# Both gates have to pass. Points alone are not enough: backfilling a busy hourly
# job yields 30 observations from one afternoon, which describes a lunchtime
# rather than a week.
MIN_TRAINING_POINTS = 8
MIN_TRAINING_DAYS = 7.0

# --- band --------------------------------------------------------------------
DEFAULT_SENSITIVITY = 3.0     # multiples of robust sigma
MAD_TO_SIGMA = 1.4826         # makes MAD comparable to a standard deviation on
                              # normally distributed data, which is the scale
                              # people's intuition for "3 sigma" is calibrated to

# A perfectly flat series has MAD 0, which would make any deviation infinitely
# anomalous -- 1001 rows after a year of exactly 1000 would page someone. The
# floor is expressed relative to the level, so it scales with the metric instead
# of assuming units.
MIN_RELATIVE_DEVIATION = 0.15


@dataclass
class Verdict:
    status: str                       # ok | breach | insufficient_data
    message: str
    value: float | None = None
    expected: float | None = None
    band_width: float = 0.0
    context: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------- primitives


def median(values: list[float]) -> float | None:
    ordered = sorted(v for v in values if v is not None)
    if not ordered:
        return None
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[middle])
    return (ordered[middle - 1] + ordered[middle]) / 2


def mad(values: list[float]) -> float:
    """Median absolute deviation. Zero for a constant series, by definition."""
    centre = median(values)
    if centre is None:
        return 0.0
    return median([abs(v - centre) for v in values if v is not None]) or 0.0


# ------------------------------------------------------------------ seasonality


def seasonal_period(points: list[Point]) -> str:
    """Which seasonal cycle, if any, this series plausibly has.

    Chosen from the observed cadence rather than from configuration, because the
    cadence is a fact we already hold and asking the user to declare it is asking
    them to get it wrong. A daily job's season is the week; an hourly job's season
    is the day. Anything irregular gets no seasonal term at all -- inventing one
    is how a detector starts explaining noise.
    """
    stamps = sorted(p.observed_at for p in points)
    if len(stamps) < 3:
        return "none"
    gaps = [
        (b - a).total_seconds() for a, b in zip(stamps, stamps[1:], strict=False) if b > a
    ]
    typical = median(gaps)
    if not typical:
        return "none"

    # Regularity test: an irregular series has gaps scattered around the median,
    # and a seasonal index built from it would be an artefact of arrival times.
    spread = mad(gaps)
    if spread > typical * 0.5:
        return "none"

    hour = 3600
    if 20 * hour <= typical <= 28 * hour:
        return "dayofweek"
    if 0.5 * hour <= typical <= 6 * hour:
        return "hourofday"
    return "none"


def _bucket(moment: datetime, period: str) -> int:
    if period == "dayofweek":
        return moment.weekday()
    if period == "hourofday":
        return moment.hour
    return 0


# -------------------------------------------------------------------- detection


def detect(
    points: list[Point],
    *,
    now: datetime | None = None,
    sensitivity: float = DEFAULT_SENSITIVITY,
    min_points: int = MIN_TRAINING_POINTS,
    min_days: float = MIN_TRAINING_DAYS,
) -> Verdict:
    """Is the newest observation unlike the ones before it?

    Never raises, and never claims anything it cannot support -- an unarmed
    detector returns `insufficient_data` rather than guessing.
    """
    usable = [p for p in points if p.value is not None]
    if not usable:
        return Verdict("insufficient_data", "no numeric observations yet")

    ordered = sorted(usable, key=lambda p: p.observed_at)
    latest = ordered[-1]
    history = ordered[:-1]
    now = now or latest.observed_at

    armed, why = _armed(ordered, min_points=min_points, min_days=min_days)
    if not armed:
        return Verdict("insufficient_data", why)

    # Feedback shapes the baseline before anything is computed from it.
    #
    #   `anomaly`  confirmed bad -> drop it, so a real incident cannot widen the
    #              band and mask its own recurrence.
    #   `expected` real and fine (Black Friday) -> keep it, because "expected"
    #              means normal and the band should learn it.
    baseline = [p for p in history if _feedback(p) != "anomaly"]
    if len(baseline) < min_points - 1:
        return Verdict(
            "insufficient_data",
            f"only {len(baseline)} usable training point(s) after excluding confirmed anomalies",
        )

    period = seasonal_period(ordered)
    level = _level(baseline)
    seasonal = _seasonal_index(baseline, period, level)

    residuals = [p.value - level - seasonal.get(_bucket(p.observed_at, period), 0.0)
                 for p in baseline]
    sigma = mad(residuals) * MAD_TO_SIGMA
    expected = level + seasonal.get(_bucket(latest.observed_at, period), 0.0)

    # Two floors on the band, and both are load-bearing. The relative one keeps a
    # flat series from flagging rounding; the sigma one keeps a noisy series from
    # having its band collapse when most residuals happen to coincide.
    band = max(sigma * sensitivity, abs(expected) * MIN_RELATIVE_DEVIATION)
    deviation = latest.value - expected

    context = {
        "expected": round(expected, 2),
        "band": round(band, 2),
        "seasonality": period,
        "training_points": len(baseline),
    }

    if _feedback(latest) == "expected":
        return Verdict(
            "ok",
            f"{_num(latest.value)} — outside the usual range, marked expected",
            value=latest.value, expected=expected, band_width=band, context=context,
        )

    if abs(deviation) > band:
        direction = "below" if deviation < 0 else "above"
        return Verdict(
            "breach",
            f"{_num(latest.value)} — {_num(abs(deviation))} {direction} the expected "
            f"{_num(expected)} (±{_num(band)}, {len(baseline)} points"
            + (f", {period} seasonality)" if period != "none" else ")"),
            value=latest.value, expected=expected, band_width=band, context=context,
        )

    return Verdict(
        "ok",
        f"{_num(latest.value)} — expected {_num(expected)} (±{_num(band)})",
        value=latest.value, expected=expected, band_width=band, context=context,
    )


def _armed(ordered: list[Point], *, min_points: int, min_days: float) -> tuple[bool, str]:
    if len(ordered) < min_points:
        return False, (
            f"{len(ordered)} of {min_points} training observations — "
            f"not enough history to judge yet"
        )
    span = ordered[-1].observed_at - ordered[0].observed_at
    if span < timedelta(days=min_days):
        return False, (
            f"{len(ordered)} observations spanning only {span.days}d of the "
            f"{min_days:g}d training window"
        )
    return True, ""


def _feedback(point: Point) -> str | None:
    value = (point.context or {}).get("feedback")
    return value if value in ("expected", "anomaly") else None


def _level(points: list[Point]) -> float:
    """A robust level that tolerates trend.

    A plain median over a growing series sits below the recent values and reports
    steady growth as a permanent anomaly. Taking the median of the most recent
    window instead tracks the trend without a regression, and without giving the
    newest point enough weight to drag the level onto itself.
    """
    window = max(len(points) // 2, 5)
    recent = points[-window:]
    return median([p.value for p in recent]) or 0.0


def _seasonal_index(points: list[Point], period: str, level: float) -> dict[int, float]:
    """Median detrended offset per seasonal bucket.

    A bucket with only one observation is left at zero: a single Saturday is not
    evidence about Saturdays, and treating it as such would bake one day's noise
    into every future weekend.
    """
    if period == "none":
        return {}
    buckets: dict[int, list[float]] = {}
    for point in points:
        buckets.setdefault(_bucket(point.observed_at, period), []).append(point.value - level)
    return {
        bucket: median(values) or 0.0
        for bucket, values in buckets.items()
        if len(values) >= 2
    }


def _num(value: float) -> str:
    if abs(value) >= 1000:
        return f"{value:,.0f}"
    if abs(value) >= 10:
        return f"{value:.0f}"
    return f"{value:.2f}".rstrip("0").rstrip(".")
