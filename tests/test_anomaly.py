"""Anomaly detection: robust seasonal decomposition + MAD bounds.

Static thresholds work, and every one of them is a number somebody invented. This
is what turns "row count below 1000" into "row count unlike the last three
Tuesdays".

The design constraint that shapes every test here is **robustness**, in the
statistical sense: the baseline is built from medians and MAD rather than means
and standard deviation, because the data always contains the very incidents we
are trying to detect. One 10× spike moves a mean enough to hide the next one; it
moves a median almost not at all.

The second constraint is silence. These tests spend more effort proving the
detector stays quiet -- on short history, on constant series, on ordinary weekly
seasonality -- than proving it fires. A detector that cries wolf is worse than no
detector, because it also burns the static thresholds sitting next to it.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from dataspine import anomaly
from dataspine.checks import Point

NOW = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)


def daily(values, *, start=None, feedback=None):
    """One point per day, oldest first, ending at NOW."""
    start = start or (NOW - timedelta(days=len(values)))
    return [
        Point(
            observed_at=start + timedelta(days=i),
            subject=f"s{i}",
            value=float(v),
            context={"feedback": feedback[i]} if feedback else {},
        )
        for i, v in enumerate(values)
    ]


def hourly(values):
    start = NOW - timedelta(hours=len(values))
    return [
        Point(observed_at=start + timedelta(hours=i), subject=f"s{i}", value=float(v))
        for i, v in enumerate(values)
    ]


# ------------------------------------------------------------------- primitives


def test_median_and_mad():
    assert anomaly.median([3, 1, 2]) == 2
    assert anomaly.median([4, 1, 2, 3]) == 2.5
    assert anomaly.median([]) is None
    # MAD of a symmetric spread is the middle absolute deviation.
    assert anomaly.mad([1, 2, 3, 4, 5]) == 1.0


def test_mad_ignores_a_single_extreme_value():
    """The property the whole detector rests on.

    A standard deviation computed over data containing one incident is inflated
    by that incident, which is exactly when the bound needs to be tight. The
    spread here is non-zero on purpose: a flat series has MAD 0, which makes the
    comparison vacuous rather than demonstrating anything.
    """
    clean = anomaly.mad([10, 12, 8, 11, 9, 10, 13])
    poisoned = anomaly.mad([10, 12, 8, 11, 9, 10, 13, 100_000])

    # A standard deviation would go from ~1.7 to ~35,000 on the same data.
    assert clean > 0
    assert poisoned <= clean * 2


# -------------------------------------------------------------------- cadence


def test_cadence_detection_picks_weekly_seasonality_for_daily_data():
    assert anomaly.seasonal_period(daily([1] * 30)) == "dayofweek"


def test_cadence_detection_picks_daily_seasonality_for_hourly_data():
    assert anomaly.seasonal_period(hourly([1] * 72)) == "hourofday"


def test_cadence_detection_gives_up_on_irregular_data():
    """Not every series has a season, and inventing one is how a detector starts
    explaining noise."""
    points = [
        Point(observed_at=NOW - timedelta(hours=h), subject=f"s{h}", value=1.0)
        for h in (0, 3, 50, 51, 900, 1000)
    ]
    assert anomaly.seasonal_period(points) == "none"


# ------------------------------------------------------------------- training


def test_too_few_points_is_not_armed():
    """A monitor must not start making claims from four observations."""
    verdict = anomaly.detect(daily([100, 102, 98, 101]), now=NOW)
    assert verdict.status == "insufficient_data"
    assert "training" in verdict.message


def test_enough_points_but_too_short_a_span_is_not_armed():
    """Twenty points from one afternoon do not describe a week.

    Backfilling a busy hourly job would otherwise arm a weekly-seasonal detector
    on a couple of hours of data.
    """
    points = [
        Point(observed_at=NOW - timedelta(minutes=5 * i), subject=f"s{i}", value=100.0)
        for i in range(30)
    ]
    verdict = anomaly.detect(points, now=NOW)
    assert verdict.status == "insufficient_data"


def test_arms_once_there_is_a_week_of_daily_history():
    verdict = anomaly.detect(daily([100, 101, 99, 100, 102, 98, 101, 100, 99, 101]), now=NOW)
    assert verdict.status == "ok"


# ------------------------------------------------------------------ detection


def test_a_collapsed_row_count_is_flagged():
    """The headline case: last night wrote a tenth of the usual rows."""
    verdict = anomaly.detect(daily([1000, 1010, 990, 1005, 995, 1000, 1002, 998, 1001, 95]),
                             now=NOW)
    assert verdict.status == "breach"
    assert "95" in verdict.message
    assert verdict.expected is not None


def test_a_spike_is_flagged():
    verdict = anomaly.detect(daily([100, 101, 99, 100, 102, 98, 101, 100, 99, 5000]), now=NOW)
    assert verdict.status == "breach"


def test_ordinary_variation_is_not_flagged():
    verdict = anomaly.detect(daily([100, 104, 97, 101, 103, 98, 99, 102, 100, 103]), now=NOW)
    assert verdict.status == "ok"


def test_a_constant_series_does_not_flag_a_trivial_change():
    """MAD is zero for a perfectly flat series, so a naive bound divides by zero
    and calls 1001 infinitely anomalous. A relative floor keeps it quiet."""
    verdict = anomaly.detect(daily([1000] * 14 + [1001]), now=NOW)
    assert verdict.status == "ok"


def test_a_constant_series_still_flags_a_real_break():
    verdict = anomaly.detect(daily([1000] * 14 + [3]), now=NOW)
    assert verdict.status == "breach"


def test_steady_growth_is_not_an_anomaly():
    """A table that grows a little every day is the normal case, and a detector
    that pages on the trend it should be modelling is useless."""
    verdict = anomaly.detect(daily([100 + 5 * i for i in range(30)]), now=NOW)
    assert verdict.status == "ok"


def test_weekly_seasonality_is_learned_not_flagged():
    """Weekend volumes are a fifth of weekday volumes on most real pipelines.

    Without seasonal decomposition every Saturday is an incident, which is the
    fastest way for a team to turn the detector off.
    """
    values = []
    start = NOW - timedelta(days=42)
    for i in range(42):
        day = (start + timedelta(days=i)).weekday()
        values.append(200 if day >= 5 else 1000)
    points = daily(values, start=start)
    verdict = anomaly.detect(points, now=NOW)
    assert verdict.status == "ok", f"seasonality treated as anomaly: {verdict.message}"


def test_a_weekday_collapse_is_flagged_even_though_weekends_are_low():
    """The other half of the same property: learning the season must not blind
    the detector to a weekday that looks like a weekend."""
    values = []
    start = NOW - timedelta(days=42)
    for i in range(42):
        day = (start + timedelta(days=i)).weekday()
        values.append(200 if day >= 5 else 1000)
    if (start + timedelta(days=41)).weekday() < 5:
        values[-1] = 200
    else:  # make the final day a weekday so the assertion tests what it claims
        start -= timedelta(days=2)
        values = []
        for i in range(42):
            day = (start + timedelta(days=i)).weekday()
            values.append(200 if day >= 5 else 1000)
        values[-1] = 200

    verdict = anomaly.detect(daily(values, start=start), now=NOW)
    assert verdict.status == "breach"


def test_sensitivity_widens_the_band():
    values = [1000, 1010, 990, 1005, 995, 1000, 1002, 998, 1001, 1200]
    assert anomaly.detect(daily(values), now=NOW, sensitivity=3.0).status == "breach"
    assert anomaly.detect(daily(values), now=NOW, sensitivity=50.0).status == "ok"


# -------------------------------------------------------------------- feedback


def test_confirmed_anomalies_are_excluded_from_the_baseline():
    """Otherwise a sustained outage makes the detector blind to its own recurrence.

    Several bad days rather than one: MAD is robust enough that a *single*
    outlier barely moves it — which is the point of using MAD — so one marked
    point cannot demonstrate the exclusion doing any work.
    """
    values = [1000] * 10 + [50, 60, 40] + [1000]
    marked = [None] * 10 + ["anomaly"] * 3 + [None]
    baseline = anomaly.detect(daily(values, feedback=marked), now=NOW)
    unmarked = anomaly.detect(daily(values), now=NOW)

    assert baseline.band_width < unmarked.band_width
    # And the consequence that matters: with the outage excluded, a repeat of it
    # is still detectable.
    repeat = anomaly.detect(daily([1000] * 10 + [50, 60, 40] + [45], feedback=marked), now=NOW)
    assert repeat.status == "breach"


def test_a_point_marked_expected_does_not_alert():
    """Black Friday was real, and it was fine. Saying so once must stop it being
    an incident."""
    values = [1000] * 13 + [9000]
    marked = [None] * 13 + ["expected"]
    verdict = anomaly.detect(daily(values, feedback=marked), now=NOW)
    assert verdict.status == "ok"
    assert "expected" in verdict.message


def test_marking_expected_keeps_the_point_in_the_baseline():
    """"Expected" means normal, so it should teach the band, not be deleted from
    it — the opposite of a confirmed anomaly."""
    values = [1000] * 13 + [1400]
    marked = [None] * 13 + ["expected"]
    widened = anomaly.detect(daily(values, feedback=marked), now=NOW)
    excluded = anomaly.detect(daily(values[:-1]), now=NOW)
    assert widened.band_width >= excluded.band_width


# ------------------------------------------------------------------- messages


def test_the_message_carries_the_numbers_that_triggered_it():
    """Same rule as heuristics.py: a reader must be able to disagree with the
    band rather than having to trust it."""
    verdict = anomaly.detect(daily([1000] * 13 + [95]), now=NOW)
    assert "95" in verdict.message
    assert "expected" in verdict.message.lower()


def test_nulls_are_ignored_rather_than_treated_as_zero():
    """A missing observation is not a row count of nought, and treating it as one
    would manufacture an incident out of an absent facet."""
    points = daily([1000] * 14)
    points[3].value = None
    verdict = anomaly.detect(points, now=NOW)
    assert verdict.status == "ok"


@pytest.mark.parametrize("kind", ["row_count", "job_duration", "queue_delay", "freshness"])
def test_anomaly_mode_is_available_for_the_numeric_kinds(kind):
    from dataspine import monitors

    assert kind in monitors.ANOMALY_KINDS
