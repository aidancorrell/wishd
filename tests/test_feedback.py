"""One-click feedback, and what it does to the baseline.

The feature is small; the reasoning is not. Two labels that look like a pair of
opposites actually do opposite things to the training data, and getting them the
wrong way round would quietly break detection:

  `expected`  real and fine — Black Friday happened. **Keep** it in the baseline.
  `anomaly`   confirmed bad. **Drop** it, or the incident widens the band enough
              to hide its own recurrence.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from dataspine import checks, monitors

NOW = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)


@pytest.fixture()
def anomaly_monitor(conn):
    monitors.apply_specs(
        conn,
        [monitors.MonitorSpec(
            "vol", "row_count", "dataset", "fct_orders", {}, source="t.yml", mode="anomaly"
        )],
        sources=["t.yml"],
    )
    return monitors.get_monitor(conn, "vol")


def _points(conn, monitor_id, values):
    start = NOW - timedelta(days=len(values))
    checks.store_points(
        conn,
        monitor_id,
        [
            checks.Point(start + timedelta(days=i), f"subject-{i}", float(v))
            for i, v in enumerate(values)
        ],
    )


def test_anomaly_mode_survives_apply_and_reload(conn, anomaly_monitor):
    assert anomaly_monitor["mode"] == "anomaly"
    # Anomaly monitors need no thresholds — that is the point of the mode.
    assert anomaly_monitor["config"] == {}


def test_threshold_keys_are_not_required_in_anomaly_mode():
    specs = monitors.parse_spec(
        {"monitors": [{"name": "v", "kind": "row_count", "dataset": "x", "mode": "anomaly"}]}
    )
    assert specs[0].mode == "anomaly"


def test_anomaly_mode_is_refused_for_deterministic_kinds():
    """schema_drift has no numeric series to learn from, and pretending otherwise
    would produce a monitor that silently never fires."""
    with pytest.raises(monitors.SpecError, match="no numeric series"):
        monitors.parse_spec(
            {"monitors": [{"name": "s", "kind": "schema_drift", "dataset": "x",
                           "mode": "anomaly"}]}
        )


def test_feedback_round_trips(conn, anomaly_monitor):
    _points(conn, anomaly_monitor["id"], [100, 200, 300])
    assert monitors.set_feedback(conn, anomaly_monitor["id"], "subject-1", "expected") is True

    labelled = {
        p["subject"]: p["feedback"] for p in monitors.recent_points(conn, anomaly_monitor["id"])
    }
    assert labelled["subject-1"] == "expected"
    assert labelled["subject-0"] is None


def test_feedback_can_be_cleared(conn, anomaly_monitor):
    """A misclick must be undoable without a SQL prompt."""
    _points(conn, anomaly_monitor["id"], [100, 200])
    monitors.set_feedback(conn, anomaly_monitor["id"], "subject-0", "anomaly")
    monitors.set_feedback(conn, anomaly_monitor["id"], "subject-0", None)

    labelled = {
        p["subject"]: p["feedback"] for p in monitors.recent_points(conn, anomaly_monitor["id"])
    }
    assert labelled["subject-0"] is None


def test_feedback_rejects_an_unknown_label(conn, anomaly_monitor):
    with pytest.raises(ValueError, match="feedback must be"):
        monitors.set_feedback(conn, anomaly_monitor["id"], "subject-0", "sort-of-fine")


def test_unknown_subject_reports_that_it_matched_nothing(conn, anomaly_monitor):
    assert monitors.set_feedback(conn, anomaly_monitor["id"], "no-such-run", "expected") is False


def test_feedback_reaches_the_detector_through_evaluate(conn, anomaly_monitor):
    """The wiring test: a label set in the database has to change the verdict.

    `evaluate` reads history back out of Postgres rather than using the points it
    just collected, so feedback stored on the row is the only way this can work.
    """
    _points(conn, anomaly_monitor["id"], [1000] * 13 + [9000])
    before = checks.judge(
        anomaly_monitor,
        [
            checks.Point(p["observed_at"], p["subject"], p["value"],
                         {**(p["context"] or {}), "feedback": p["feedback"]})
            for p in monitors.recent_points(conn, anomaly_monitor["id"])
        ],
        now=NOW,
    )
    assert before.status == "breach"

    monitors.set_feedback(conn, anomaly_monitor["id"], "subject-13", "expected")
    after = checks.evaluate(conn, anomaly_monitor, now=NOW)
    assert after["status"] == "ok"
    assert "expected" in after["message"]


def test_collecting_again_does_not_wipe_feedback(conn, anomaly_monitor):
    """Re-collection overwrites an observation's context. The label lives in its
    own column precisely so an hourly check cannot erase a human's correction."""
    _points(conn, anomaly_monitor["id"], [100, 200])
    monitors.set_feedback(conn, anomaly_monitor["id"], "subject-0", "anomaly")
    _points(conn, anomaly_monitor["id"], [100, 200])

    labelled = {
        p["subject"]: p["feedback"] for p in monitors.recent_points(conn, anomaly_monitor["id"])
    }
    assert labelled["subject-0"] == "anomaly"


# ------------------------------------------------------------------ the surface


def test_feedback_endpoint_updates_and_redirects(api_client):
    from dataspine import db

    with db.connection() as conn:
        monitors.apply_specs(
            conn,
            [monitors.MonitorSpec("vol", "row_count", "dataset", "x", {}, source="t.yml",
                                  mode="anomaly")],
            sources=["t.yml"],
        )
        monitor = monitors.get_monitor(conn, "vol")
        _points(conn, monitor["id"], [100, 200])
        conn.commit()

    response = api_client.post(
        "/monitors/vol/feedback",
        data={"subject": "subject-0", "feedback": "expected"},
        follow_redirects=False,
    )
    assert response.status_code == 303

    with db.connection() as conn:
        labelled = {
            p["subject"]: p["feedback"] for p in monitors.recent_points(conn, monitor["id"])
        }
    assert labelled["subject-0"] == "expected"


def test_feedback_buttons_appear_only_for_anomaly_monitors(api_client):
    """A threshold monitor has no baseline to teach, so offering the buttons
    would be offering a control that does nothing."""
    from dataspine import db

    with db.connection() as conn:
        monitors.apply_specs(
            conn,
            [
                monitors.MonitorSpec("learned", "row_count", "dataset", "x", {},
                                     source="t.yml", mode="anomaly"),
                monitors.MonitorSpec("stated", "row_count", "dataset", "x", {"min": 1},
                                     source="t.yml"),
            ],
            sources=["t.yml"],
        )
        for name in ("learned", "stated"):
            _points(conn, monitors.get_monitor(conn, name)["id"], [100, 200])
        conn.commit()

    assert "feedback" in api_client.get("/monitors/learned").text
    assert "feedback" not in api_client.get("/monitors/stated").text


def test_api_exposes_the_learned_band(api_client):
    """The band has to be inspectable, or "unlike the last three Tuesdays" is
    just a number the tool refuses to justify."""
    from dataspine import db

    with db.connection() as conn:
        monitors.apply_specs(
            conn,
            [monitors.MonitorSpec("vol", "row_count", "dataset", "x", {}, source="t.yml",
                                  mode="anomaly")],
            sources=["t.yml"],
        )
        monitor = monitors.get_monitor(conn, "vol")
        _points(conn, monitor["id"], [1000] * 13 + [95])
        checks.evaluate(conn, monitor, now=NOW)
        conn.commit()

    detail = api_client.get("/api/v1/monitors/vol").json()
    threshold = detail["results"][0]["threshold"]
    assert threshold["mode"] == "anomaly"
    assert threshold["expected"] == pytest.approx(1000, rel=0.1)
    assert json.dumps(detail)  # serialisable end to end
