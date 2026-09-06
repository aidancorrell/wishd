"""The overview page: the live board and the concurrency timeline.

Two questions on one page, and the tests are split the same way. The board's
hard part is deciding what counts as *live*; the timeline's is geometry, which
is where an off-by-one renders as a bar in the wrong place rather than as an
exception.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from dataspine import queries, web
from dataspine.events import RunEvent
from dataspine.ingest import ingest_run_event
from dataspine.simulate import build_pipeline, truncate_at

NOW = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)


def ingest(conn, events):
    for event in events:
        ingest_run_event(conn, RunEvent.model_validate(event))


def in_flight(*, ending_at: datetime | None = None, fraction: float = 0.5):
    """A pipeline caught mid-execution, cut at a fraction of its *own* span.

    The cut point must come from the very pipeline being used. `simulate` draws
    model durations from `random.randint(40, 400)`, so spans range from under
    six minutes to over thirteen -- and two builds never agree. A fixed cutoff
    flaked because a fast pipeline had already finished by then; measuring a
    *probe* pipeline and truncating a second one flaked for the same reason, one
    level further down.

    So: build once, measure that list, and shift its timestamps so the cut point
    lands exactly on `now`. Shifting `eventTime` is enough -- the correlator
    takes `started_at` and `ended_at` from it.

    Returns (events, cut_point).
    """
    now = ending_at or datetime.now(UTC)
    events = build_pipeline(start=now)
    moments = [datetime.fromisoformat(e["eventTime"]) for e in events]
    span = max(moments) - min(moments)
    shift = now - (min(moments) + span * fraction)

    shifted = [
        {**e, "eventTime": (datetime.fromisoformat(e["eventTime"]) + shift).isoformat()}
        for e in events
    ]
    return truncate_at(shifted, now), now


# ----------------------------------------------------------------- live board


def test_live_pipelines_finds_work_still_in_flight(conn):
    """Truncating a real event sequence is what produces an in-flight tree."""
    events, _ = in_flight()
    ingest(conn, events)

    live = queries.live_pipelines(conn)
    assert len(live) == 1
    assert live[0]["running"] > 0
    assert live[0]["total"] >= live[0]["done"]


def test_live_survives_a_root_that_reported_complete_early(conn):
    """A COMPLETED root above live children must still count as in flight.

    dbt and Spark both report their own terminal state without waiting for
    children, so a board that keyed on the root's state would go blank exactly
    when the cluster is busiest. This is the same lesson `root` taught the
    correlator, arriving in a second place.
    """
    root, child = uuid4(), uuid4()
    now = datetime.now(UTC)
    ingest(conn, [
        _event("START", now - timedelta(minutes=5), root, "ns", "parent"),
        _event("COMPLETE", now - timedelta(minutes=4), root, "ns", "parent"),
        _event("START", now - timedelta(minutes=4), child, "ns", "child", parent=root),
    ])

    live = queries.live_pipelines(conn)
    assert [p["job_name"] for p in live] == ["parent"]
    assert live[0]["running"] == 1


def test_finished_pipelines_are_not_live(conn):
    ingest(conn, build_pipeline(start=datetime.now(UTC) - timedelta(hours=2)))
    assert queries.live_pipelines(conn) == []


def test_a_pipeline_that_went_quiet_is_not_counted_as_live(conn):
    """The case OpenLineage cannot express: a cluster that died mid-run.

    There is no heartbeat, so those runs stay RUNNING forever and any elapsed
    time computed from `now` grows without bound — an abandoned run renders as
    the busiest thing on the page. Silence is the only available evidence.
    """
    events, _ = in_flight(ending_at=datetime.now(UTC) - timedelta(days=17))
    ingest(conn, events)

    live = queries.live_pipelines(conn)
    assert len(live) == 1, "still RUNNING in the database, as it would really be"

    card = web._live_card(live[0], {}, datetime.now(UTC))
    assert card["stale"] is True
    assert card["silent_ms"] > 0


def test_a_recently_active_pipeline_is_not_stale(conn):
    events, _ = in_flight()
    ingest(conn, events)
    card = web._live_card(queries.live_pipelines(conn)[0], {}, datetime.now(UTC))
    assert card["stale"] is False


# -------------------------------------------------------------------- slowness


def test_baselines_need_enough_history_to_be_trusted(conn):
    """Two samples is not a baseline. A comparison nobody can trust gets ignored,
    and then gets ignored when it matters."""
    now = datetime.now(UTC)
    for i in range(2):
        run = uuid4()
        ingest(conn, [
            _event("START", now - timedelta(hours=i + 2), run, "ns", "thin"),
            _event("COMPLETE", now - timedelta(hours=i + 2, minutes=-5), run, "ns", "thin"),
        ])
    assert queries.job_baselines(conn, ["thin"]) == {}


def test_baseline_ignores_failed_runs(conn):
    """A failed run's duration measures how long it took to give up."""
    now = datetime.now(UTC)
    for i in range(4):
        run = uuid4()
        ingest(conn, [
            _event("START", now - timedelta(hours=i + 1), run, "ns", "mixed"),
            _event("COMPLETE", now - timedelta(hours=i + 1) + timedelta(minutes=10),
                   run, "ns", "mixed"),
        ])
    quick = uuid4()
    ingest(conn, [
        _event("START", now - timedelta(minutes=30), quick, "ns", "mixed"),
        _event("FAIL", now - timedelta(minutes=29), quick, "ns", "mixed"),
    ])

    baseline = queries.job_baselines(conn, ["mixed"])["mixed"]
    assert baseline == pytest.approx(10 * 60 * 1000, rel=0.05), (
        "a one-minute failure dragged the baseline down"
    )


def test_slow_is_only_claimed_with_a_baseline():
    """Without history the honest statement is the elapsed time, not a verdict."""
    pipeline = {
        "job_name": "j", "started_at": NOW - timedelta(hours=5), "total": 4,
        "done": 1, "running": 3, "failed": 0, "integrations": "DBT",
        "spark_namespaces": None, "last_event_at": NOW,
    }
    assert web._live_card(pipeline, {}, NOW)["slow"] is False
    assert web._live_card(pipeline, {"j": 60_000}, NOW)["slow"] is True


# ----------------------------------------------------------- peak concurrency


def test_peak_concurrency_counts_the_busiest_instant():
    base = NOW
    runs = [
        {"started_at": base, "ended_at": base + timedelta(minutes=30)},
        {"started_at": base + timedelta(minutes=5), "ended_at": base + timedelta(minutes=20)},
        {"started_at": base + timedelta(minutes=10), "ended_at": base + timedelta(minutes=15)},
        {"started_at": base + timedelta(minutes=40), "ended_at": base + timedelta(minutes=50)},
    ]
    peak = queries.peak_concurrency(runs, now=base)
    assert peak["peak"] == 3
    assert peak["at"] == base + timedelta(minutes=10)


def test_a_handover_is_not_concurrency():
    """One run ending exactly as another begins is a handover, not two at once."""
    base = NOW
    runs = [
        {"started_at": base, "ended_at": base + timedelta(minutes=10)},
        {"started_at": base + timedelta(minutes=10), "ended_at": base + timedelta(minutes=20)},
    ]
    assert queries.peak_concurrency(runs, now=base)["peak"] == 1


def test_an_unfinished_run_is_open_until_now():
    base = NOW
    runs = [
        {"started_at": base, "ended_at": None},
        {"started_at": base + timedelta(minutes=5), "ended_at": None},
    ]
    assert queries.peak_concurrency(runs, now=base + timedelta(minutes=10))["peak"] == 2


def test_peak_of_nothing_is_zero():
    assert queries.peak_concurrency([], now=NOW) == {"peak": 0, "at": None}


# ------------------------------------------------------------ timeline lanes


def _bar(**kw):
    return {
        "run_id": uuid4(), "job_name": "j", "integration": "AIRFLOW",
        "state": "COMPLETED", "duration_ms": 1000, **kw,
    }


def test_a_run_starting_before_the_window_is_clamped_and_marked():
    """The most important row on the page: a long-running pipeline that began
    before the window. A negative offset would render it off-screen."""
    since, now = NOW, NOW + timedelta(hours=1)
    lanes = web._timeline_lanes(
        [_bar(started_at=since - timedelta(hours=3), ended_at=None, state="RUNNING")],
        since=since, now=now,
    )
    bar = lanes[0]["bars"][0]
    assert bar["left"] == 0.0
    assert bar["clipped"] is True
    assert bar["live"] is True
    assert 0 < bar["width"] <= 100


def test_bar_geometry_stays_inside_the_window():
    since, now = NOW, NOW + timedelta(hours=1)
    lanes = web._timeline_lanes(
        [_bar(started_at=since + timedelta(minutes=30),
              ended_at=since + timedelta(minutes=45))],
        since=since, now=now,
    )
    bar = lanes[0]["bars"][0]
    assert bar["left"] == pytest.approx(50, abs=0.1)
    assert bar["width"] == pytest.approx(25, abs=0.1)


def test_a_very_short_run_still_gets_a_clickable_width():
    since, now = NOW, NOW + timedelta(days=7)
    lanes = web._timeline_lanes(
        [_bar(started_at=since + timedelta(hours=1),
              ended_at=since + timedelta(hours=1, seconds=3))],
        since=since, now=now,
    )
    assert lanes[0]["bars"][0]["width"] >= 0.006


def test_lanes_with_failures_sort_first():
    since, now = NOW, NOW + timedelta(hours=1)
    lanes = web._timeline_lanes(
        [
            _bar(job_name="aaa_healthy", started_at=since, ended_at=now),
            _bar(job_name="zzz_broken", started_at=since, ended_at=now, state="FAILED"),
        ],
        since=since, now=now,
    )
    assert [lane["job_name"] for lane in lanes] == ["zzz_broken", "aaa_healthy"]


# -------------------------------------------------------------------- the page


def test_overview_renders_with_live_work(api_client):
    events, _ = in_flight()
    assert api_client.post("/api/v1/lineage/batch", json=events).status_code in (200, 201)

    page = api_client.get("/overview")
    assert page.status_code == 200
    assert "In flight" in page.text
    assert "Concurrency" in page.text


def test_overview_renders_on_an_empty_database(api_client):
    """A fresh install opens this page before ingesting anything."""
    page = api_client.get("/overview")
    assert page.status_code == 200
    assert "Nothing running." in page.text


def test_an_unknown_window_falls_back_rather_than_erroring(api_client):
    """A hand-edited URL must not 500 the page."""
    page = api_client.get("/overview?window=nonsense")
    assert page.status_code == 200


@pytest.mark.parametrize("window", ["1h", "6h", "24h", "7d"])
def test_every_window_renders(api_client, window):
    assert api_client.get(f"/overview?window={window}").status_code == 200


def _event(kind, when, run_id, namespace, name, parent=None):
    run_facets = {}
    if parent:
        run_facets["parent"] = {
            "_producer": "test", "_schemaURL": "test",
            "run": {"runId": str(parent)},
            "job": {"namespace": namespace, "name": "parent"},
        }
    return {
        "eventTime": when.isoformat(),
        "producer": "test",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        "eventType": kind,
        "run": {"runId": str(run_id), "facets": run_facets},
        "job": {"namespace": namespace, "name": name},
        "inputs": [],
        "outputs": [],
    }


# ------------------------------------------------------- inline expansion


def test_lane_bars_carry_their_tree(conn):
    """A lane expands in place, so the tree has to be on the bar already."""
    events, _ = in_flight()
    ingest(conn, events)
    executions = queries.concurrency_timeline(
        conn, since=datetime.now(UTC) - timedelta(hours=6)
    )
    trees = queries.trees_for(conn, [e["run_id"] for e in executions])

    lanes = web._timeline_lanes(
        executions, since=datetime.now(UTC) - timedelta(hours=6),
        now=datetime.now(UTC), trees=trees,
    )
    assert lanes[0]["bars"][0]["tree"], "the bar cannot expand without its steps"


def test_a_bar_past_the_cap_has_no_tree_rather_than_a_wrong_one():
    """`<details>` renders its contents whether open or shut, so every
    expandable row is paid for on every load. Past the cap the row links out."""
    since, now = NOW, NOW + timedelta(hours=1)
    lanes = web._timeline_lanes(
        [_bar(started_at=since, ended_at=now)], since=since, now=now, trees={}
    )
    assert lanes[0]["bars"][0]["tree"] == []


def test_expansion_priority_is_failed_then_live_then_recent():
    """The cap must fall on the rows nobody opens."""
    base = NOW
    healthy_old = _bar(state="COMPLETED", started_at=base, ended_at=base + timedelta(minutes=1))
    healthy_new = _bar(state="COMPLETED", started_at=base + timedelta(hours=2),
                       ended_at=base + timedelta(hours=2, minutes=1))
    live = _bar(state="RUNNING", started_at=base + timedelta(minutes=30), ended_at=None)
    failed = _bar(state="FAILED", started_at=base, ended_at=base + timedelta(minutes=2))

    ordered = web._worth_expanding([healthy_old, healthy_new, live, failed])
    assert ordered[0] is failed
    assert ordered[1] is live
    assert ordered[2] is healthy_new, "newer green run beats older green run"


def test_trees_for_is_one_query_not_one_per_root(conn):
    """The N+1 this shares with the run list."""
    for _ in range(3):
        ingest(conn, build_pipeline(start=datetime.now(UTC) - timedelta(hours=3)))
    roots = [r["run_id"] for r in conn.execute(
        "select run_id from runs where parent_run_id is null"
    ).fetchall()]

    trees = queries.trees_for(conn, roots)
    assert len(trees) == len(roots)
    assert all(trees[root] for root in roots)
    # Depth is assigned for the indent the template draws.
    assert {r["depth_display"] for r in trees[roots[0]]} >= {1, 2}


def test_trees_for_of_nothing_is_empty(conn):
    assert queries.trees_for(conn, []) == {}


def test_overview_expands_without_a_link_inside_a_summary(api_client):
    """A link nested in a <summary> is both a navigation and a toggle, which is
    ambiguous to a mouse and broken to a keyboard."""
    import re

    events, _ = in_flight()
    api_client.post("/api/v1/lineage/batch", json=events)
    html = api_client.get("/overview").text

    for match in re.finditer(r"<summary>(.*?)</summary>", html, re.S):
        assert "<a " not in match.group(1), "link nested inside a summary"
    assert "<details" in html
    assert "Full detail, SQL and metrics" in html


# ------------------------------------------------- redundant state encoding


def test_the_state_dot_is_named_not_hidden(api_client):
    """The dot is often the only carrier of state -- run rows and timeline
    entries show it with no state word beside it -- so it must not be
    aria-hidden."""
    events, _ = in_flight()
    api_client.post("/api/v1/lineage/batch", json=events)
    html = api_client.get("/overview").text

    assert 'class="dot dot-running"' in html
    assert 'aria-label="running"' in html
    assert 'class="dot dot-running"\n      role="img" aria-hidden' not in html


def test_every_state_has_a_distinct_shape_rule():
    """Colour alone leaves a deuteranope, a greyscale print and a bad projector
    with nothing. Each state must differ in silhouette, not just hue."""
    css = (
        Path(__file__).resolve().parents[1]
        / "src" / "dataspine" / "static" / "style.css"
    ).read_text()

    failed = css[css.index(".dot-failed {"):css.index(".dot-aborted {")]
    aborted = css[css.index(".dot-aborted {"):css.index(".dot-unknown")]
    unknown = css[css.index(".dot-unknown"):css.index(".dot-unknown") + 200]

    assert "border-radius: 1px" in failed, "failed should be a square"
    assert "rotate(45deg)" in aborted, "aborted should be a diamond"
    assert "dashed" in unknown, "unknown should be a dashed ring"
    # And a failed bar fills the track, so a break is findable by profile.
    bar = css[css.index(".tl-bar.s-failed"):css.index(".tl-bar.s-failed") + 120]
    assert "top: 1px" in bar


# --------------------------------------------------------- aligned columns


def test_in_flight_rows_emit_every_column(api_client):
    """An empty cell keeps its column. A column that moves per row is exactly
    what the grid replaces, so the cells are emitted unconditionally."""
    events, _ = in_flight()
    api_client.post("/api/v1/lineage/batch", json=events)
    html = api_client.get("/overview").text

    row = html[html.index('class="board-row'):]
    row = row[:row.index("</summary>")]
    for cell in ("c-job", "c-steps", "c-bar", "c-el", "c-med", "c-infra", "c-flags"):
        assert f'class="{cell}"' in row, f"{cell} missing -- the column would collapse"


def test_a_pipeline_without_a_baseline_still_holds_its_column(api_client):
    """The comparison cell is empty until there is enough history for a median.
    It must still be emitted, or that row's later columns shift left."""
    events, _ = in_flight()
    api_client.post("/api/v1/lineage/batch", json=events)
    html = api_client.get("/overview").text
    assert 'class="c-med"></span>' in html or 'class="c-med">~' in html
