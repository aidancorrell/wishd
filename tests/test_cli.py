"""CLI tests.

The CLI is the surface every user touches first — the quickstart is four of
these commands in a row — and it was the one module in the project with no tests
at all. That is not a coverage statistic: the two commands the README tells a new
user to run in sequence could not be run in sequence, because `runs` truncated
run ids to 8 characters and `tree` demanded a full 36-character UUID. The round
trip a user actually performs is asserted here so it cannot break silently again.

These drive Typer's runner against the same real Postgres as the rest of the
suite. The CLI opens its own pooled connection from DATASPINE_DATABASE_URL rather
than taking one, so these cannot use the rolled-back `conn` fixture and truncate
instead.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from typer.testing import CliRunner

from dataspine import db
from dataspine.cli import app

runner = CliRunner()


@pytest.fixture()
def cli(database_url: str):
    """A CLI whose commands hit the test database, truncated between tests."""
    db.reset_pool()
    yield runner
    with db.connection() as conn:
        conn.execute(
            "truncate events, run_datasets, runs, datasets, jobs, spark_apps, "
            "artifacts, artifact_blobs, monitors, dataset_snapshots, "
            "column_profiles, external_checks, dataset_entities, incidents, "
            "clusters, cost_line_items restart identity cascade"
        )
        conn.commit()
    db.reset_pool()


def invoke(*args: str):
    result = runner.invoke(app, list(args))
    if result.exception and not isinstance(result.exception, SystemExit):
        raise result.exception
    return result


# ------------------------------------------------------------------ round trip


def test_seed_then_runs_then_tree_round_trips(cli):
    """The quickstart, exactly as the README prints it.

    The failure this guards was not that any command was broken on its own —
    each worked given the right input. It was that the output of one could not
    be the input of the next, which is the only way anybody uses them.
    """
    assert invoke("seed", "--pipelines", "2").exit_code == 0

    listed = invoke("runs", "--roots")
    assert listed.exit_code == 0

    # Take the id off the screen the way a user does: the first column of the
    # first data row, exactly as printed, with no reformatting.
    displayed = listed.output.splitlines()[1].split()[0]

    tree = invoke("tree", displayed)
    assert tree.exit_code == 0, tree.output
    assert "analytics_daily" in tree.output
    # The whole point of `tree`: three producers, one tree.
    assert "AIRFLOW" in tree.output and "DBT" in tree.output and "SPARK" in tree.output


def test_listed_run_ids_are_unambiguous(cli):
    """Every id `runs` prints must resolve on its own.

    UUIDv7 spends its first 48 bits on a millisecond timestamp, so the runs of a
    single pipeline — which start within seconds of each other — share a long
    leading hex run. A truncation that is merely *usually* unique is unique
    exactly until someone looks at one pipeline's runs, which is the normal case.
    """
    invoke("seed", "--pipelines", "3")

    listed = invoke("runs", "--limit", "50")
    displayed = [line.split()[0] for line in listed.output.splitlines()[1:] if "-" in line]
    assert len(displayed) > 10, "expected the seeded pipeline's runs, not just roots"

    for shown in displayed:
        result = invoke("tree", shown, "--here")
        assert result.exit_code == 0, f"{shown!r} did not resolve: {result.output}"


# ----------------------------------------------------------- prefix resolution


def test_tree_accepts_a_full_uuid(cli):
    invoke("seed", "--pipelines", "1")
    with db.connection() as conn:
        run_id = conn.execute("select run_id from runs limit 1").fetchone()["run_id"]
    assert invoke("tree", str(run_id)).exit_code == 0


def test_tree_rejects_an_ambiguous_prefix_by_listing_candidates(cli):
    """Ambiguity is reported with the candidates, not with 'be more specific'.

    The user cannot be more specific: they are holding everything the previous
    command printed. Showing the full ids is the only response that lets them
    continue without a database query.
    """
    invoke("seed", "--pipelines", "1")
    with db.connection() as conn:
        rows = conn.execute("select run_id from runs").fetchall()
    # The most-colliding *pair*, not the prefix every run shares. Runs spread
    # across a 45-minute pipeline diverge early; the ones that collide are the
    # ones emitted in the same millisecond -- a Spark application and its SQL
    # execution -- which is exactly the pair a user is choosing between.
    shared = _longest_shared_prefix_of_any_pair(
        [str(r["run_id"]).replace("-", "") for r in rows]
    )
    assert len(shared) >= 8, "runs emitted together should share a timestamp prefix"

    result = invoke("tree", shared)
    assert result.exit_code == 1
    assert "ambiguous" in result.output
    # At least one full id offered, so the next command is copy-paste.
    assert any(str(r["run_id"]) in result.output.replace("\n", "") for r in rows[:10])


def test_tree_reports_an_unknown_id_without_a_traceback(cli):
    result = invoke("tree", "deadbeef")
    assert result.exit_code == 1
    assert "no run matches" in result.output
    assert "Traceback" not in result.output


def test_tree_reports_a_malformed_id_without_a_traceback(cli):
    """This is the exact regression: `UUID(run_id)` raised ValueError straight
    through Typer, so a mistyped id met the user with a Python traceback."""
    result = invoke("tree", "not-a-uuid")
    assert result.exit_code == 1
    assert "not a run id" in result.output
    assert "Traceback" not in result.output


def test_tree_rejects_an_overlong_id(cli):
    result = invoke("tree", "0" * 40)
    assert result.exit_code == 1
    assert "too long" in result.output


def test_resolve_run_id_is_case_and_dash_insensitive(conn):
    """A user pasting from the web UI brings whatever casing it rendered."""
    from dataspine import queries
    from dataspine.events import RunEvent
    from dataspine.ingest import ingest_run_event
    from dataspine.simulate import build_pipeline

    for event in build_pipeline():
        ingest_run_event(conn, RunEvent.model_validate(event))
    run_id = conn.execute("select run_id from runs limit 1").fetchone()["run_id"]

    assert queries.resolve_run_id(conn, str(run_id)) == run_id
    assert queries.resolve_run_id(conn, str(run_id).upper()) == run_id
    assert queries.resolve_run_id(conn, str(run_id).replace("-", "")) == run_id
    assert queries.resolve_run_id(conn, f"  {run_id}  ") == run_id


def test_resolve_run_id_rejects_empty(conn):
    from dataspine import queries

    with pytest.raises(queries.RunIdError):
        queries.resolve_run_id(conn, "   ")


# ------------------------------------------------------------------- --here


def test_tree_here_starts_at_the_run_not_the_root(cli):
    """Without --here, any id in a tree renders the whole tree. With it, the
    subtree — which is how you look at one failing model rather than the DAG."""
    invoke("seed", "--pipelines", "1")
    with db.connection() as conn:
        leaf = conn.execute(
            "select r.run_id from runs r join jobs j on j.id = r.job_id "
            "where j.name like 'model.analytics.%' order by r.depth desc limit 1"
        ).fetchone()["run_id"]

    whole = invoke("tree", str(leaf))
    subtree = invoke("tree", str(leaf), "--here")
    assert whole.exit_code == subtree.exit_code == 0
    assert "analytics_daily" in whole.output
    assert "analytics_daily" not in subtree.output


# --------------------------------------------------------------------- reads


def test_runs_filters(cli):
    invoke("seed", "--pipelines", "2")

    spark = invoke("runs", "--integration", "SPARK", "--limit", "50")
    assert spark.exit_code == 0
    assert "AIRFLOW" not in spark.output

    failed = invoke("runs", "--state", "FAILED", "--limit", "50")
    assert failed.exit_code == 0
    assert "COMPLETED" not in failed.output

    # Rich ellipsizes the job column to fit the terminal, so assert on what the
    # filter selected rather than on cell text that may be visually truncated.
    everything = invoke("runs", "--limit", "50")
    by_job = invoke("runs", "--job", "fct_orders", "--limit", "50")
    assert by_job.exit_code == 0
    assert 0 < _row_count(by_job.output) < _row_count(everything.output)
    assert _row_count(invoke("runs", "--job", "no_such_model").output) == 0


def test_health_reports_the_correlation_alarm(cli):
    """`unstitched_runs` is the one number that means the product is broken, so
    it must be present and zero on a clean seed."""
    invoke("seed", "--pipelines", "2")
    result = invoke("health")
    assert result.exit_code == 0
    assert "unstitched_runs" in result.output
    assert "0" in result.output.split("unstitched_runs")[1].split("\n")[0]


def test_reads_on_an_empty_database_do_not_crash(cli):
    """A fresh install runs these before ingesting anything."""
    for args in (("runs",), ("runs", "--roots"), ("health",), ("monitors",), ("incidents",)):
        result = invoke(*args)
        assert result.exit_code == 0, f"{args} failed: {result.output}"


# -------------------------------------------------------------------- monitors


def test_apply_is_idempotent_and_dry_run_writes_nothing(cli, tmp_path):
    spec = tmp_path / "m.yml"
    spec.write_text(
        "monitors:\n"
        "  - name: fct_orders_freshness\n"
        "    kind: freshness\n"
        "    dataset: fct_orders\n"
        "    max_age_minutes: 90\n"
    )
    invoke("seed", "--pipelines", "1")

    dry = invoke("apply", str(spec), "--dry-run")
    assert dry.exit_code == 0
    with db.connection() as conn:
        assert conn.execute("select count(*) c from monitors").fetchone()["c"] == 0

    first = invoke("apply", str(spec))
    assert first.exit_code == 0
    assert "created" in first.output

    second = invoke("apply", str(spec))
    assert second.exit_code == 0
    assert "unchanged" in second.output
    with db.connection() as conn:
        assert conn.execute("select count(*) c from monitors").fetchone()["c"] == 1


def test_apply_backfill_arms_without_moving_last_status(cli, tmp_path):
    """The invariant `apply`'s own comment flags, now asserted.

    Arming evaluates a new monitor against the run archive so it is useful on day
    one. It must do that with `record=False`: if arming wrote `last_status`, the
    first real `check` would see no transition and would never alert on a breach
    that arming had just discovered. The monitor would look healthy precisely
    because it had already found the problem.
    """
    invoke("seed", "--pipelines", "2")
    spec = tmp_path / "m.yml"
    # A freshness ceiling the seeded data cannot meet, so arming finds a breach.
    spec.write_text(
        "monitors:\n"
        "  - name: stale_orders\n"
        "    kind: freshness\n"
        "    dataset: fct_orders\n"
        "    max_age_minutes: 1\n"
    )

    result = invoke("apply", str(spec))
    assert result.exit_code == 0

    with db.connection() as conn:
        row = conn.execute(
            "select last_status from monitors where name = 'stale_orders'"
        ).fetchone()
    assert row["last_status"] in (None, "unknown"), (
        "arming moved last_status; the first real check will now see no "
        "transition and will never alert on this breach"
    )

    # `check` exits non-zero on a breach on purpose: it is meant to fail a CI
    # job or a cron alert. A breach found here is the assertion, not a problem.
    checked = invoke("check")
    assert checked.exit_code == 1, checked.output
    with db.connection() as conn:
        row = conn.execute(
            "select last_status from monitors where name = 'stale_orders'"
        ).fetchone()
    assert row["last_status"] == "breach", "the first real check did not record the breach"


def test_apply_disables_a_removed_monitor_rather_than_deleting_it(cli, tmp_path):
    """A monitor usually vanishes from a file because of a bad merge, and
    deleting would take its history with it."""
    spec = tmp_path / "m.yml"
    spec.write_text(
        "monitors:\n"
        "  - name: a\n    kind: freshness\n    dataset: fct_orders\n    max_age_minutes: 90\n"
        "  - name: b\n    kind: freshness\n    dataset: fct_order_items\n    max_age_minutes: 90\n"
    )
    invoke("apply", str(spec), "--no-backfill")

    spec.write_text(
        "monitors:\n"
        "  - name: a\n    kind: freshness\n    dataset: fct_orders\n    max_age_minutes: 90\n"
    )
    result = invoke("apply", str(spec), "--no-backfill")
    assert result.exit_code == 0
    with db.connection() as conn:
        row = conn.execute("select enabled from monitors where name = 'b'").fetchone()
    assert row is not None, "removing a monitor from a file deleted its history"
    assert row["enabled"] is False


def test_apply_reports_a_bad_spec_without_a_traceback(cli, tmp_path):
    spec = tmp_path / "m.yml"
    spec.write_text("monitors:\n  - name: nope\n    kind: not_a_real_kind\n")
    result = invoke("apply", str(spec))
    assert result.exit_code == 1
    assert "Traceback" not in result.output


def test_monitors_resolve_shows_what_a_target_matched(cli, tmp_path):
    """The dataset match is loose by necessity — one table arrives under several
    identities — so the command that prints what it resolved to is the one that
    makes the looseness reviewable."""
    invoke("seed", "--pipelines", "1")
    spec = tmp_path / "m.yml"
    spec.write_text(
        "monitors:\n"
        "  - name: fct_orders_freshness\n    kind: freshness\n"
        "    dataset: fct_orders\n    max_age_minutes: 90\n"
    )
    invoke("apply", str(spec))

    result = invoke("monitors", "--resolve")
    assert result.exit_code == 0
    assert "fct_orders" in result.output


# ------------------------------------------------------------------- plumbing


def test_migrate_is_idempotent(cli):
    result = invoke("migrate")
    assert result.exit_code == 0
    assert "already up to date" in result.output


def test_replay_rebuilds_the_projection_from_the_event_archive(cli):
    """`events` is the source of truth; runs/jobs/datasets are a projection.
    Replay is what makes a correlator logic change safe to ship."""
    invoke("seed", "--pipelines", "1")
    with db.connection() as conn:
        before = conn.execute("select count(*) c from runs").fetchone()["c"]
        conn.execute("delete from runs")
        conn.commit()

    result = invoke("replay", "--yes")
    assert result.exit_code == 0
    with db.connection() as conn:
        after = conn.execute("select count(*) c from runs").fetchone()["c"]
    assert after == before


def test_maintain_provisions_partitions(cli):
    result = invoke("maintain", "--months-ahead", "2")
    assert result.exit_code == 0


def test_seed_backdates_run_ids_to_their_own_start_time(cli):
    """UUIDv7 carries a timestamp, so a backdated run must carry a backdated id.

    Stamping every seeded id at generation time gave several days of "history"
    one shared id prefix — an artefact no real producer emits, and the reason
    the truncated ids in `runs` collided.
    """
    invoke("seed", "--pipelines", "3")
    with db.connection() as conn:
        rows = conn.execute(
            "select run_id, started_at from runs where parent_run_id is null "
            "order by started_at"
        ).fetchall()
    assert len(rows) >= 3

    for row in rows:
        stamped = datetime.fromtimestamp(
            (row["run_id"].int >> 80) / 1000, tz=UTC
        )
        assert abs(stamped - row["started_at"]) < timedelta(minutes=5), (
            f"run id timestamp {stamped} does not match started_at {row['started_at']}"
        )

    # And therefore: distinct days, distinct prefixes.
    prefixes = {str(r["run_id"])[:8] for r in rows}
    assert len(prefixes) == len(rows)


def _row_count(output: str) -> int:
    """The trailing `N run(s)` line the listings print."""
    for line in reversed(output.splitlines()):
        if "run(s)" in line:
            return int(line.split()[0])
    raise AssertionError(f"no count line in output:\n{output}")


def _shared_prefix(a: str, b: str) -> str:
    length = 0
    while length < min(len(a), len(b)) and a[length] == b[length]:
        length += 1
    return a[:length]


def _longest_shared_prefix_of_any_pair(values: list[str]) -> str:
    ordered = sorted(values)
    return max(
        (_shared_prefix(x, y) for x, y in zip(ordered, ordered[1:], strict=False)),
        key=len,
        default="",
    )
