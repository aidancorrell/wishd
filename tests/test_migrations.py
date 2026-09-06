"""The upgrade path: applying migrations to a database that already has data.

This was the largest hole in the suite. Every other test runs against a schema
migrated once, from empty, at session start — so the operation an operator
actually performs, and the only one that can destroy their data, had no coverage
at all. Migration 019 contains a `truncate` by design, which is exactly the kind
of migration that needs a test and exactly the kind nobody writes one for.

Three properties are worth holding:

  **Incremental application preserves data.** Migrating to version N, writing
  rows, then migrating to HEAD must not lose the rows that later migrations do
  not deliberately remove.

  **Concurrent migration is safe.** Two replicas booting simultaneously used to
  race: both read `schema_migrations`, both saw the same version unapplied, both
  ran it. An advisory lock serialises them.

  **Applied migrations are immutable.** Editing a file that has already run is
  silent otherwise — the version is recorded, so it is skipped forever and the
  database quietly stops matching the repository.
"""

from __future__ import annotations

import shutil
import threading
from pathlib import Path

import psycopg
import pytest
from psycopg.rows import dict_row

from dataspine import db

MIGRATIONS = Path(__file__).parent.parent / "migrations"


@pytest.fixture()
def blank_db(tmp_path_factory):
    """A genuinely empty database — not the session one, which is migrated."""
    pgserver = pytest.importorskip("pgserver")
    data_dir = tmp_path_factory.mktemp("pgdata_migrate")
    server = pgserver.get_server(data_dir, cleanup_mode="stop")
    return server.get_uri()


@pytest.fixture()
def staged(tmp_path, monkeypatch):
    """A migrations directory we can apply in slices, to simulate an old deploy."""
    staging = tmp_path / "migrations"
    staging.mkdir()
    monkeypatch.setattr(db, "migrations_dir", lambda: staging)

    def install(upto: str | None = None) -> list[Path]:
        for existing in staging.glob("*.sql"):
            existing.unlink()
        chosen = []
        for path in sorted(MIGRATIONS.glob("*.sql")):
            shutil.copy(path, staging / path.name)
            chosen.append(path)
            if upto and path.stem.startswith(upto):
                break
        return chosen

    return staging, install


def _versions(url: str) -> list[str]:
    with psycopg.connect(url, row_factory=dict_row) as conn:
        return [
            r["version"]
            for r in conn.execute(
                "select version from schema_migrations order by version"
            ).fetchall()
        ]


# ------------------------------------------------------------ upgrading with data


def test_data_written_at_an_old_version_survives_upgrade_to_head(blank_db, staged):
    """The operator's actual experience: a running deployment, then an upgrade.

    Runs are the table that must never be lost — they are the archive everything
    else is derived from, and unlike `cost_line_items` no migration is entitled
    to clear them.
    """
    _, install = staged

    # An older deployment: migrate only as far as the run tree existing.
    install(upto="004")
    db.migrate(blank_db)
    early = _versions(blank_db)
    assert early, "no migrations applied at the staged version"

    with psycopg.connect(blank_db, row_factory=dict_row) as conn:
        conn.execute(
            "insert into jobs (namespace, name, integration) values ('t', 'nightly', 'AIRFLOW')"
        )
        job_id = conn.execute("select id from jobs limit 1").fetchone()["id"]
        conn.execute(
            "insert into runs (run_id, job_id, state) "
            "values ('11111111-1111-7111-8111-111111111111', %s, 'COMPLETED')",
            (job_id,),
        )
        conn.commit()

    # Now upgrade to HEAD, as `dataspine migrate` does on deploy.
    install()
    applied = db.migrate(blank_db)
    assert applied, "upgrade applied nothing; the staging fixture is wrong"
    assert len(_versions(blank_db)) > len(early)

    with psycopg.connect(blank_db, row_factory=dict_row) as conn:
        survived = conn.execute(
            "select count(*) c from runs where run_id = '11111111-1111-7111-8111-111111111111'"
        ).fetchone()["c"]
    assert survived == 1, "an upgrade destroyed a run"


def test_migrating_twice_is_a_no_op(blank_db, staged):
    _, install = staged
    install()
    first = db.migrate(blank_db)
    second = db.migrate(blank_db)
    assert first, "nothing applied on a blank database"
    assert second == [], f"re-running re-applied {second}"


# --------------------------------------------------------------- concurrency


def test_two_replicas_migrating_at_once_do_not_race(blank_db, staged):
    """The defect this lock exists for.

    Before the advisory lock, two processes booting together both read
    `schema_migrations`, both saw version N unapplied, and both executed it. Some
    migrations survive that; the one containing a `truncate` does not.

    Threads rather than processes because the lock is held by Postgres, not by
    the client — two connections is the condition being tested, and threads give
    two connections.
    """
    _, install = staged
    install()

    results: list[object] = []
    barrier = threading.Barrier(2)

    def run() -> None:
        try:
            barrier.wait(timeout=30)
            results.append(db.migrate(blank_db))
        except Exception as exc:  # noqa: BLE001 - the failure mode under test
            results.append(exc)

    threads = [threading.Thread(target=run) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=120)

    assert len(results) == 2
    for result in results:
        assert not isinstance(result, Exception), f"concurrent migrate raised: {result!r}"

    # Exactly one of them did the work; the other found it already done.
    did_work = [r for r in results if r]
    assert len(did_work) == 1, "both processes applied migrations"

    versions = _versions(blank_db)
    assert len(versions) == len(set(versions)), "a migration was recorded twice"


# ------------------------------------------------------------------ immutability


def test_editing_an_applied_migration_is_refused(blank_db, staged):
    """Silent otherwise: the version is recorded, so the edit never runs and the
    database stops matching the repository with nothing to notice."""
    staging, install = staged
    install(upto="003")
    db.migrate(blank_db)

    victim = sorted(staging.glob("*.sql"))[-1]
    victim.write_text(victim.read_text() + "\n-- an edit made after this was applied\n")

    with pytest.raises(db.MigrationDrift) as caught:
        db.migrate(blank_db)
    assert victim.stem in str(caught.value)


def test_a_deployment_predating_checksums_is_adopted_not_refused(blank_db, staged):
    """Existing installs have no recorded checksum. Refusing to start would
    block every upgrade on evidence of drift we do not have."""
    _, install = staged
    install(upto="003")
    db.migrate(blank_db)

    with psycopg.connect(blank_db) as conn:
        conn.execute("update schema_migrations set checksum = null")
        conn.commit()

    install()
    db.migrate(blank_db)  # must not raise

    with psycopg.connect(blank_db, row_factory=dict_row) as conn:
        missing = conn.execute(
            "select count(*) c from schema_migrations where checksum is null"
        ).fetchone()["c"]
    assert missing == 0, "checksums were not backfilled"
