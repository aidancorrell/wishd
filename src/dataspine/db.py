"""Postgres access. Boring on purpose.

No ORM. The queries this system runs are recursive graph traversals and jsonb
merges -- both of which are clearer as SQL than as anything an ORM would
generate, and both of which we will want to hand-tune later.
"""

from __future__ import annotations

import hashlib
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .config import env

_pool: ConnectionPool | None = None
_pool_lock = threading.Lock()


def migrations_dir() -> Path:
    """Use bundled SQL in wheels, repository SQL in editable installs, or an override."""
    override = env.get("DATASPINE_MIGRATIONS_DIR")
    if override:
        return Path(override)

    candidates = [
        Path(__file__).resolve().parent / "migrations",  # installed wheel
        Path(__file__).resolve().parents[2] / "migrations",  # editable: src/dataspine/../..
    ]
    for path in candidates:
        if path.is_dir():
            return path
    raise RuntimeError(
        "Could not find the migrations directory. Set DATASPINE_MIGRATIONS_DIR. "
        f"Looked in: {', '.join(str(c) for c in candidates)}"
    )


def database_url() -> str:
    url = env.get("DATASPINE_DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATASPINE_DATABASE_URL is not set. Start Postgres with `make up` "
            "(Docker) or `make dev-db` (no Docker), or point it at your own instance."
        )
    return url


def get_pool() -> ConnectionPool:
    """Return the process-wide pool, creating it once.

    The lock is load-bearing, not defensive. Ingest workers start simultaneously
    and all call this on their first event; unguarded, several threads see
    `_pool is None` together, each builds a ConnectionPool, and every one but the
    last is orphaned -- leaking its connections to the server until the garbage
    collector gets around to them. Found via a stray ConnectionPool.__del__
    warning in the test suite.
    """
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:  # re-check: another thread may have won the race
                _pool = ConnectionPool(
                    database_url(),
                    min_size=1,
                    max_size=int(env.get("DATASPINE_POOL_SIZE", "10")),
                    kwargs={"row_factory": dict_row},
                    open=True,
                )
    return _pool


def reset_pool() -> None:
    """Drop the pool. Tests use this when swapping databases."""
    global _pool
    with _pool_lock:
        if _pool is not None:
            _pool.close()
        _pool = None


@contextmanager
def connection() -> Iterator[psycopg.Connection]:
    with get_pool().connection() as conn:
        yield conn


# A fixed key for the migration advisory lock. Any constant works as long as it
# never changes: the whole point is that every process picks the same number.
MIGRATION_LOCK_KEY = 8_675_309


class MigrationDrift(RuntimeError):
    """An already-applied migration file no longer matches what was applied."""


def _checksum(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def migrate(url: str | None = None) -> list[str]:
    """Apply every unapplied migration in filename order. Returns what ran.

    Numbered .sql files, each applied in its own transaction, tracked in
    `schema_migrations`.

    **Serialised across processes by a Postgres advisory lock.** Without it this
    is unsafe the moment a second replica exists: both read `schema_migrations`,
    both see version N unapplied, and both run it. Some migrations are
    idempotent and survive that; 019 contains a `truncate`, and does not. The
    lock is taken on the connection's session, so it is released when the
    connection closes even if this raises. A second process blocks here, then
    finds the work already done and applies nothing -- which is exactly the
    behaviour a rolling deploy needs.

    **Checksums detect an applied migration being edited afterwards.** Editing a
    file that has already run is silent today: the version is in the table, so it
    is skipped forever and the database no longer matches the repository. That is
    the kind of divergence that is discovered months later while debugging
    something else, so it is refused loudly here instead.
    """
    url = url or database_url()
    applied: list[str] = []
    files = sorted(migrations_dir().glob("*.sql"))
    with psycopg.connect(url, row_factory=dict_row) as conn:
        # Blocks until any other migrating process finishes. Taken before the
        # table is read, so the read cannot be stale by the time we act on it.
        conn.execute("select pg_advisory_lock(%s)", (MIGRATION_LOCK_KEY,))
        conn.commit()
        try:
            conn.execute(
                "create table if not exists schema_migrations "
                "(version text primary key, applied_at timestamptz not null default now())"
            )
            # Added after the fact, so existing deployments get the column
            # without a migration -- this table is our own bookkeeping and
            # cannot itself be migrated by the thing it tracks.
            conn.execute("alter table schema_migrations add column if not exists checksum text")
            conn.commit()

            rows = conn.execute("select version, checksum from schema_migrations").fetchall()
            done = {r["version"]: r["checksum"] for r in rows}

            for path in files:
                version = path.stem
                text = path.read_text()
                digest = _checksum(text)

                if version in done:
                    recorded = done[version]
                    if recorded is None:
                        # Applied before checksums existed. Adopt what is on
                        # disk rather than refusing to start: we have no
                        # evidence it drifted, and blocking every pre-existing
                        # deployment on an upgrade would be worse.
                        conn.execute(
                            "update schema_migrations set checksum = %s where version = %s",
                            (digest, version),
                        )
                        conn.commit()
                    elif recorded != digest:
                        raise MigrationDrift(
                            f"{version} has changed since it was applied "
                            f"(recorded {recorded[:12]}, on disk {digest[:12]}). "
                            "Migrations are immutable once applied; add a new one."
                        )
                    continue

                conn.execute(text)
                conn.execute(
                    "insert into schema_migrations (version, checksum) values (%s, %s)",
                    (version, digest),
                )
                conn.commit()
                applied.append(version)
        finally:
            conn.execute("select pg_advisory_unlock(%s)", (MIGRATION_LOCK_KEY,))
            conn.commit()
    return applied
