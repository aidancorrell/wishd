"""Storage upkeep, and the failure it exists to prevent.

`ensure_partitions` provisions months ahead and has always been correct. Nothing
called it. Its own docstring assumed a cron the project never shipped, so a
deployment left running past its provisioned horizon writes into the DEFAULT
backstop partition instead — and that failure has no symptom. Inserts succeed.
Queries return. The table simply stops being partitioned, which is the exact
condition partitioning was introduced to avoid.

Two rules the tests pin down:

  **Provisioning is automatic, expiry is not.** Creating next month's partition
  is safe and idempotent. Dropping one destroys data, so it happens only when an
  operator sets a retention window.

  **A lapse must be visible.** Silent degradation is the whole problem, so the
  provisioned horizon is reported in health rather than left to be inferred.
"""

from __future__ import annotations

import pytest

from dataspine import retention, upkeep


@pytest.fixture(autouse=True)
def _enabled(monkeypatch):
    """The suite disables upkeep globally so it cannot race other tests; these
    tests are the ones that want it on."""
    monkeypatch.setenv(upkeep.ENABLED_ENV, "on")
    monkeypatch.delenv(upkeep.RETENTION_ENV, raising=False)


def test_provisioning_runs_without_anyone_scheduling_it(conn):
    result = upkeep.run_once(conn)
    assert "error" not in result

    names = retention.partition_names(conn)
    assert len(names) >= 2, "no headroom provisioned"


def test_retention_stays_off_unless_an_operator_asks(conn, monkeypatch):
    """A tool that silently deletes last quarter's archive because a default
    said three months is a tool nobody trusts with the archive."""
    upkeep.run_once(conn)
    before = set(retention.partition_names(conn))

    result = upkeep.run_once(conn)
    assert result["dropped"] == []
    assert set(retention.partition_names(conn)) == before


def test_retention_runs_when_configured(conn, monkeypatch):
    monkeypatch.setenv(upkeep.RETENTION_ENV, "1")
    retention.ensure_partitions(conn, around=_long_ago())
    aged = [n for n in retention.partition_names(conn) if "2020" in n]
    assert aged, "fixture did not create an old partition"

    result = upkeep.run_once(conn)
    assert result["dropped"], "configured retention dropped nothing"
    assert not [n for n in retention.partition_names(conn) if "2020" in n]


def _long_ago():
    from datetime import UTC, datetime

    return datetime(2020, 1, 15, tzinfo=UTC)


def test_a_lapsed_horizon_is_reported_not_silent(conn):
    """The number that matters is months *ahead*, not months total.

    A table with two years of history and nothing provisioned forward is the
    failure case, and a total count would look reassuring while it happened.
    """
    upkeep.run_once(conn)
    status = upkeep.status(conn)

    assert status["months_provisioned"] >= 2
    assert status["healthy"] is True


def test_upkeep_failure_never_takes_the_server_down(conn, monkeypatch):
    """A gateway that refuses to serve because it could not create next month's
    partition has turned a performance problem into an outage."""

    def explode(*args, **kwargs):
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(retention, "ensure_partitions", explode)
    result = upkeep.run_once(conn)

    assert result["error"] == "disk on fire"
    assert result["created"] == []


def test_upkeep_can_be_switched_off(monkeypatch):
    """An operator already running `dataspine maintain` from their own scheduler
    should not get a second thing provisioning partitions underneath it."""
    monkeypatch.setenv(upkeep.ENABLED_ENV, "off")
    assert upkeep.enabled() is False

    worker = upkeep.Upkeep()
    worker.start()
    assert worker._thread is None
    worker.stop()


def test_a_malformed_retention_setting_disables_rather_than_guesses(monkeypatch):
    monkeypatch.setenv(upkeep.RETENTION_ENV, "three")
    assert upkeep.retention_months() is None
