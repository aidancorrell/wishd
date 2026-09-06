"""Keeping the storage layer healthy without an operator remembering to.

`retention.ensure_partitions` provisions month partitions a few months ahead and
its own docstring says "running this from a cron every hour is harmless" -- a
cron this project never shipped. Nothing called it. So a deployment left alone
runs out of provisioned months, every insert lands in the DEFAULT partition
backstop, and the largest table in the system quietly degrades into the thing
partitioning existed to prevent. Nobody notices, because nothing fails.

Two halves, deliberately treated differently:

  **Provisioning is automatic.** Creating a partition for next month is safe,
  idempotent and cheap, so it runs on startup and on a slow tick. There is no
  reading of this where "we created a table you will need" is the wrong call.

  **Retention is opt-in.** Dropping a partition destroys data. A tool that
  silently deletes last quarter's archive because a default said three months is
  a tool nobody can trust with the archive. It runs only when
  `DATASPINE_RETENTION_MONTHS` is set, which is the operator saying so.

Running this in-process is a compromise, and worth stating: if the gateway is
down, provisioning is not happening. That is acceptable *here* because the
backstop partition means a lapse degrades performance rather than losing writes,
and because `dataspine maintain` remains available for anyone who would rather
drive it from their own scheduler. It would not be acceptable for alerting,
which is why Phase 03 deliberately refused the same pattern.
"""

from __future__ import annotations

import logging
import threading
from datetime import UTC, datetime
from typing import Any

from . import retention
from .config import env
from .db import connection

log = logging.getLogger("dataspine.upkeep")

ENABLED_ENV = "DATASPINE_UPKEEP"
RETENTION_ENV = "DATASPINE_RETENTION_MONTHS"
INTERVAL_ENV = "DATASPINE_UPKEEP_INTERVAL_SECONDS"
MONTHS_AHEAD_ENV = "DATASPINE_PARTITION_MONTHS_AHEAD"

DEFAULT_INTERVAL = 6 * 60 * 60  # six hours; partitions are a monthly concern
DEFAULT_MONTHS_AHEAD = 3

# Tracked so health can report a lapse instead of it being invisible.
_last_run: datetime | None = None
_last_error: str | None = None


def enabled() -> bool:
    """On unless switched off.

    Off is a real deployment choice, not just a test affordance: an operator
    already running `dataspine maintain` from their own scheduler does not want a
    second thing provisioning partitions underneath it, and anyone auditing what
    touches their database is entitled to turn ours off.
    """
    return env.get(ENABLED_ENV, "on").strip().lower() not in {
        "0", "off", "false", "no",
    }


def months_ahead() -> int:
    try:
        return max(1, int(env.get(MONTHS_AHEAD_ENV, DEFAULT_MONTHS_AHEAD)))
    except ValueError:
        return DEFAULT_MONTHS_AHEAD


def retention_months() -> int | None:
    raw = env.get(RETENTION_ENV, "").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError:
        log.warning("%s=%r is not a number; retention stays off", RETENTION_ENV, raw)
        return None
    return value if value > 0 else None


def run_once(conn: Any = None) -> dict[str, Any]:
    """Provision ahead, and expire only if the operator asked. Never raises.

    Upkeep failing must not take the process with it: a gateway that refuses to
    serve because it could not create next month's partition has turned a
    performance problem into an outage.
    """
    global _last_run, _last_error

    def _work(handle: Any) -> dict[str, Any]:
        created = retention.ensure_partitions(handle, months_ahead=months_ahead())
        dropped: list[str] = []
        keep = retention_months()
        if keep:
            dropped = retention.apply_retention(handle, keep_months=keep)
        return {"created": created, "dropped": dropped}

    try:
        if conn is not None:
            result = _work(conn)
        else:
            with connection() as handle:
                result = _work(handle)
        _last_run, _last_error = datetime.now(UTC), None
        if result["created"] or result["dropped"]:
            log.info(
                "upkeep: created %s, dropped %s", result["created"], result["dropped"]
            )
        return result
    except Exception as exc:  # noqa: BLE001 - upkeep must never kill the server
        _last_error = str(exc)
        log.warning("upkeep failed: %s", exc)
        return {"created": [], "dropped": [], "error": str(exc)}


def status(conn: Any = None) -> dict[str, Any]:
    """What health reports. `months_provisioned` is the number that matters.

    Counting *future* partitions rather than total is the whole point: a table
    with two years of history and nothing provisioned ahead is the failure case,
    and a total would look reassuring.
    """
    now = datetime.now(UTC)
    ahead = 0
    try:
        handle_names = retention.partition_names(conn) if conn is not None else None
        if handle_names is None:
            with connection() as handle:
                handle_names = retention.partition_names(handle)
        current = f"{now.year:04d}_{now.month:02d}"
        ahead = sum(1 for n in handle_names if n.split("events_")[-1] >= current)
    except Exception as exc:  # noqa: BLE001
        return {"months_provisioned": None, "error": str(exc)}

    return {
        "months_provisioned": ahead,
        "retention_months": retention_months(),
        "last_run_at": _last_run.isoformat() if _last_run else None,
        "last_error": _last_error,
        # One provisioned month means this month only -- inserts start hitting
        # the DEFAULT backstop as soon as it rolls over.
        "healthy": ahead >= 2,
    }


class Upkeep:
    """A daemon thread that ticks slowly. Stopped on shutdown."""

    def __init__(self, interval: int | None = None) -> None:
        self.interval = interval or int(
            env.get(INTERVAL_ENV, DEFAULT_INTERVAL)
        )
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if not enabled():
            log.info("upkeep disabled by %s; partitions are yours to maintain", ENABLED_ENV)
            return
        # Once immediately, so a deploy is itself the maintenance trigger and a
        # lapsed deployment repairs on restart.
        run_once()
        self._thread = threading.Thread(target=self._loop, name="upkeep", daemon=True)
        self._thread.start()

    def _loop(self) -> None:
        while not self._stop.wait(self.interval):
            run_once()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
