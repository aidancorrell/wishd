"""Bounded ingest queue with load shedding.

Why this exists, in one sentence: **the gateway must never make someone's Spark
job slower.**

The OpenLineage HTTP transport emits from the driver thread. If we write to
Postgres inline, then our p99 is their p99, and a bad minute on our database
becomes a bad minute on their ETL. Nobody keeps an observability tool that does
that. So requests hand off to a bounded queue and return immediately; worker
threads do the database work out of band.

The interesting decision is what happens when the queue fills. We **shed**: drop
the event, count it, and still answer 2xx. Two reasons:

  - Blocking would push the latency we just removed straight back onto the
    producer, which defeats the entire design.
  - Answering 5xx would make well-behaved OpenLineage clients retry, turning a
    struggling database into a retry storm while the driver waits on each
    attempt.

Losing observability data is a real cost, and we do not pretend otherwise --
`dropped` is surfaced in ingest health precisely so a saturated deployment is
visible rather than quietly lossy.
"""

from __future__ import annotations

import logging
import queue
import threading
from typing import Any

from .config import env

log = logging.getLogger("dataspine.queue")

DEFAULT_QUEUE_SIZE = 10_000
DEFAULT_WORKERS = 4


def async_enabled() -> bool:
    return env.get("DATASPINE_INGEST_ASYNC", "true").lower() in ("1", "true", "yes")


class IngestQueue:
    """Bounded work queue draining to Postgres on background threads."""

    def __init__(self, maxsize: int | None = None, workers: int | None = None) -> None:
        self.maxsize = maxsize or int(
            env.get("DATASPINE_INGEST_QUEUE_SIZE", DEFAULT_QUEUE_SIZE)
        )
        self.worker_count = workers or int(
            env.get("DATASPINE_INGEST_WORKERS", DEFAULT_WORKERS)
        )
        self._q: queue.Queue[Any] = queue.Queue(maxsize=self.maxsize)
        self._threads: list[threading.Thread] = []
        self._stopping = threading.Event()
        self._lock = threading.Lock()
        self.dropped = 0
        self.processed = 0
        self.failed = 0

    # ------------------------------------------------------------- lifecycle

    def start(self) -> None:
        if self._threads:
            return
        self._stopping.clear()
        for i in range(self.worker_count):
            thread = threading.Thread(
                target=self._worker, name=f"dataspine-ingest-{i}", daemon=True
            )
            thread.start()
            self._threads.append(thread)
        log.info("ingest queue started: %d workers, maxsize=%d", self.worker_count, self.maxsize)

    def drain_and_stop(self, timeout: float = 30.0) -> None:
        """Finish what is queued, then stop.

        A deploy must not vaporise in-flight events. We drain first and only
        then signal workers to exit.
        """
        if not self._threads:
            return
        try:
            self._q.join()
        except Exception:  # pragma: no cover - defensive
            log.exception("error draining ingest queue")
        self._stopping.set()
        for _ in self._threads:
            # Unblock any worker parked on an empty queue.
            try:
                self._q.put_nowait(_SENTINEL)
            except queue.Full:  # pragma: no cover
                pass
        current = threading.current_thread()
        for thread in self._threads:
            # A worker can reach this path during interpreter shutdown; joining
            # yourself raises rather than deadlocking, but either is a bug.
            if thread is current:
                continue
            thread.join(timeout=timeout / max(len(self._threads), 1))
        self._threads.clear()

    # ---------------------------------------------------------------- submit

    def submit(self, payload: Any) -> bool:
        """Enqueue. Returns False if we shed rather than blocking.

        `put_nowait` is the whole point -- any blocking put would reintroduce the
        producer latency this class exists to remove.
        """
        try:
            self._q.put_nowait(payload)
            return True
        except queue.Full:
            with self._lock:
                self.dropped += 1
                total = self.dropped
            # Log the first few and then every thousandth: saturation must be
            # visible without the log itself becoming the bottleneck.
            if total <= 5 or total % 1000 == 0:
                log.warning(
                    "ingest queue full (maxsize=%d); shed %d event(s) so far", self.maxsize, total
                )
            return False

    # ---------------------------------------------------------------- stats

    def stats(self) -> dict[str, int]:
        return {
            "queue_depth": self._q.qsize(),
            "queue_maxsize": self.maxsize,
            "dropped_events": self.dropped,
            "processed_events": self.processed,
            "failed_events": self.failed,
        }

    # ---------------------------------------------------------------- worker

    def _worker(self) -> None:
        # Imported here, and called through the module, so tests can monkeypatch
        # the write path -- and so this module does not import the DB at import
        # time.
        from . import ingest as ingest_module
        from .db import connection
        from .events import RunEvent

        while True:
            try:
                item = self._q.get(timeout=0.25)
            except queue.Empty:
                if self._stopping.is_set():
                    return
                continue

            if item is _SENTINEL:
                self._q.task_done()
                return

            try:
                with connection() as conn:
                    ingest_module.ingest_run_event(conn, RunEvent.model_validate(item))
                with self._lock:
                    self.processed += 1
            except Exception:
                # A failed write must never kill the worker -- one poison event
                # would otherwise stop all ingest until a restart.
                with self._lock:
                    self.failed += 1
                log.exception("failed to ingest a queued event")
            finally:
                self._q.task_done()


_SENTINEL = object()

_queue: IngestQueue | None = None


def get_queue() -> IngestQueue:
    global _queue
    if _queue is None:
        _queue = IngestQueue()
    return _queue


def reset_queue() -> None:
    """Drop the singleton. Tests use this when changing queue configuration."""
    global _queue
    if _queue is not None:
        _queue.drain_and_stop(timeout=5)
    _queue = None
