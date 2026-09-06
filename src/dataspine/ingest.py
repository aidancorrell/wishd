"""Ingest + correlation.

The correlator is the reason this project exists. Airflow, dbt and Spark each
emit perfectly good telemetry and none of them know about each other; the
`parent` run facet is the only thread connecting them, and stitching that thread
reliably -- across out-of-order delivery, missing parents, and producers that
omit the `root` field -- is the whole job of this module.

Everything here is idempotent. Events are replayed on retry, arrive twice from
buffered clients, and arrive backwards; ingesting the same event ten times must
leave the same rows behind as ingesting it once.
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import psycopg

from . import events as ev
from .events import RunEvent

# Guard against a malformed parent chain looping forever. No real pipeline is
# 64 levels deep; anything claiming to be is a cycle.
MAX_TREE_DEPTH = 64


class IngestResult(dict):
    """Thin dict subclass so the API can return it directly."""


def ingest_run_event(
    conn: psycopg.Connection, event: RunEvent, *, archive: bool = True
) -> IngestResult:
    """Persist one OpenLineage RunEvent and stitch it into the run tree.

    `archive=False` is for replay, which reads from the archive and must not
    write back into it -- otherwise every rebuild doubles the source of truth.
    """
    run_id = event.run.runId
    run_facets = event.run.facets or {}
    job_facets = event.job.facets or {}

    if archive:
        _archive(conn, event)

    job_id = _upsert_job(conn, event.job.namespace, event.job.name, job_facets)

    # --- resolve the parent chain -------------------------------------------
    parent = ev.parent_facet(run_facets)
    parent_run_id: UUID | None = None
    root_run_id: UUID | None = None
    root_hint: UUID | None = None

    if parent:
        parent_run_id = UUID(str(parent["run"]["runId"]))
        if parent_run_id == run_id:
            # A run cannot parent itself. Seen in the wild from misconfigured
            # wrappers that reuse one run id for the whole invocation.
            parent_run_id = None
        else:
            _ensure_placeholder(
                conn,
                run_id=parent_run_id,
                namespace=parent["job"]["namespace"],
                name=parent["job"]["name"],
            )

        # The `root` facet is a HINT, not the answer.
        #
        # Verified against openlineage-dbt 1.52.0 on 2026-08-07: the dbt
        # integration sets `root` to its own *parent* (the Airflow task), because
        # dbt cannot see the DAG above it. Trusting it verbatim rooted 19 dbt
        # runs at the task, so `tree <dbt-run-id>` returned a subtree missing the
        # DAG and its sibling tasks -- exactly the correlation failure this
        # project exists to prevent.
        #
        # We resolve the chain ourselves instead: we can see every producer's
        # events, so our view of the tree is strictly better than any single
        # producer's. The facet is still used to pre-create the ancestor (so a
        # deep run resolves before intermediate levels arrive) and as a fallback
        # when the parent is genuinely unknown.
        root = parent.get("root") or {}
        root_run = root.get("run") or {}
        root_job = root.get("job") or {}
        if root_run.get("runId") and root_job.get("namespace") and root_job.get("name"):
            hinted = UUID(str(root_run["runId"]))
            if hinted != run_id:
                _ensure_placeholder(
                    conn,
                    run_id=hinted,
                    namespace=root_job["namespace"],
                    name=root_job["name"],
                )
                root_hint = hinted

    depth = 0
    if parent_run_id is not None:
        row = conn.execute(
            "select root_run_id, depth from runs where run_id = %s", (parent_run_id,)
        ).fetchone()
        if row:
            # Authoritative: the parent's resolved root beats any hint, because
            # the parent's row already reflects everything we know about the
            # chain above it.
            depth = (row["depth"] or 0) + 1
            root_run_id = row["root_run_id"] or parent_run_id
        else:
            # Parent genuinely unknown (not even a placeholder). Fall back to the
            # hint, then to the parent itself. Either way _repair_subtree
            # corrects this row once the real ancestor arrives.
            root_run_id = root_hint or parent_run_id
            depth = 1

    if root_run_id is None:
        root_run_id = run_id  # self-rooted: this run is the top of its tree

    # --- state --------------------------------------------------------------
    new_state = ev.EVENT_TYPE_TO_STATE.get(event.eventType or "OTHER")
    is_start = event.eventType == "START"
    is_terminal = new_state in ev.TERMINAL_STATES

    nominal_start, nominal_end = ev.nominal_times(run_facets)
    error_message, error_stack = ev.error_details(run_facets)

    before = conn.execute(
        "select root_run_id, depth from runs where run_id = %s", (run_id,)
    ).fetchone()

    conn.execute(
        _UPSERT_RUN,
        {
            "run_id": run_id,
            "job_id": job_id,
            "parent_run_id": parent_run_id,
            "root_run_id": root_run_id,
            "depth": depth,
            "new_state": new_state,
            "started_at": event.eventTime if is_start else None,
            "ended_at": event.eventTime if is_terminal else None,
            "nominal_start": nominal_start,
            "nominal_end": nominal_end,
            "error_message": error_message,
            "error_stack": error_stack,
            "producer": event.producer,
            "facets": json.dumps(run_facets),
            "event_time": event.eventTime,
        },
    )

    # If this run's position in the tree moved, every descendant's cached root
    # and depth is now stale. This is the out-of-order repair path.
    moved = before is not None and (
        before["root_run_id"] != root_run_id or (before["depth"] or 0) != depth
    )
    repaired = _repair_subtree(conn, run_id) if moved or before is None else 0

    datasets = _upsert_datasets(conn, run_id, event)

    return IngestResult(
        run_id=str(run_id),
        job=f"{event.job.namespace}/{event.job.name}",
        state=new_state,
        parent_run_id=str(parent_run_id) if parent_run_id else None,
        root_run_id=str(root_run_id),
        depth=depth,
        datasets=datasets,
        descendants_repaired=repaired,
    )


# ------------------------------------------------------------------- internals


def _archive(conn: psycopg.Connection, event: RunEvent) -> None:
    conn.execute(
        """
        insert into events (event_time, event_kind, event_type, run_id,
                            job_namespace, job_name, producer, payload)
        values (%s, 'RUN', %s, %s, %s, %s, %s, %s)
        """,
        (
            event.eventTime,
            event.eventType,
            event.run.runId,
            event.job.namespace,
            event.job.name,
            event.producer,
            json.dumps(event.model_dump(mode="json", exclude_none=True)),
        ),
    )


def _upsert_job(
    conn: psycopg.Connection, namespace: str, name: str, facets: dict[str, Any]
) -> int:
    integration, jtype, ptype = ev.job_type(facets)
    row = conn.execute(
        """
        insert into jobs (namespace, name, integration, job_type, processing_type,
                          description, facets)
        values (%(namespace)s, %(name)s, %(integration)s, %(job_type)s,
                %(processing_type)s, %(description)s, %(facets)s)
        on conflict (namespace, name) do update set
            -- coalesce(new, old): a producer that omits a field must not erase
            -- what a richer producer already told us about the same job.
            integration     = coalesce(excluded.integration, jobs.integration),
            job_type        = coalesce(excluded.job_type, jobs.job_type),
            processing_type = coalesce(excluded.processing_type, jobs.processing_type),
            description     = coalesce(excluded.description, jobs.description),
            facets          = jobs.facets || excluded.facets,
            updated_at      = now()
        returning id
        """,
        {
            "namespace": namespace,
            "name": name,
            "integration": integration,
            "job_type": jtype,
            "processing_type": ptype,
            "description": ev.documentation(facets),
            "facets": json.dumps(facets),
        },
    ).fetchone()
    return row["id"]


def _ensure_placeholder(
    conn: psycopg.Connection, run_id: UUID, namespace: str, name: str
) -> None:
    """Create a stub row for a parent we have heard about but not heard from.

    The parent facet carries the parent's job namespace and name, so a stub is
    genuinely useful rather than an empty shell: the run tree renders correctly
    even when the parent's own START event is still in flight or was dropped.
    """
    job_id = _upsert_job(conn, namespace, name, {})
    conn.execute(
        """
        insert into runs (run_id, job_id, root_run_id, is_placeholder)
        values (%s, %s, %s, true)
        on conflict (run_id) do nothing
        """,
        (run_id, job_id, run_id),
    )


_UPSERT_RUN = """
insert into runs (run_id, job_id, parent_run_id, root_run_id, depth, state,
                  started_at, ended_at, nominal_start_time, nominal_end_time,
                  error_message, error_stacktrace, producer, facets,
                  is_placeholder, event_count, first_event_at, last_event_at)
values (%(run_id)s, %(job_id)s, %(parent_run_id)s, %(root_run_id)s, %(depth)s,
        coalesce(%(new_state)s, 'UNKNOWN'),
        %(started_at)s, %(ended_at)s, %(nominal_start)s, %(nominal_end)s,
        %(error_message)s, %(error_stack)s, %(producer)s, %(facets)s,
        false, 1, %(event_time)s, %(event_time)s)
on conflict (run_id) do update set
    job_id        = excluded.job_id,
    parent_run_id = coalesce(excluded.parent_run_id, runs.parent_run_id),
    root_run_id   = excluded.root_run_id,
    depth         = excluded.depth,
    -- State only moves forward. A duplicate START delivered after COMPLETE
    -- must not resurrect the run.
    state = case
        when %(new_state)s is null then runs.state
        when (case runs.state when 'UNKNOWN' then 0 when 'RUNNING' then 1 else 2 end)
             <= (case %(new_state)s when 'UNKNOWN' then 0 when 'RUNNING' then 1 else 2 end)
        then %(new_state)s
        else runs.state
    end,
    -- Timestamps take the outermost bound seen, so replays and duplicates are
    -- harmless and a late START cannot shrink a known-longer run.
    started_at         = least(runs.started_at, excluded.started_at),
    ended_at           = greatest(runs.ended_at, excluded.ended_at),
    nominal_start_time = coalesce(excluded.nominal_start_time, runs.nominal_start_time),
    nominal_end_time   = coalesce(excluded.nominal_end_time, runs.nominal_end_time),
    error_message      = coalesce(excluded.error_message, runs.error_message),
    error_stacktrace   = coalesce(excluded.error_stacktrace, runs.error_stacktrace),
    producer           = coalesce(nullif(excluded.producer, ''), runs.producer),
    -- Shallow jsonb merge: OpenLineage events are accumulative, each one
    -- potentially carrying facets the previous ones did not.
    facets             = runs.facets || excluded.facets,
    is_placeholder     = false,
    event_count        = runs.event_count + 1,
    first_event_at     = least(runs.first_event_at, excluded.first_event_at),
    last_event_at      = greatest(runs.last_event_at, excluded.last_event_at),
    updated_at         = now()
"""


_REPAIR_SUBTREE = """
with recursive tree as (
    select run_id, root_run_id, depth
    from runs
    where run_id = %(run_id)s
  union all
    select c.run_id, t.root_run_id, t.depth + 1
    from runs c
    join tree t on c.parent_run_id = t.run_id
    where t.depth < %(max_depth)s
      and c.run_id <> t.run_id
)
update runs r
set root_run_id = t.root_run_id,
    depth       = t.depth,
    updated_at  = now()
from tree t
where r.run_id = t.run_id
  and r.run_id <> %(run_id)s
  and (r.root_run_id is distinct from t.root_run_id or r.depth is distinct from t.depth)
"""


def _repair_subtree(conn: psycopg.Connection, run_id: UUID) -> int:
    """Push corrected root/depth down to descendants ingested before this run.

    This is what makes arrival order irrelevant: a Spark task event that landed
    before its dbt model, which landed before its Airflow task, ends up in
    exactly the same tree as if they had arrived in order.
    """
    cur = conn.execute(_REPAIR_SUBTREE, {"run_id": run_id, "max_depth": MAX_TREE_DEPTH})
    return cur.rowcount or 0


def _upsert_datasets(conn: psycopg.Connection, run_id: UUID, event: RunEvent) -> dict[str, int]:
    counts = {"inputs": 0, "outputs": 0}
    for direction, items in (("INPUT", event.inputs), ("OUTPUT", event.outputs)):
        for ds in items:
            dataset_id = conn.execute(
                """
                insert into datasets (namespace, name, facets)
                values (%s, %s, %s)
                on conflict (namespace, name) do update set
                    facets     = datasets.facets || excluded.facets,
                    updated_at = now()
                returning id
                """,
                (ds.namespace, ds.name, json.dumps(ds.facets or {})),
            ).fetchone()["id"]

            if direction == "OUTPUT":
                io_facets = ds.outputFacets or {}
                rows, size = ev.output_statistics(io_facets)
            else:
                io_facets = ds.inputFacets or {}
                rows, size = ev.input_statistics(io_facets)

            # Snapshot the schema onto the write edge, not just onto the dataset.
            #
            # `datasets.facets` is a running `||` merge, so it always holds the
            # CURRENT schema and can never answer "what were the columns when this
            # run wrote it" -- which is the only question schema-drift detection
            # asks. Without the per-write copy, comparing two historical writes
            # compares today's schema against itself and no drift is ever visible.
            #
            # Cheap in practice: a SchemaDatasetFacet is a short field list, and it
            # only changes when the table does. `dataspine replay` rebuilds these
            # edges from the event archive, so this gives schema history
            # retroactively rather than only from today forward.
            edge_facets = dict(io_facets)
            schema = (ds.facets or {}).get("schema")
            if schema is not None:
                edge_facets["schema"] = schema

            conn.execute(
                """
                insert into run_datasets (run_id, dataset_id, direction, facets,
                                          row_count, size_bytes)
                values (%s, %s, %s, %s, %s, %s)
                on conflict (run_id, dataset_id, direction) do update set
                    facets     = run_datasets.facets || excluded.facets,
                    row_count  = coalesce(excluded.row_count, run_datasets.row_count),
                    size_bytes = coalesce(excluded.size_bytes, run_datasets.size_bytes),
                    updated_at = now()
                """,
                (run_id, dataset_id, direction, json.dumps(edge_facets), rows, size),
            )
            counts["inputs" if direction == "INPUT" else "outputs"] += 1
    return counts
