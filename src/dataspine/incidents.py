"""Grouping breaches into incidents, and suppressing the consequences.

Everything before this produced signals. This decides which of them are the same
event, which one caused it, and which are merely downstream of it.

The stake, in the roadmap's own words: **alert fatigue is what kills these tools
in month three.** One late source table breaches the freshness monitor on every
one of the fifty tables built from it. Fifty pages get the channel muted, and the
fifty-first — a genuinely different problem — is then missed too.

So: breaches that lineage connects and that land close together in time become
one incident. The furthest-upstream breach is the cause; the rest are its blast
radius. Only the cause is delivered. The consequences are still recorded, because
"what else is waiting on this" is the second question everyone asks.

Two failure modes are worse than the alert fatigue, and the code leans away from
both:

  **Merging unrelated problems.** Two incidents reported as one makes the second
  invisible behind the first one's story. Grouping requires an actual lineage
  path *and* temporal proximity, never one alone.

  **Swallowing an alert we cannot reason about.** A fresh install has no graph.
  Suppression passes everything through unless it can point at the specific
  upstream breach that explains a downstream one.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg

from . import lineage

log = logging.getLogger("dataspine.incidents")

# How close in time two breaches must be to be candidates for one incident.
#
# Lineage proximity alone would merge every breach a table ever had into a single
# immortal incident -- yesterday's outage on `stg_orders` is not today's. Six
# hours comfortably covers a nightly pipeline's spread from first task to last.
DEFAULT_WINDOW_HOURS = 6

# How far downstream a cause is allowed to explain a breach. Beyond this, the
# claim "this is why that broke" is doing more asserting than the graph supports.
MAX_BLAST_DEPTH = 10


def detect(
    conn: psycopg.Connection,
    *,
    now: datetime | None = None,
    window_hours: float = DEFAULT_WINDOW_HOURS,
    persist: bool = False,
) -> list[dict[str, Any]]:
    """Group current breaches into incidents. Returns causes with consequences.

    `persist=False` computes without writing, which is what suppression needs on
    the alerting path; `persist=True` records them so the UI and the API can show
    an incident timeline.
    """
    now = now or datetime.now(UTC)
    breaching = _breaching_monitors(conn)
    if not breaching:
        if persist:
            _resolve_stale(conn, open_causes=set(), now=now)
        return []

    by_entity = {m["entity_id"]: m for m in breaching if m["entity_id"]}

    # A breach is a consequence when some *other* current breach sits upstream of
    # it in the graph. Whatever is left has nothing broken above it, and is a
    # cause.
    window = timedelta(hours=window_hours)
    explained: dict[int, tuple[dict[str, Any], int]] = {}
    for monitor in breaching:
        if not monitor["entity_id"]:
            continue
        # Every breaching ancestor, not just the nearest. The cause is the one
        # with nothing broken above it, so attribution has to reach past the
        # intermediate breaches -- which are themselves consequences.
        for ancestor in lineage.upstream(
            conn, monitor["entity_id"], depth=MAX_BLAST_DEPTH
        ):
            candidate = by_entity.get(ancestor["id"])
            if not candidate or candidate["monitor_id"] == monitor["monitor_id"]:
                continue
            # Lineage proximity is necessary and not sufficient. Without the time
            # check, a breach from three days ago keeps explaining today's on the
            # same table forever -- one immortal incident that absorbs every
            # future problem downstream of it.
            if abs(candidate["evaluated_at"] - monitor["evaluated_at"]) > window:
                continue
            current = explained.get(monitor["monitor_id"])
            if current is None or ancestor["distance"] > current[1]:
                explained[monitor["monitor_id"]] = (candidate, ancestor["distance"])

    causes = [m for m in breaching if m["monitor_id"] not in explained]

    incidents = []
    for cause in causes:
        consequences = [
            {
                "monitor_id": monitor_id,
                "monitor": by_id["monitor"],
                "entity_id": by_id["entity_id"],
                "entity_name": by_id["entity_name"],
                "distance": distance,
            }
            for monitor_id, (explainer, distance) in explained.items()
            if explainer["monitor_id"] == cause["monitor_id"]
            for by_id in [_by_monitor_id(breaching, monitor_id)]
        ]
        consequences.sort(key=lambda c: (c["distance"], c["monitor"]))
        incidents.append(
            {
                "id": None,
                "cause_monitor_id": cause["monitor_id"],
                "cause_monitor": cause["monitor"],
                "cause_entity_id": cause["entity_id"],
                "cause_entity_name": cause["entity_name"],
                "cause_result_id": cause["result_id"],
                "message": cause["message"],
                "opened_at": cause["evaluated_at"],
                "consequences": consequences,
            }
        )

    if persist:
        for incident in incidents:
            incident["id"] = _persist(conn, incident, now=now)
        _resolve_stale(
            conn, open_causes={i["cause_monitor_id"] for i in incidents}, now=now
        )
    return incidents


def _by_monitor_id(rows: list[dict[str, Any]], monitor_id: int) -> dict[str, Any]:
    return next(r for r in rows if r["monitor_id"] == monitor_id)


def _breaching_monitors(conn: psycopg.Connection) -> list[dict[str, Any]]:
    """Monitors currently in breach, with the entity each one watches.

    Every breaching monitor, with no time filter. The window governs *grouping*,
    not inclusion: a monitor still in breach from three days ago is still broken
    and still deserves an incident -- it simply is not the same event as
    today's.

    A dataset monitor resolves to an entity through the same leaf rule the rest
    of the system uses; a job monitor resolves through the datasets its job
    writes, because a slow job is very often the cause of the staleness beneath
    it and dropping those would lose the most useful causes in the set.
    """
    return conn.execute(
        """
        select m.id as monitor_id, m.name as monitor, m.target_kind, m.target,
               r.id as result_id, r.message, r.evaluated_at,
               e.id as entity_id, e.name as entity_name
        from monitors m
        join lateral (
            select id, message, evaluated_at
            from monitor_results
            where monitor_id = m.id
            order by evaluated_at desc, id desc
            limit 1
        ) r on true
        left join lateral (
            select de.id, de.name
            from dataset_entities de
            join dataset_identities di on di.entity_id = de.id
            join datasets d on d.id = di.dataset_id
            where (m.target_kind = 'dataset' and (
                      d.name = m.target
                      or regexp_replace(d.name, '^.*[./]', '')
                         = regexp_replace(m.target, '^.*[./]', '')))
               or (m.target_kind = 'job' and exists (
                      select 1
                      from run_datasets rd
                      join runs run on run.run_id = rd.run_id
                      join jobs j on j.id = run.job_id
                      where rd.dataset_id = d.id
                        and rd.direction = 'OUTPUT'
                        and j.name = m.target))
            limit 1
        ) e on true
        where m.enabled
          and m.last_status = 'breach'
        order by r.evaluated_at
        """
    ).fetchall()


def _persist(conn: psycopg.Connection, incident: dict[str, Any], *, now: datetime) -> int:
    """Insert or reuse the open incident for this cause.

    The partial unique index does the work: a breach persisting across sweeps
    stays one incident rather than opening a new one every hour, which would be
    the same alert fatigue moved one level up.
    """
    row = conn.execute(
        """
        insert into incidents (cause_monitor_id, cause_result_id, cause_entity_id, opened_at)
        values (%(monitor_id)s, %(result_id)s, %(entity_id)s, %(opened_at)s)
        on conflict (cause_monitor_id) where resolved_at is null
        do update set updated_at = now()
        returning id
        """,
        {
            "monitor_id": incident["cause_monitor_id"],
            "result_id": incident["cause_result_id"],
            "entity_id": incident["cause_entity_id"],
            "opened_at": incident["opened_at"] or now,
        },
    ).fetchone()
    incident_id = row["id"]

    for consequence in incident["consequences"]:
        conn.execute(
            """
            insert into incident_consequences (incident_id, monitor_id, entity_id, distance)
            values (%s, %s, %s, %s)
            on conflict (incident_id, monitor_id) do update set
                distance = excluded.distance
            """,
            (
                incident_id,
                consequence["monitor_id"],
                consequence["entity_id"],
                consequence["distance"],
            ),
        )
    return incident_id


def _resolve_stale(
    conn: psycopg.Connection, *, open_causes: set[int], now: datetime
) -> None:
    """Close incidents whose cause is no longer breaching."""
    conn.execute(
        """
        update incidents
        set resolved_at = %(now)s, updated_at = now()
        where resolved_at is null
          and not (cause_monitor_id = any(%(open)s))
        """,
        {"now": now, "open": list(open_causes)},
    )


# ------------------------------------------------------------------ suppression


def suppress(
    conn: psycopg.Connection,
    results: list[dict[str, Any]],
    *,
    now: datetime | None = None,
    window_hours: float = DEFAULT_WINDOW_HOURS,
) -> list[dict[str, Any]]:
    """Drop breaches that another breach in the same sweep explains.

    Never suppresses a recovery: "it is fixed" has to get through even for a
    downstream table whose breach was suppressed as a consequence, or a table
    silently stays marked broken in everyone's memory.

    Never suppresses anything it cannot explain. Without a lineage graph — a
    fresh install — every alert passes through, because swallowing alerts we
    cannot reason about is worse than sending too many.
    """
    now = now or datetime.now(UTC)
    breaches = [r for r in results if r.get("status") == "breach"]
    if len(breaches) < 2:
        return results

    try:
        incidents = detect(conn, now=now, window_hours=window_hours)
    except Exception as exc:  # noqa: BLE001 - alerting must survive a graph problem
        log.warning("could not group incidents, alerting ungrouped: %s", exc)
        return results

    consequence_names = {
        c["monitor"] for incident in incidents for c in incident["consequences"]
    }
    radius = {
        incident["cause_monitor"]: len(incident["consequences"]) for incident in incidents
    }

    kept = []
    for result in results:
        if result.get("status") == "breach" and result["monitor"] in consequence_names:
            continue
        downstream = radius.get(result["monitor"], 0)
        if downstream and result.get("status") == "breach":
            # A cause with its blast radius is a more useful page than a cause
            # alone: "fix this, and these are waiting on you".
            result = {
                **result,
                "message": (
                    f"{result['message']} — {downstream} downstream "
                    f"monitor{'s' if downstream != 1 else ''} also breaching"
                ),
            }
        kept.append(result)
    return kept


# ------------------------------------------------------------------- read side


def open_incidents(conn: psycopg.Connection) -> list[dict[str, Any]]:
    return conn.execute(
        """
        select i.id, i.opened_at, m.name as cause_monitor, e.name as cause_entity,
               r.message,
               coalesce(count(c.monitor_id) filter (where c.monitor_id is not null), 0)
                   as consequence_count
        from incidents i
        join monitors m on m.id = i.cause_monitor_id
        left join dataset_entities e on e.id = i.cause_entity_id
        left join monitor_results r on r.id = i.cause_result_id
        left join incident_consequences c on c.incident_id = i.id
        where i.resolved_at is null
        group by i.id, i.opened_at, m.name, e.name, r.message
        order by i.opened_at desc
        """
    ).fetchall()


def get_incident(conn: psycopg.Connection, incident_id: int) -> dict[str, Any] | None:
    incident = conn.execute(
        """
        select i.*, m.name as cause_monitor, e.name as cause_entity, r.message
        from incidents i
        join monitors m on m.id = i.cause_monitor_id
        left join dataset_entities e on e.id = i.cause_entity_id
        left join monitor_results r on r.id = i.cause_result_id
        where i.id = %s
        """,
        (incident_id,),
    ).fetchone()
    if incident is None:
        return None
    incident["consequences"] = conn.execute(
        """
        select c.distance, m.name as monitor, e.name as entity_name
        from incident_consequences c
        join monitors m on m.id = c.monitor_id
        left join dataset_entities e on e.id = c.entity_id
        where c.incident_id = %s
        order by c.distance, m.name
        """,
        (incident_id,),
    ).fetchall()
    return incident
