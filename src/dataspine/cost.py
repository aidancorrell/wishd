"""What a pipeline cost, and which model to blame.

The roadmap's target output:

    "fct_orders cost $34 to build last night, up 4x from last week, because a
     broadcast join spilled across 180 tasks."

Three divisions get there, and every one is a chance to be quietly wrong:

  **Bill -> cluster.** AWS Cost and Usage Report rows carry `resourceTags/...`,
  never cluster ids, so the tags captured by the lifecycle sync are the join key.
  That is why D4 had to land first.

  **Cluster -> application**, by core-seconds. Those come free from the Spark
  event log (ADR-004, third payoff). Splitting a cluster's cost evenly across
  concurrent applications would charge a five-minute job the same as an
  eight-hour one, which is exactly the question this exists to answer.

  **Application -> model**, through the run tree. Nothing in an event log or an
  AWS bill knows what a dbt model is; the correlator does, and has since
  Phase 00.

Two rules the arithmetic follows, both about refusing to invent numbers:

  **Idle time is nobody's cost.** A cluster that sat empty for six hours has real
  spend that no model caused. Spreading it silently would make every model's cost
  depend on how idle the cluster happened to be -- a number that moves for
  reasons no dbt author can act on. It is reported as idle, separately.

  **Unpriced is not free.** An application with no cost data gets no row rather
  than a zero. On a dashboard "$0" and "we do not know" look identical and mean
  opposite things.
"""

from __future__ import annotations

import csv
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg

log = logging.getLogger("dataspine.cost")

# CUR column names. Several are aliased because the column set differs between
# the legacy CUR and CUR 2.0.
#
# **The CUR 2.0 names below are verified against a real account** (2026-08-11,
# `bcm-data-exports GetTable`), not against documentation. The legacy family is
# still documentation-only -- see D12.
#
# One verified gotcha: `line_item_resource_id` exists only if the export sets
# `INCLUDE_RESOURCES=TRUE`, which is *not* the default. A default export drops
# to 115 columns and every resource id we read is null.
COLUMNS = {
    "line_item_id": ("identity/LineItemId", "identity_line_item_id"),
    "service": ("lineItem/ProductCode", "line_item_product_code"),
    "resource": ("lineItem/ResourceId", "line_item_resource_id"),
    "start": ("lineItem/UsageStartDate", "line_item_usage_start_date"),
    "end": ("lineItem/UsageEndDate", "line_item_usage_end_date"),
    "cost": ("lineItem/UnblendedCost", "line_item_unblended_cost"),
}

TAG_PREFIXES = ("resourceTags/user:", "resource_tags_user_")


def _pick(row: dict[str, Any], key: str) -> Any:
    for candidate in COLUMNS[key]:
        if candidate in row:
            return row[candidate]
    return None


def _tags(row: dict[str, Any]) -> dict[str, str]:
    """Pull cost-allocation tags out of a CUR row.

    **This reads nothing from a default CUR 2.0 export, and that is a real
    hazard rather than a theoretical one.** Legacy CUR flattened tags into one
    column per key (`resourceTags/user:cluster`). CUR 2.0 nests them into a
    single `resource_tags` column of type `Map`, so the prefix scan below finds
    no match, `import_cur` attributes every row to no cluster, and the totals
    still reconcile perfectly against the AWS console. Verified against a real
    account on 2026-08-11.

    The flat columns come back only if the export's own SQL asks for them:

        SELECT …, resource_tags.user_cluster AS resource_tags_user_cluster
        FROM COST_AND_USAGE_REPORT

    AWS accepts that alias for a tag key that has never existed, so a typo is
    another route to the same silent nothing. Two further conditions, both from
    AWS's documentation and both invisible from here: the key must be activated
    as a cost allocation tag in the Billing console, and activation is not
    retroactive.

    Not yet handled: reading the nested `resource_tags` map directly, for
    someone who points this at an unaliased export. Deliberately not guessed --
    how a `Map` renders in a delivered CSV is a question a delivered file
    answers, and one is on its way. See D12.
    """
    tags = {}
    for column, value in row.items():
        for prefix in TAG_PREFIXES:
            if column.startswith(prefix) and value:
                tags[column[len(prefix):]] = str(value)
    return tags


# ------------------------------------------------------------- CUR ingestion


def import_cur(
    conn: psycopg.Connection, rows: list[dict[str, Any]], *, tag_key: str = "cluster"
) -> int:
    """Store CUR line items, attaching each to a cluster by tag. Returns rows kept.

    Idempotent on **(line item id, usage start)**, because reports are restated
    through the month and the same hour arrives repeatedly -- a naive insert
    would multiply the bill by however many times it was imported.

    **The hour is part of the key, and a real bill is what proved it.** CUR's
    `identity/LineItemId` names a resource + usage type, not a row: it repeats
    once per hour, so keying on it alone kept one hour per resource and dropped
    the rest. The first real report imported here was 139 rows carrying 27
    distinct ids. See migration 019 and D12.

    A row matching no cluster is kept with a null cluster, not dropped. Spend we
    cannot attribute is still spend, and totals that disagree with the AWS
    console are worse than useless.
    """
    written = 0
    for row in rows:
        line_item_id = _pick(row, "line_item_id")
        start = _as_datetime(_pick(row, "start"))
        end = _as_datetime(_pick(row, "end"))
        try:
            amount = float(_pick(row, "cost") or 0)
        except (TypeError, ValueError):
            # One unparseable line must not lose a month of billing.
            log.warning("skipping CUR row with unreadable cost: %s", line_item_id)
            continue
        if not line_item_id or start is None:
            log.warning("skipping CUR row with no id or start date")
            continue

        tags = _tags(row)
        conn.execute(
            """
            insert into cost_line_items
                (line_item_id, cluster_id, service, resource_id,
                 period_start, period_end, cost_usd, tags)
            values (
                %(line_item_id)s,
                (select cluster_id from clusters
                  where tags ->> %(tag_key)s is not null
                    and tags ->> %(tag_key)s = %(tag_value)s
                  order by started_at desc nulls last
                  limit 1),
                %(service)s, %(resource)s, %(start)s, %(end)s, %(cost)s, %(tags)s
            )
            on conflict (line_item_id, period_start) do update set
                cluster_id  = excluded.cluster_id,
                cost_usd    = excluded.cost_usd,
                period_end  = excluded.period_end,
                tags        = excluded.tags,
                imported_at = now()
            """,
            {
                "line_item_id": str(line_item_id),
                "tag_key": tag_key,
                "tag_value": tags.get(tag_key),
                "service": _pick(row, "service"),
                "resource": _pick(row, "resource"),
                "start": start,
                "end": end or start,
                "cost": amount,
                "tags": json.dumps(tags),
            },
        )
        written += 1
    return written


# Per-platform usage row readers.
#
# Normalising here rather than teaching attribution about DBUs and GCP SKUs is
# the seam that matters: the three divisions never learn there is more than one
# cloud, so a Databricks bill attributes through exactly the same code path an
# EMR bill does.
#
# **Validation status:** both mappings are built from the documented schemas
# (`system.billing.usage`, GCP's BigQuery billing export) and have **never been
# run against a real account** — the same category as the EMR mapper (D4/D5) and
# the warehouse pollers (D9).
def _databricks_amount(row: dict[str, Any]) -> float | None:
    """`system.billing.usage` reports quantity and price separately."""
    try:
        return float(row.get("usage_quantity") or 0) * float(row.get("list_price") or 0)
    except (TypeError, ValueError):
        return None


def _dataproc_amount(row: dict[str, Any]) -> float | None:
    try:
        return float(row.get("cost") or 0)
    except (TypeError, ValueError):
        return None


def _databricks_tags(row: dict[str, Any]) -> dict[str, str]:
    tags = row.get("custom_tags") or row.get("tags") or {}
    return {str(k): str(v) for k, v in tags.items()} if isinstance(tags, dict) else {}


def _dataproc_tags(row: dict[str, Any]) -> dict[str, str]:
    """GCP exports labels as an array of {key, value}, not a mapping."""
    labels = row.get("labels") or []
    if isinstance(labels, dict):
        return {str(k): str(v) for k, v in labels.items()}
    return {
        str(entry.get("key")): str(entry.get("value"))
        for entry in labels
        if isinstance(entry, dict) and entry.get("key")
    }


PLATFORMS = {
    "databricks": (_databricks_amount, _databricks_tags),
    "dataproc": (_dataproc_amount, _dataproc_tags),
}


def import_usage(
    conn: psycopg.Connection,
    *,
    platform: str,
    rows: list[dict[str, Any]],
    tag_key: str = "cluster",
) -> int:
    """Import usage rows from a non-AWS billing source into `cost_line_items`.

    Converts each platform's shape into the CUR-shaped row `import_cur` already
    understands, so there is one storage format and one attribution path.
    """
    if platform not in PLATFORMS:
        raise ValueError(f"unknown platform {platform!r}; expected one of {list(PLATFORMS)}")
    amount_of, tags_of = PLATFORMS[platform]

    translated = []
    for row in rows:
        amount = amount_of(row)
        if amount is None:
            log.warning("skipping %s usage row with unreadable cost", platform)
            continue
        line = {
            "identity/LineItemId": str(row.get("record_id") or row.get("id") or ""),
            "lineItem/ProductCode": platform,
            "lineItem/UsageStartDate": row.get("usage_start_time") or row.get("start_time"),
            "lineItem/UsageEndDate": row.get("usage_end_time") or row.get("end_time"),
            "lineItem/UnblendedCost": str(amount),
            "lineItem/ResourceId": row.get("cluster_id") or row.get("resource_id") or "",
        }
        for key, value in tags_of(row).items():
            line[f"resourceTags/user:{key}"] = value
        translated.append(line)

    return import_cur(conn, translated, tag_key=tag_key)


def import_cur_file(
    conn: psycopg.Connection, path: str | Path, *, tag_key: str = "cluster"
) -> int:
    """Import a CUR CSV. Gzipped files are handled transparently."""
    path = Path(path)
    opener = open
    if path.suffix == ".gz":
        import gzip

        opener = gzip.open  # type: ignore[assignment]

    with opener(path, "rt", newline="") as handle:  # type: ignore[operator]
        return import_cur(conn, list(csv.DictReader(handle)), tag_key=tag_key)


# -------------------------------------------------------------- attribution


def attribute(conn: psycopg.Connection) -> int:
    """Divide cluster cost across the applications that ran. Returns rows written.

    Each billing period's cost is split among the applications overlapping it,
    weighted by the core-seconds each held *during that period*. An application
    spanning two hours draws from both, in proportion.

    Whatever is left over is idle time and is deliberately not assigned; see
    `cluster_summary`.
    """
    conn.execute("delete from application_costs")
    conn.execute(
        """
        -- `spans`, not `overlaps`: OVERLAPS is a reserved word in SQL (the
        -- period-comparison operator) and cannot name a CTE.
        with spans as (
            select li.line_item_id,
                   li.period_start,
                   li.cluster_id,
                   li.cost_usd,
                   s.app_id,
                   -- Core-seconds this application held inside this billing
                   -- period, prorated by the overlap. A run that started at
                   -- half past owns half the hour's worth of its own compute.
                   (s.metrics ->> 'core_seconds')::numeric
                     * extract(epoch from (
                           least(li.period_end, s.ended_at)
                         - greatest(li.period_start, s.started_at)))
                     / nullif(extract(epoch from (s.ended_at - s.started_at)), 0)
                   as weight
            from cost_line_items li
            join spark_apps s
              on s.cluster_id = li.cluster_id
             and s.started_at < li.period_end
             and s.ended_at   > li.period_start
            where li.cluster_id is not null
              and s.ended_at is not null
              and s.started_at is not null
              and coalesce((s.metrics ->> 'core_seconds')::numeric, 0) > 0
        ),
        shares as (
            -- The window has to be its own level: a window function cannot be
            -- nested inside an aggregate in the same select.
            select app_id, cluster_id, weight,
                   -- Partitioned by the *row*, which is (id, hour) and not id
                   -- alone: one id covers every hour of a resource's usage, so
                   -- partitioning by it would pool a month of hours and divide
                   -- one hour's cost by a month of weights. See migration 019.
                   cost_usd * weight
                       / sum(weight) over (partition by line_item_id, period_start)
                       as cost_share
            from spans
            where weight > 0
        )
        insert into application_costs (app_id, cluster_id, cost_usd, core_seconds)
        select app_id, cluster_id, sum(cost_share), sum(weight)
        from shares
        group by app_id, cluster_id
        on conflict (app_id) do update set
            cost_usd = excluded.cost_usd,
            core_seconds = excluded.core_seconds,
            computed_at = now()
        """
    )
    return conn.execute("select count(*) as n from application_costs").fetchone()["n"]


# ------------------------------------------------------------------ read side


def for_application(conn: psycopg.Connection, app_id: str) -> dict[str, Any] | None:
    return conn.execute(
        "select * from application_costs where app_id = %s", (app_id,)
    ).fetchone()


def for_run(conn: psycopg.Connection, run_id: UUID | str) -> dict[str, Any] | None:
    """Cost of a run and everything beneath it in the tree.

    Descends the tree because the dbt model run is what a human names, while the
    Spark applications that actually cost money hang below it. Returns None when
    nothing under the run has a price -- unpriced is not free.
    """
    from .ingest import MAX_TREE_DEPTH

    row = conn.execute(
        f"""
        with recursive tree as (
            select run_id, 0 as level from runs where run_id = %(run_id)s
          union all
            select c.run_id, t.level + 1
            from runs c join tree t on c.parent_run_id = t.run_id
            where t.level < {MAX_TREE_DEPTH}
        )
        select sum(ac.cost_usd) as cost_usd,
               sum(ac.core_seconds) as core_seconds,
               count(*) as applications
        from tree t
        join spark_apps s on s.run_id = t.run_id
        join application_costs ac on ac.app_id = s.app_id
        """,
        {"run_id": str(run_id)},
    ).fetchone()
    return row if row and row["cost_usd"] is not None else None


def by_job(conn: psycopg.Connection, *, limit: int = 100) -> list[dict[str, Any]]:
    """Cost per job, heaviest first. This is "cost per model".

    The third division, and the one the real captures forced. **The Spark
    application sits beside the dbt models, not beneath them.** In the captured
    tree the application is parented to the Airflow *task*, with the models as
    its siblings -- one long-lived application (a thrift session) serves many
    models. Rolling an application's cost up to its parent job therefore lands
    the entire bill on the Airflow task and never reaches a model, which makes
    "cost per model" -- the actual target -- unanswerable.

    So the application's cost is split across the dbt model runs in the same run
    tree, weighted by how long each model's own Spark work took. That is the
    roadmap's "dbt models via query attribution", and it needs the run tree the
    correlator built in Phase 00: nothing in an event log or an AWS bill knows
    what a dbt model is.

    An application with no dbt models in its tree keeps its cost where it is --
    a bare `spark-submit` is real work, and its spend must not vanish just
    because there is nothing to divide it among.
    """
    return conn.execute(
        """
        with priced as (
            select ac.app_id, ac.cost_usd,
                   coalesce(r.root_run_id, r.run_id) as root_run_id,
                   r.run_id as app_run_id
            from application_costs ac
            join spark_apps s on s.app_id = ac.app_id
            join runs r       on r.run_id = s.run_id
        ),
        -- dbt model runs sharing a run tree with a priced application, and the
        -- duration of the Spark work each one drove.
        model_work as (
            select p.app_id, p.cost_usd, m.run_id as model_run_id, m.job_id,
                   greatest(
                       coalesce(sum(extract(epoch from (sql.ended_at - sql.started_at))), 0),
                       coalesce(extract(epoch from (m.ended_at - m.started_at)), 0)
                   ) as weight
            from priced p
            join runs m on coalesce(m.root_run_id, m.run_id) = p.root_run_id
            join jobs mj on mj.id = m.job_id
            left join runs sql on sql.parent_run_id = m.run_id
            where mj.integration = 'DBT' and mj.job_type = 'MODEL'
            group by p.app_id, p.cost_usd, m.run_id, m.job_id, m.ended_at, m.started_at
        ),
        model_share as (
            select app_id, model_run_id, job_id,
                   cost_usd * weight / sum(weight) over (partition by app_id) as cost_usd
            from model_work
            where weight > 0
        ),
        -- Applications with no models to split across keep their own cost,
        -- reported against the job that ran them.
        unsplit as (
            select p.app_id, p.app_run_id as model_run_id, r.job_id, p.cost_usd
            from priced p
            join runs r on r.run_id = coalesce(
                (select parent_run_id from runs where run_id = p.app_run_id), p.app_run_id
            )
            where not exists (select 1 from model_share ms where ms.app_id = p.app_id)
        ),
        attributed as (
            select * from model_share
            union all
            select * from unsplit
        )
        select j.name as job_name, j.integration,
               sum(a.cost_usd) as cost_usd,
               count(distinct a.model_run_id) as runs,
               max(r.started_at) as last_run_at
        from attributed a
        join jobs j on j.id = a.job_id
        join runs r on r.run_id = a.model_run_id
        group by j.name, j.integration
        order by sum(a.cost_usd) desc
        limit %s
        """,
        (limit,),
    ).fetchall()


def cluster_summary(conn: psycopg.Connection, cluster_id: str) -> dict[str, Any]:
    """Billed, attributed and idle cost for one cluster.

    Idle is reported rather than distributed. A cluster left running overnight
    costs real money that no model caused, and burying it in the models' numbers
    would hide the single cheapest optimisation most teams have available.
    """
    return conn.execute(
        """
        select
          (select coalesce(sum(cost_usd), 0) from cost_line_items
            where cluster_id = %(cluster_id)s)                       as billed_cost_usd,
          (select coalesce(sum(cost_usd), 0) from application_costs
            where cluster_id = %(cluster_id)s)                       as attributed_cost_usd,
          (select coalesce(sum(cost_usd), 0) from cost_line_items
            where cluster_id = %(cluster_id)s)
          - (select coalesce(sum(cost_usd), 0) from application_costs
              where cluster_id = %(cluster_id)s)                     as idle_cost_usd,
          (select count(*) from spark_apps where cluster_id = %(cluster_id)s)
                                                                     as applications
        """,
        {"cluster_id": cluster_id},
    ).fetchone()


def unattributed(conn: psycopg.Connection) -> dict[str, Any]:
    """Spend matching no cluster at all — the coverage gap, stated plainly."""
    return conn.execute(
        """
        select coalesce(sum(cost_usd), 0) as cost_usd, count(*) as line_items
        from cost_line_items where cluster_id is null
        """
    ).fetchone()


def _as_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, str) and value:
        for text in (value, value.replace(" ", "T")):
            try:
                parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError:
                continue
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return None
