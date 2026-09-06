"""Re-joining run trees that a shared Spark session split apart.

D6 found the problem: on dbt-spark over a shared Thrift Server, dbt and the
Spark listener emit **disjoint run trees**. Zero shared root run ids. The
correlation this whole project is built on silently does not happen on its own
reference stack, and `unstitched_runs` cannot see it because both trees are
internally consistent.

D3's obvious fix does not work. A session-scoped `SET
spark.openlineage.parentRunId=…` was tried against a real Thrift Server: the
value lands in the session conf, and the listener ignores it. It reads that
config once when the application starts, so every query on a server that lives
for days gets the *application's* run as its parent.

The fix that does work needs nothing from either producer, because dbt is
already telling us. **dbt's `query_comment` is enabled by default** and prefixes
every statement it sends with its own metadata:

    /* {"app": "dbt", "dbt_version": "1.12.0", "profile_name": "analytics_spark",
        "target_name": "thrift", "node_id": "model.analytics.stg_customers"} */
    drop table if exists analytics_marts_marts.stg_customers

The Spark listener captures the SQL text verbatim, comment included. So the dbt
job name is *inside* the SQL that Spark reports, in dbt's own vocabulary, and it
matches a job name we already store exactly.

That is worth distinguishing from the fuzzy merge Phase 04 deliberately refused.
This is not "these two things have similar names and happened around the same
time". It is dbt stating which node issued the statement. The time window is a
guard against attaching to the wrong *invocation* of that node, not the evidence
itself.

Runs post-hoc rather than at ingest, for the same reason identity resolution
does: arrival order is not ours to control, and the dbt run routinely lands after
the Spark query that it explains.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

import psycopg

from .ingest import MAX_TREE_DEPTH, _repair_subtree

log = logging.getLogger("dataspine.stitch")

# dbt's query comment is a JSON object in a leading `/* … */`. Matched
# structurally rather than by regexing out the node id, so a comment that merely
# contains the words cannot be mistaken for dbt's.
_COMMENT = re.compile(r"/\*\s*(\{.*?\})\s*\*/", re.DOTALL)

# How far apart a Spark query and its dbt node run may be and still be the same
# invocation. Generous, because a dbt model's run spans all its statements and
# clock skew between containers is real; bounded, because the same model runs
# nightly and attaching to last Tuesday's would put cost and lineage on the
# wrong run.
WINDOW_SECONDS = 3600


def dbt_node_id(sql: str | None) -> str | None:
    """The dbt node id from a query comment, or None.

    Requires `"app": "dbt"` as well as a `node_id`. A comment that happens to
    carry a `node_id` from some other tool is not evidence about dbt, and
    trusting it would re-parent a Spark run onto a job that never issued it.
    """
    if not sql:
        return None
    for match in _COMMENT.finditer(sql):
        try:
            payload = json.loads(match.group(1))
        except (json.JSONDecodeError, ValueError):
            continue
        if not isinstance(payload, dict):
            continue
        if payload.get("app") != "dbt":
            continue
        node = payload.get("node_id")
        if isinstance(node, str) and node:
            return node
    return None


def stitch_query_comments(conn: psycopg.Connection) -> int:
    """Re-parent Spark runs onto the dbt node that issued them. Returns count.

    Only touches runs that are *not already* in the dbt tree. Where propagation
    genuinely works — a `spark-submit` given an explicit parent run id — the real
    correlation stands, and replacing it with an inferred one would be a
    downgrade.
    """
    candidates = conn.execute(
        f"""
        with recursive up as (
            select r.run_id, r.parent_run_id, r.job_id, 0 as hops
            from runs r
            join jobs j on j.id = r.job_id
            where j.integration = 'SPARK'
              and j.facets #>> '{{sql,query}}' is not null
          union all
            select u.run_id, p.parent_run_id, p.job_id, u.hops + 1
            from up u
            join runs p on p.run_id = u.parent_run_id
            where u.hops < {MAX_TREE_DEPTH}
        )
        select r.run_id,
               r.started_at,
               j.facets #>> '{{sql,query}}' as query,
               -- Whether anything above this run is already a dbt job. If so,
               -- the producers are correlated and we leave it alone.
               bool_or(aj.integration = 'DBT') as has_dbt_ancestor
        from up u
        join runs r on r.run_id = u.run_id
        join jobs j on j.id = r.job_id
        left join jobs aj on aj.id = u.job_id
        group by r.run_id, r.started_at, j.facets #>> '{{sql,query}}'
        """
    ).fetchall()

    stitched = 0
    for row in candidates:
        if row["has_dbt_ancestor"]:
            continue
        node = dbt_node_id(row["query"])
        if not node:
            continue

        # The invocation of that node whose window brackets this query. Ordered
        # by closeness so several nightly runs cannot collide.
        parent = conn.execute(
            """
            select r.run_id
            from runs r
            join jobs j on j.id = r.job_id
            where j.integration = 'DBT'
              and j.name = %(node)s
              and r.started_at is not null
              and abs(extract(epoch from (r.started_at - %(moment)s)))
                  <= %(window)s
            order by abs(extract(epoch from (r.started_at - %(moment)s)))
            limit 1
            """,
            {"node": node, "moment": row["started_at"], "window": WINDOW_SECONDS},
        ).fetchone()
        if parent is None:
            # dbt's run has not arrived, or never will. Leaving the split
            # visible beats inventing a parent.
            log.debug("no dbt run for node %s near %s", node, row["started_at"])
            continue

        conn.execute(
            """
            update runs child
            set parent_run_id = %(parent)s,
                root_run_id = coalesce(
                    (select coalesce(p.root_run_id, p.run_id)
                       from runs p where p.run_id = %(parent)s),
                    %(parent)s),
                depth = coalesce(
                    (select p.depth + 1 from runs p where p.run_id = %(parent)s), 1),
                updated_at = now()
            where child.run_id = %(child)s
            """,
            {"parent": parent["run_id"], "child": row["run_id"]},
        )
        # Descendants of the moved run carry a stale root and depth. The
        # correlator already knows how to push the correction down.
        _repair_subtree(conn, row["run_id"])
        stitched += 1

    if stitched:
        log.info("stitched %s Spark run(s) onto their dbt nodes", stitched)
    return stitched


def disjoint_trees(conn: psycopg.Connection) -> list[dict[str, Any]]:
    """Tables written by producers whose run trees never meet.

    The alarm `unstitched_runs` cannot raise. That view fires when a run names a
    parent nobody sent — a *dangling* reference. A shared Thrift Server produces
    something different and worse: two internally consistent trees that simply do
    not touch, so nothing dangles and the alarm stays at zero while the lineage
    graph is split in half.

    This looks for the symptom instead: one table name, written by runs that
    share no root. That is either a correlation gap or two genuinely different
    tables with the same name — and both are worth a human's attention.
    """
    return conn.execute(
        """
        with writes as (
            select regexp_replace(d.name, '^.*[./]', '') as leaf,
                   coalesce(r.root_run_id, r.run_id) as root,
                   j.integration
            from run_datasets rd
            join runs r     on r.run_id = rd.run_id
            join jobs j     on j.id = r.job_id
            join datasets d on d.id = rd.dataset_id
            where rd.direction = 'OUTPUT'
        )
        select leaf as name,
               count(distinct root) as trees,
               array_agg(distinct integration) as integrations
        from writes
        group by leaf
        having count(distinct root) > 1
           and count(distinct integration) > 1
        order by count(distinct root) desc, leaf
        """
    ).fetchall()
