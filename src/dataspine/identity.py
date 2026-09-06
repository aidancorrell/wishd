"""Deciding when two dataset rows are the same table.

Phase 03 resolved a monitor target to several dataset rows and *showed* the
resolution rather than trusting it, which was honest and sufficient. Lineage
cannot work that way: an edge joins two identities, and if `fct_orders` is three
nodes then the graph is wrong in a way no amount of display honesty repairs. Open
question 4 comes due here.

The evidence, in descending order of what it proves:

  **Co-writing.** Two datasets written by runs in the *same run tree* that share
  a leaf name are one table reported twice. This is the strong signal, and it is
  one no single producer can supply — the correlator earned it in Phase 00. It is
  what separates "dbt and Spark describing one write" from "two different tables
  that happen to be called `events`".

  **Leaf name.** Necessary, nowhere near sufficient. Used only as a precondition
  for co-writing, never on its own.

Everything here is derived. `datasets` stays authoritative and `resolve()`
rebuilds entities from it, so a wrong merge is fixed by improving this resolver
and re-running rather than by surgery on production rows.

**The bias is against merging.** A false merge silently rewrites someone's
lineage graph and there is nothing for anyone to notice; a false split shows up
as two nodes with the same name, which is visible and annoying and gets reported.
Given the asymmetry, the resolver declines whenever the evidence is thin.
"""

from __future__ import annotations

import logging
from typing import Any

import psycopg

log = logging.getLogger("dataspine.identity")


def leaf(name: str) -> str:
    """The last segment of a dataset name, splitting on `/` or `.`.

    Shared with `checks._leaf` in behaviour deliberately: a monitor target and a
    lineage node must agree about what a table is called, or a monitor could
    breach on a table the graph shows as untouched.
    """
    return name.replace("\\", "/").rstrip("/").rsplit("/", 1)[-1].rsplit(".", 1)[-1]


def resolve(conn: psycopg.Connection) -> dict[str, int]:
    """Rebuild dataset entities from the current datasets and run tree.

    Idempotent, and safe to run repeatedly: an existing entity is *extended* when
    a new identity joins it rather than replaced. That matters more than it
    sounds — a producer starting to report a fourth name for a table it already
    writes is an ordinary config change, and forking a new entity there would
    split someone's lineage graph in half without warning.
    """
    datasets = conn.execute("select id, namespace, name from datasets").fetchall()
    if not datasets:
        return {"entities": 0, "merged": 0}

    leaves = {row["id"]: leaf(row["name"]) for row in datasets}

    # Union-find over dataset ids. The relation being closed is "co-written in
    # one run tree with a matching leaf", which is not transitive on its own --
    # but identity is, and Monday proving A=B while Tuesday proves B=C has to
    # yield one entity rather than two overlapping pairs.
    parent: dict[int, int] = {row["id"]: row["id"] for row in datasets}

    def find(node: int) -> int:
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)

    merged = 0

    # Declared identities first, because they are evidence of a different kind.
    #
    # Everything below this is inference: co-writing plus a shared leaf name is
    # a good guess that two rows are one table. A `symlinks` facet is not a
    # guess -- it is the producer stating the catalog identity of the thing it
    # just wrote. Iceberg forced the issue (Spark names an Iceberg table by its
    # physical path while dbt calls it `db.table`), but the facet was in the
    # thrift and Spark captures all along, unread. Reading it improves those
    # retroactively too.
    #
    # Same precedence rule as D3: a declared fact beats an inferred one. These
    # unions are applied first and recorded with their own reason, so a reader
    # deciding whether to trust an edge can tell which kind produced it.
    declared: set[int] = set()
    for pair in _symlink_pairs(conn):
        left, right = pair["a"], pair["b"]
        if find(left) != find(right):
            merged += 1
        union(left, right)
        declared.update((left, right))

    for pair in _co_written_pairs(conn):
        left, right = pair["a"], pair["b"]
        # The leaf check is the precondition, not the evidence. Without it, every
        # table a pipeline writes would collapse into one node -- the single most
        # destructive thing a lineage graph can do.
        if leaves.get(left) and leaves.get(left) == leaves.get(right):
            if find(left) != find(right):
                merged += 1
            union(left, right)

    groups: dict[int, list[int]] = {}
    for dataset_id in parent:
        groups.setdefault(find(dataset_id), []).append(dataset_id)

    for root, members in groups.items():
        if len(members) == 1:
            reason = "sole"
        elif declared.intersection(members):
            reason = "declared"
        else:
            reason = "co-written"
        entity_id = _entity_for(conn, members, leaves[root])
        for member in members:
            conn.execute(
                """
                insert into dataset_identities (dataset_id, entity_id, match_reason)
                values (%s, %s, %s)
                on conflict (dataset_id) do update set
                    entity_id = excluded.entity_id,
                    match_reason = excluded.match_reason,
                    updated_at = now()
                """,
                (member, entity_id, reason),
            )

    _drop_empty_entities(conn)
    return {"entities": len(groups), "merged": merged}


def _symlink_pairs(conn: psycopg.Connection) -> list[dict[str, Any]]:
    """Dataset pairs a producer has *declared* to be the same table.

    Reads the `symlinks` facet openlineage-spark attaches to Iceberg datasets and
    joins each stated identifier back onto whatever dataset already carries that
    name. Verified against a real capture:

        file:/warehouse/iceberg/analytics_marts_marts/stg_customers
          symlinks -> analytics_marts_marts.stg_customers @ http://iceberg-rest:8181

    which is exactly the name dbt reports for the same table over thrift.

    **Matched on the fully-qualified name, not the leaf.** The co-write path
    matches leaves because it has nothing better; here the producer supplies
    `schema.table`, so requiring the whole thing costs nothing and removes the
    "two schemas both containing `events`" failure that made leaf matching need
    corroborating evidence in the first place.

    Self-matches are excluded: a dataset whose own name equals its symlink is
    one row, not two, and unioning it with itself would report a merge that did
    not happen.
    """
    return conn.execute(
        """
        select distinct least(d.id, o.id) as a, greatest(d.id, o.id) as b
        from datasets d
        cross join lateral jsonb_array_elements(
            coalesce(d.facets -> 'symlinks' -> 'identifiers', '[]'::jsonb)
        ) as link
        join datasets o
          on o.name = link ->> 'name'
         and o.id <> d.id
        where d.facets -> 'symlinks' -> 'identifiers' is not null
        """
    ).fetchall()


def _co_written_pairs(conn: psycopg.Connection) -> list[dict[str, Any]]:
    """Pairs of datasets written as OUTPUT by runs sharing a root run.

    INPUT is deliberately excluded. A run that reads `stg_orders` and writes
    `fct_orders` says nothing about the two being the same table, and treating it
    as evidence would collapse every table a job touches into a single node.
    """
    return conn.execute(
        """
        select distinct least(a.dataset_id, b.dataset_id) as a,
                        greatest(a.dataset_id, b.dataset_id) as b
        from run_datasets a
        join runs ra on ra.run_id = a.run_id
        join runs rb on coalesce(rb.root_run_id, rb.run_id)
                      = coalesce(ra.root_run_id, ra.run_id)
        join run_datasets b on b.run_id = rb.run_id
        where a.direction = 'OUTPUT'
          and b.direction = 'OUTPUT'
          and a.dataset_id <> b.dataset_id
        """
    ).fetchall()


def _entity_for(conn: psycopg.Connection, members: list[int], name: str) -> int:
    """Find the entity these datasets already belong to, or create one.

    Reusing an existing id is what makes re-resolution non-destructive: anything
    referencing an entity (a lineage edge, an incident, a catalog note) keeps
    pointing at the same node when a fourth identity shows up.
    """
    existing = conn.execute(
        """
        select entity_id, count(*) as members
        from dataset_identities
        where dataset_id = any(%s)
        group by entity_id
        order by count(*) desc, entity_id
        limit 1
        """,
        (members,),
    ).fetchone()

    if existing:
        conn.execute(
            "update dataset_entities set name = %s, updated_at = now() where id = %s",
            (name, existing["entity_id"]),
        )
        return existing["entity_id"]

    return conn.execute(
        "insert into dataset_entities (name) values (%s) returning id", (name,)
    ).fetchone()["id"]


def _drop_empty_entities(conn: psycopg.Connection) -> None:
    """Remove entities every member has left.

    Happens when a merge that used to hold stops holding -- a resolver
    improvement, or a replay that changed the run tree.
    """
    conn.execute(
        """
        delete from dataset_entities e
        where not exists (
            select 1 from dataset_identities i where i.entity_id = e.id
        )
        """
    )


# ------------------------------------------------------------------- read side


def entities(conn: psycopg.Connection, *, limit: int = 500) -> list[dict[str, Any]]:
    """Every logical table, with the physical identities it was assembled from."""
    rows = conn.execute(
        """
        select e.id, e.name,
               json_agg(json_build_object(
                   'dataset_id', d.id,
                   'namespace', d.namespace,
                   'name', d.name,
                   'match_reason', i.match_reason
               ) order by d.namespace) as datasets
        from dataset_entities e
        join dataset_identities i on i.entity_id = e.id
        join datasets d on d.id = i.dataset_id
        group by e.id, e.name
        order by e.name
        limit %s
        """,
        (limit,),
    ).fetchall()
    return rows


def entity_for_dataset(conn: psycopg.Connection, dataset_id: int) -> dict[str, Any] | None:
    return conn.execute(
        """
        select e.*
        from dataset_identities i
        join dataset_entities e on e.id = i.entity_id
        where i.dataset_id = %s
        """,
        (dataset_id,),
    ).fetchone()


def get_entity(conn: psycopg.Connection, entity_id: int) -> dict[str, Any] | None:
    rows = conn.execute(
        """
        select e.id, e.name,
               json_agg(json_build_object(
                   'dataset_id', d.id,
                   'namespace', d.namespace,
                   'name', d.name,
                   'match_reason', i.match_reason
               ) order by d.namespace) as datasets
        from dataset_entities e
        join dataset_identities i on i.entity_id = e.id
        join datasets d on d.id = i.dataset_id
        where e.id = %s
        group by e.id, e.name
        """,
        (entity_id,),
    ).fetchone()
    return rows


def unmerged_candidates(
    conn: psycopg.Connection, entity_id: int
) -> list[dict[str, Any]]:
    """Datasets sharing this entity's leaf name that were *not* merged into it.

    Where the resolver declines, it says so. Monitors match on leaf name
    (Phase 03) while lineage merges only on co-write evidence, so the two can
    legitimately disagree about what one table is -- a monitor watching three
    identities while the graph shows two nodes. Both choices are right for their
    own job, but a disagreement nobody can see is a trap: someone reads a graph
    showing no downstream consumers and concludes a table is safe to drop.

    Verified against real seeded data, where an `s3://` identity of `fct_orders`
    is only ever co-written inside its own run tree and so has no evidence tying
    it to the dbt and Spark pair.
    """
    return conn.execute(
        """
        select d.id as dataset_id, d.namespace, d.name, i.entity_id
        from dataset_entities e
        join datasets d
          on regexp_replace(d.name, '^.*[./]', '') = e.name
        join dataset_identities i on i.dataset_id = d.id
        where e.id = %s and i.entity_id <> e.id
        order by d.namespace, d.name
        """,
        (entity_id,),
    ).fetchall()


def find(conn: psycopg.Connection, term: str, *, limit: int = 50) -> list[dict[str, Any]]:
    """Entities matching a name. Exact leaf match first.

    Ordering matters: `orders` and `fct_orders` both match a substring search for
    "orders", and a user who typed the first must not silently be handed the
    second.
    """
    return conn.execute(
        """
        select e.id, e.name, count(i.dataset_id) as identities
        from dataset_entities e
        join dataset_identities i on i.entity_id = e.id
        where e.name = %(term)s or e.name ilike %(like)s
        group by e.id, e.name
        order by (e.name = %(term)s) desc, length(e.name), e.name
        limit %(limit)s
        """,
        {"term": term, "like": f"%{term}%", "limit": limit},
    ).fetchall()
