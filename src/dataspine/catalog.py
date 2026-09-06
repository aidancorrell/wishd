"""The catalog: a view over what is already stored, plus search.

The roadmap is specific that this is a *view*, not a new system. Every fact on a
catalog page — what writes this table, what it depends on, which monitors watch
it, when it last changed — is already in the database because something else
needed it. A catalog with its own ingestion path would be a second source of
truth to keep in sync, and the copy that drifts is always the one nobody is paged
about.

The only genuinely new state is the part no pipeline can infer: **who owns this
and what it is for**. Those are annotations, and they live in the same reviewed
YAML as monitors rather than behind a form — an owner set by clicking is an owner
nobody can diff, and it goes stale the week the person changes team.

Search is Postgres full-text. A warehouse has thousands of tables, not millions
of documents, which is several orders of magnitude short of where a dedicated
search engine starts to earn its operational cost.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import psycopg

log = logging.getLogger("dataspine.catalog")


class AnnotationError(ValueError):
    """A `datasets:` block that cannot be applied."""


@dataclass
class Annotation:
    name: str
    owner: str | None = None
    description: str | None = None
    tags: list[str] = field(default_factory=list)
    source: str | None = None


# ------------------------------------------------------------------ annotation


def parse_annotations(raw: Any, *, source: str | None = None) -> list[Annotation]:
    """Read a `datasets:` block. Same strictness as monitor specs, same reason:
    silently accepting a malformed entry yields documentation that is quietly
    wrong, which is worse than none."""
    entries = raw.get("datasets") if isinstance(raw, dict) else None
    if not isinstance(entries, list):
        return []

    annotations = []
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise AnnotationError(f"dataset #{index + 1} is not a mapping")
        name = entry.get("name")
        if not isinstance(name, str) or not name.strip():
            raise AnnotationError(f"dataset #{index + 1} needs a `name:`")
        tags = entry.get("tags") or []
        if not isinstance(tags, list) or any(not isinstance(t, str) for t in tags):
            raise AnnotationError(f"dataset `{name}`: `tags:` must be a list of strings")
        annotations.append(
            Annotation(
                name=name.strip(),
                owner=entry.get("owner"),
                description=entry.get("description"),
                tags=[t.strip() for t in tags],
                source=source,
            )
        )
    return annotations


def apply_annotations(
    conn: psycopg.Connection, annotations: list[Annotation], *, sources: list[str]
) -> int:
    """Reconcile annotations. Keyed by name, so declaring ownership before the
    table exists works — which is how a model gets an owner on the PR that
    creates it."""
    for annotation in annotations:
        conn.execute(
            """
            insert into dataset_annotations (name, owner, description, tags, source)
            values (%s, %s, %s, %s, %s)
            on conflict (name) do update set
                owner = excluded.owner,
                description = excluded.description,
                tags = excluded.tags,
                source = excluded.source,
                updated_at = now()
            """,
            (
                annotation.name,
                annotation.owner,
                annotation.description,
                annotation.tags,
                annotation.source or (sources[0] if sources else None),
            ),
        )
    reindex(conn)
    return len(annotations)


# --------------------------------------------------------------------- listing

_ENTRY_COLUMNS = """
    e.id, e.name,
    a.owner, a.description, coalesce(a.tags, '{}') as tags,
    (select count(*) from lineage_edges le where le.downstream_id = e.id) as upstream_count,
    (select count(*) from lineage_edges le where le.upstream_id = e.id) as downstream_count,
    (select count(*) from monitors m
       where m.enabled and m.target_kind = 'dataset'
         and regexp_replace(m.target, '^.*[./]', '') = e.name) as monitor_count,
    (select max(coalesce(r.ended_at, r.started_at))
       from dataset_identities di
       join run_datasets rd on rd.dataset_id = di.dataset_id and rd.direction = 'OUTPUT'
       join runs r on r.run_id = rd.run_id
      where di.entity_id = e.id) as last_written_at,
    -- The table format, taken from the producer's own `storage` facet rather
    -- than declared by anyone. openlineage-spark reports `storageLayer:
    -- iceberg` on every Iceberg write, so "which of our tables are Iceberg" is
    -- answerable without a second source of truth to keep in sync -- which is
    -- the same argument that kept the whole catalog a view over stored data.
    (select d.facets #>> '{storage,storageLayer}'
       from dataset_identities di
       join datasets d on d.id = di.dataset_id
      where di.entity_id = e.id
        and d.facets #>> '{storage,storageLayer}' is not null
      limit 1) as table_format
"""


def list_entries(conn: psycopg.Connection, *, limit: int = 500) -> list[dict[str, Any]]:
    """Every table, with the context that makes it worth looking at.

    `monitor_count` is on the list rather than buried in a detail page because
    the most useful thing a catalog surfaces on day one is what nobody is
    watching.
    """
    return conn.execute(
        f"""
        select {_ENTRY_COLUMNS}
        from dataset_entities e
        left join dataset_annotations a on a.name = e.name
        order by e.name
        limit %s
        """,
        (limit,),
    ).fetchall()


def get_entry(conn: psycopg.Connection, entity_id: int) -> dict[str, Any] | None:
    entry = conn.execute(
        f"""
        select {_ENTRY_COLUMNS}
        from dataset_entities e
        left join dataset_annotations a on a.name = e.name
        where e.id = %s
        """,
        (entity_id,),
    ).fetchone()
    if entry is None:
        return None

    # The physical names this table arrives under. Shown rather than hidden: a
    # reader has to be able to see that three identities are one table, or the
    # catalog looks like it is concealing something.
    entry["identities"] = conn.execute(
        """
        select d.namespace, d.name, i.match_reason
        from dataset_identities i
        join datasets d on d.id = i.dataset_id
        where i.entity_id = %s
        order by d.namespace, d.name
        """,
        (entity_id,),
    ).fetchall()

    entry["produced_by"] = conn.execute(
        """
        select distinct j.id, j.name, j.integration
        from dataset_identities i
        join run_datasets rd on rd.dataset_id = i.dataset_id and rd.direction = 'OUTPUT'
        join runs r on r.run_id = rd.run_id
        join jobs j on j.id = r.job_id
        where i.entity_id = %s
        order by j.name
        """,
        (entity_id,),
    ).fetchall()

    entry["monitors"] = conn.execute(
        """
        select m.name, m.kind, m.last_status, m.last_evaluated_at
        from monitors m
        where m.enabled
          and m.target_kind = 'dataset'
          and regexp_replace(m.target, '^.*[./]', '') = %s
        order by m.name
        """,
        (entry["name"],),
    ).fetchall()

    entry["columns"] = conn.execute(
        """
        select distinct c.downstream_column as name
        from column_edges c
        where c.downstream_id = %s
        order by 1
        """,
        (entity_id,),
    ).fetchall()

    return entry


# --------------------------------------------------------------------- search


def reindex(conn: psycopg.Connection) -> int:
    """Rebuild the search index from entities, annotations and schemas.

    Materialised rather than computed per query: the column list lives inside a
    jsonb facet, and re-extracting it on every keystroke would make search the
    slowest page in the product.
    """
    conn.execute(
        """
        insert into catalog_search (entity_id, name, document, updated_at)
        select e.id,
               e.name,
               setweight(to_tsvector('simple', coalesce(e.name, '')), 'A')
             || setweight(to_tsvector('simple',
                    coalesce((select string_agg(d.name, ' ')
                                from dataset_identities di
                                join datasets d on d.id = di.dataset_id
                               where di.entity_id = e.id), '')), 'B')
             || setweight(to_tsvector('simple', coalesce(a.owner, '')), 'B')
             || setweight(to_tsvector('simple', array_to_string(coalesce(a.tags, '{}'), ' ')),
                          'B')
             || setweight(to_tsvector('english', coalesce(a.description, '')), 'C')
             -- Columns come from the schema facet on the write edge, which is
             -- where drift detection reads them too. "Which tables have
             -- customer_email" is how a GDPR request gets scoped, and it is the
             -- question that turns a catalog from documentation into a tool.
             || setweight(to_tsvector('simple', coalesce((
                    select string_agg(f ->> 'name', ' ')
                      from dataset_identities di
                      join run_datasets rd on rd.dataset_id = di.dataset_id
                      join datasets d2 on d2.id = di.dataset_id
                      cross join lateral jsonb_array_elements(
                          coalesce(rd.facets -> 'schema' -> 'fields',
                                   d2.facets -> 'schema' -> 'fields', '[]'::jsonb)) as f
                     where di.entity_id = e.id), '')), 'B')
               as document,
               now()
        from dataset_entities e
        left join dataset_annotations a on a.name = e.name
        on conflict (entity_id) do update set
            name = excluded.name,
            document = excluded.document,
            updated_at = now()
        """
    )
    return conn.execute("select count(*) as n from catalog_search").fetchone()["n"]


def search(conn: psycopg.Connection, term: str, *, limit: int = 50) -> list[dict[str, Any]]:
    """Full-text search over the catalog.

    The query is built with `plainto_tsquery`, which treats the input as words
    rather than as tsquery syntax. Users paste fully-qualified names and stray
    punctuation; `analytics.public.fct_orders & |` must be a search, not a
    syntax error.

    An empty term returns nothing rather than everything. "Show me the whole
    warehouse" is the catalog listing, and returning it from an empty search box
    is how a page accidentally becomes a full table scan on every keystroke.
    """
    term = (term or "").strip()
    if not term:
        return []

    # Ensure the index exists for entities created since the last write. Cheap:
    # it is an upsert over thousands of rows, not millions.
    if not conn.execute("select 1 from catalog_search limit 1").fetchone():
        reindex(conn)

    return conn.execute(
        """
        select s.entity_id as id, s.name,
               ts_rank(s.document, plainto_tsquery('simple', %(term)s)) as rank
        from catalog_search s
        where s.document @@ plainto_tsquery('simple', %(term)s)
           or s.document @@ plainto_tsquery('english', %(term)s)
           or s.name ilike %(like)s
        order by (s.name = %(term)s) desc,
                 ts_rank(s.document, plainto_tsquery('simple', %(term)s)) desc,
                 length(s.name),
                 s.name
        limit %(limit)s
        """,
        {"term": term, "like": f"%{term}%", "limit": limit},
    ).fetchall()
