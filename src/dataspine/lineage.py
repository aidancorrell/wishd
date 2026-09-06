"""Building and walking the lineage graph.

The roadmap assumed column lineage would mean SQLGlot over collected SQL text.
The captures said otherwise: **openlineage-spark 1.52.0 already emits a
`columnLineage` facet** carrying, per output column, the input columns, the
transformation type (DIRECT/INDIRECT), its subtype (IDENTITY, JOIN, GROUP_BY) and
a masking flag. That comes from Spark's *resolved logical plan* — it knows the
catalog, the view definitions and the actual types, none of which a parser can
recover from text alone.

So the order is **facet first, SQLGlot second** (ADR-007). SQLGlot fills the gap
for producers that send SQL but no facet, which on the reference stack means dbt.

Two rules run through the whole module:

  **Edges join entities, never raw dataset rows.** dbt and Spark report the same
  table under different names inside one run tree; an edge graph built on dataset
  ids breaks every chain in the middle. That is why `identity.resolve()` runs
  first.

  **Decline rather than guess.** A `select *` with no schema, or an unqualified
  column with two candidate tables, yields no column edge. A missing edge is
  recoverable — someone notices and asks. A wrong edge silently misdirects the
  root-cause walk of an incident, and nobody notices at all.
"""

from __future__ import annotations

import logging
from typing import Any

import psycopg

log = logging.getLogger("dataspine.lineage")

# The dialect SQL is parsed as. SparkSQL on the reference stack (ADR-003); it is
# also the most permissive of the ANSI-ish dialects, so it degrades well when a
# statement actually came from somewhere else.
DEFAULT_DIALECT = "spark"

# Traversal ceiling. A lineage graph acquires cycles through incremental models
# and bad replays, and a walk that does not terminate takes the UI down with it.
MAX_DEPTH = 25


def resolve(conn: psycopg.Connection) -> dict[str, int]:
    """Rebuild the lineage graph. Assumes `identity.resolve()` has run.

    Idempotent: upserts edges and refreshes counts, so running it after every
    ingest batch or from cron are both fine.
    """
    tables = _resolve_table_edges(conn)
    facet_columns = _resolve_column_edges_from_facets(conn)
    sql_columns = _resolve_column_edges_from_sql(conn)
    return {
        "table_edges": tables,
        "column_edges_facet": facet_columns,
        "column_edges_sql": sql_columns,
    }


# --------------------------------------------------------------- table edges


def _resolve_table_edges(conn: psycopg.Connection) -> int:
    """Every (input entity -> output entity) pair a run demonstrates.

    `upstream_id <> downstream_id` drops self-edges. An incremental model reads
    and writes the same table, and a self-edge makes every downstream walk cycle
    forever for no informational gain.
    """
    conn.execute(
        """
        insert into lineage_edges (upstream_id, downstream_id, run_count, last_seen_at)
        select ui.entity_id, di.entity_id, count(distinct i.run_id), max(r.started_at)
        from run_datasets i
        join run_datasets o          on o.run_id = i.run_id
        join runs r                  on r.run_id = i.run_id
        join dataset_identities ui   on ui.dataset_id = i.dataset_id
        join dataset_identities di   on di.dataset_id = o.dataset_id
        where i.direction = 'INPUT'
          and o.direction = 'OUTPUT'
          and ui.entity_id <> di.entity_id
        group by ui.entity_id, di.entity_id
        on conflict (upstream_id, downstream_id) do update set
            run_count = excluded.run_count,
            last_seen_at = excluded.last_seen_at
        """
    )
    return conn.execute("select count(*) as n from lineage_edges").fetchone()["n"]


# -------------------------------------------------------- column edges: facet


def _resolve_column_edges_from_facets(conn: psycopg.Connection) -> int:
    """Column edges from the producer's own `columnLineage` facet."""
    rows = conn.execute(
        """
        select di.entity_id as downstream_id,
               d.facets -> 'columnLineage' -> 'fields' as fields
        from datasets d
        join dataset_identities di on di.dataset_id = d.id
        where d.facets -> 'columnLineage' ? 'fields'
        """
    ).fetchall()

    written = 0
    for row in rows:
        fields = row["fields"]
        if not isinstance(fields, dict):
            continue
        for column, spec in fields.items():
            for source in (spec or {}).get("inputFields") or []:
                upstream = _entity_for_name(
                    conn, source.get("namespace"), source.get("name")
                )
                if upstream is None or upstream == row["downstream_id"]:
                    continue
                transformation = (source.get("transformations") or [{}])[0]
                _upsert_column_edge(
                    conn,
                    downstream_id=row["downstream_id"],
                    downstream_column=column,
                    upstream_id=upstream,
                    upstream_column=source.get("field") or "",
                    source="facet",
                    kind=transformation.get("type"),
                    subtype=transformation.get("subtype"),
                    masking=bool(transformation.get("masking")),
                )
                written += 1
    return written


def _entity_for_name(
    conn: psycopg.Connection, namespace: Any, name: Any
) -> int | None:
    """Entity id for a (namespace, name) the facet points at.

    Falls back to a leaf match: a facet can name a dataset under an identity we
    have stored differently, and refusing to resolve it would drop exactly the
    cross-producer edges this project exists to provide.
    """
    if not isinstance(name, str):
        return None
    row = conn.execute(
        """
        select i.entity_id
        from datasets d
        join dataset_identities i on i.dataset_id = d.id
        where (d.namespace = %(ns)s and d.name = %(name)s)
           or regexp_replace(d.name, '^.*[./]', '')
              = regexp_replace(%(name)s, '^.*[./]', '')
        order by (d.namespace = %(ns)s and d.name = %(name)s) desc
        limit 1
        """,
        {"ns": namespace, "name": name},
    ).fetchone()
    return row["entity_id"] if row else None


def _upsert_column_edge(
    conn: psycopg.Connection,
    *,
    downstream_id: int,
    downstream_column: str,
    upstream_id: int,
    upstream_column: str,
    source: str,
    kind: str | None = None,
    subtype: str | None = None,
    masking: bool = False,
) -> None:
    """Store a column edge.

    A `sql`-sourced edge never overwrites a `facet`-sourced one: the facet knows
    the catalog and the parser is inferring from text. The reverse is allowed --
    a producer that starts sending facets should supersede the guesswork.
    """
    conn.execute(
        """
        insert into column_edges (downstream_id, downstream_column, upstream_id,
                                  upstream_column, source, transformation_type,
                                  transformation_subtype, masking)
        values (%(downstream_id)s, %(downstream_column)s, %(upstream_id)s,
                %(upstream_column)s, %(source)s, %(kind)s, %(subtype)s, %(masking)s)
        on conflict (downstream_id, downstream_column, upstream_id, upstream_column)
        do update set
            source = case
                when column_edges.source = 'facet' and excluded.source = 'sql'
                then column_edges.source else excluded.source end,
            transformation_type = case
                when column_edges.source = 'facet' and excluded.source = 'sql'
                then column_edges.transformation_type else excluded.transformation_type end,
            transformation_subtype = case
                when column_edges.source = 'facet' and excluded.source = 'sql'
                then column_edges.transformation_subtype
                else excluded.transformation_subtype end,
            masking = case
                when column_edges.source = 'facet' and excluded.source = 'sql'
                then column_edges.masking else excluded.masking end,
            updated_at = now()
        """,
        {
            "downstream_id": downstream_id,
            "downstream_column": downstream_column,
            "upstream_id": upstream_id,
            "upstream_column": upstream_column,
            "source": source,
            "kind": kind,
            "subtype": subtype,
            "masking": masking,
        },
    )


# ------------------------------------------------------ column edges: SQLGlot


def _resolve_column_edges_from_sql(conn: psycopg.Connection) -> int:
    """Column edges parsed from collected SQL, for producers that send no facet.

    Only runs for outputs that have no facet-derived columns already. Parsing SQL
    to re-derive what Spark told us directly would be spending CPU to produce a
    worse answer.
    """
    rows = conn.execute(
        """
        select distinct di.entity_id as downstream_id,
               j.facets #>> '{sql,query}' as query,
               j.id as job_id
        from run_datasets o
        join runs r                on r.run_id = o.run_id
        join jobs j                on j.id = r.job_id
        join dataset_identities di on di.dataset_id = o.dataset_id
        where o.direction = 'OUTPUT'
          and j.facets #>> '{sql,query}' is not null
          and not exists (
              select 1 from column_edges c
              where c.downstream_id = di.entity_id and c.source = 'facet'
          )
        """
    ).fetchall()

    written = 0
    for row in rows:
        for edge in parse_column_lineage(row["query"]):
            upstream = _entity_for_name(conn, None, edge["upstream_table"])
            if upstream is None or upstream == row["downstream_id"]:
                continue
            _upsert_column_edge(
                conn,
                downstream_id=row["downstream_id"],
                downstream_column=edge["column"],
                upstream_id=upstream,
                upstream_column=edge["upstream_column"],
                source="sql",
                kind="DIRECT",
            )
            written += 1
    return written


def parse_column_lineage(query: str | None) -> list[dict[str, str]]:
    """Map output columns to (table, column) using SQLGlot. Never raises.

    Declines in exactly the cases where a parser cannot know the answer:

      * `select *` — the columns depend on a schema we were not given.
      * an unqualified column with more than one candidate table — `select id
        from a join b` could mean either, and picking one puts a wrong edge into
        the graph an incident's root-cause walk follows.

    dbt also emits DDL (`alter table …`) and vendor-specific statements among its
    per-statement SQL, so unparseable input is normal rather than exceptional and
    is skipped quietly.
    """
    if not query or not query.strip():
        return []

    try:
        import sqlglot
        from sqlglot import expressions as exp
        from sqlglot.lineage import lineage as sqlglot_lineage
    except ImportError:  # pragma: no cover - declared dependency
        log.warning("sqlglot is not installed; SQL column lineage is unavailable")
        return []

    try:
        statement = sqlglot.parse_one(query, dialect=DEFAULT_DIALECT)
    except Exception:
        return []
    if statement is None:
        return []

    select = statement if isinstance(statement, exp.Select) else statement.find(exp.Select)
    if select is None:
        return []

    # A star means the output columns are whatever the source has, which we do
    # not know without a schema. Nothing here is recoverable by guessing.
    if any(isinstance(projection, exp.Star) for projection in select.expressions):
        return []

    outputs = []
    for projection in select.expressions:
        name = projection.alias_or_name
        if name and name != "*":
            outputs.append(name)

    edges: list[dict[str, str]] = []
    for column in outputs:
        try:
            node = sqlglot_lineage(column, query, dialect=DEFAULT_DIALECT)
        except Exception:
            continue

        # Leaf nodes of the lineage tree are the physical tables. Anything else
        # is an intermediate scope.
        sources = []
        for downstream in node.walk():
            source = downstream.source
            if isinstance(source, exp.Table):
                table = source.name
                field = downstream.name.rsplit(".", 1)[-1]
                if table and field:
                    sources.append((table, field))

        # More than one distinct table for one output column, from an
        # unqualified reference, means the parser guessed. Decline.
        if len({table for table, _ in sources}) > 1 and not _is_qualified(select, column):
            continue
        for table, field in sources:
            edges.append(
                {"column": column, "upstream_table": table, "upstream_column": field}
            )
    return edges


# ------------------------------------------------------------------- coverage


def coverage_of(query: str | None) -> dict[str, Any]:
    """How much of one statement's column lineage SQLGlot could resolve.

    The reason is the useful part. "40% declined because they were all `select *`"
    is a schema-registry problem; "40% unparseable" is a dialect problem; and the
    two have entirely different fixes. A single coverage percentage hides that.

    Reasons:
      no_projection  DDL with no columns -- `drop table`, `alter table`. Not a
                     failure, and excluded from the rate: dbt emits these among
                     its per-statement SQL, and counting them would report a
                     coverage catastrophe caused by statements that never had
                     columns to resolve.
      unparseable    SQLGlot could not read it. A dialect gap.
      star           `select *` with no schema. A catalog gap, not a parser one.
      ambiguous      an unqualified column with several candidate tables. We
                     decline on purpose (ADR-007): a wrong edge misdirects an
                     incident's root-cause walk, and a missing one is noticed.
      no_upstream    parsed and projected, but there is no table underneath at
                     all -- a literal-only select. Correct silence, not a miss,
                     and excluded from the rate.
      no_source      tables were present but a column could not be traced to
                     one. A genuine gap, and it counts against us.
    """
    empty = {
        "outputs": 0, "resolved": 0, "declined": 0, "declined_reason": None,
    }
    if not query or not query.strip():
        return empty

    try:
        import sqlglot
        from sqlglot import expressions as exp
    except ImportError:  # pragma: no cover - declared dependency
        return {**empty, "declined_reason": "unparseable"}

    # sqlglot logs a warning for every statement it falls back to `Command` on
    # (`show databases`, `show table extended`). dbt emits dozens per run, and
    # the noise buries the report we are here to read -- the fallback is already
    # captured as a decline reason.
    _sqlglot_log = logging.getLogger("sqlglot")
    previous = _sqlglot_log.level
    _sqlglot_log.setLevel(logging.ERROR)
    try:
        statement = sqlglot.parse_one(query, dialect=DEFAULT_DIALECT)
    except Exception:
        return {**empty, "declined": 1, "declined_reason": "unparseable"}
    finally:
        _sqlglot_log.setLevel(previous)
    if statement is None:
        return {**empty, "declined": 1, "declined_reason": "unparseable"}

    select = statement if isinstance(statement, exp.Select) else statement.find(exp.Select)
    if select is None:
        return {**empty, "declined_reason": "no_projection"}

    if any(isinstance(projection, exp.Star) for projection in select.expressions):
        columns = [p for p in select.expressions if p.alias_or_name not in ("", "*")]
        return {
            "outputs": max(len(select.expressions), 1),
            "resolved": 0,
            "declined": max(len(select.expressions), 1),
            "declined_reason": "star",
            **({} if columns else {}),
        }

    outputs = [p.alias_or_name for p in select.expressions if p.alias_or_name]
    if not outputs:
        return {**empty, "declined_reason": "no_projection"}

    # A select with no table underneath it -- `select 1 as id, 'a' as segment` --
    # genuinely has no upstream. Scoring that as a miss understates coverage and
    # sends someone chasing a parser bug that does not exist, which is the same
    # mistake as reporting 0% for a corpus of pure DDL. Real dbt projects are
    # full of these: every seed-style staging model is a literal select.
    if not any(t.name for t in select.find_all(exp.Table)):
        return {**empty, "declined_reason": "no_upstream"}

    resolved = 0
    reasons: list[str] = []
    for column in outputs:
        edges = parse_column_lineage(query)
        matched = [e for e in edges if e["column"] == column]
        if matched:
            resolved += 1
            continue
        # Distinguish "we refused because it was ambiguous" from "there was no
        # table under it at all". Both yield no edge; only the first is a
        # decision we made.
        reasons.append("ambiguous" if _has_multiple_sources(select) else "no_source")

    return {
        "outputs": len(outputs),
        "resolved": resolved,
        "declined": len(outputs) - resolved,
        "declined_reason": reasons[0] if reasons else None,
    }


def _has_multiple_sources(select: Any) -> bool:
    from sqlglot import expressions as exp

    return len({t.name for t in select.find_all(exp.Table) if t.name}) > 1


def coverage(statements: list[str | None]) -> dict[str, Any]:
    """Aggregate coverage over a corpus of SQL. Reasons ordered worst-first.

    `coverage` is None rather than 0.0 when nothing in the corpus projected any
    columns. Nought out of nought is not nought per cent, and reporting it as
    such sends someone hunting a parser bug that does not exist.
    """
    reasons: dict[str, int] = {}
    total_outputs = 0
    total_resolved = 0
    with_projection = 0

    for statement in statements:
        report = coverage_of(statement)
        if report["declined_reason"]:
            reasons[report["declined_reason"]] = reasons.get(report["declined_reason"], 0) + 1
        if report["outputs"]:
            with_projection += 1
            total_outputs += report["outputs"]
            total_resolved += report["resolved"]

    return {
        "statements": len(statements),
        "with_projection": with_projection,
        "output_columns": total_outputs,
        "resolved_columns": total_resolved,
        "coverage": (total_resolved / total_outputs) if total_outputs else None,
        "reasons": dict(sorted(reasons.items(), key=lambda kv: (-kv[1], kv[0]))),
    }


def coverage_from_db(conn: psycopg.Connection) -> dict[str, Any]:
    """Coverage over the SQL this installation has actually collected.

    Reports facet-derived edges separately: those did not go through the parser
    at all, and folding them in would flatter the parser's number with work it
    did not do.
    """
    statements = [
        row["query"]
        for row in conn.execute(
            "select facets #>> '{sql,query}' as query from jobs "
            "where facets #>> '{sql,query}' is not null"
        ).fetchall()
    ]
    report = coverage(statements)
    sources = conn.execute(
        "select source, count(*) as n from column_edges group by source"
    ).fetchall()
    report["edges_by_source"] = {row["source"]: row["n"] for row in sources}
    return report


def _is_qualified(select: Any, column: str) -> bool:
    """Whether this output column's source was written with a table qualifier.

    `c.segment` is unambiguous even when two tables are in scope; a bare
    `segment` is not, and the difference decides whether the parse is evidence or
    a coin toss.
    """
    from sqlglot import expressions as exp

    for projection in select.expressions:
        if projection.alias_or_name != column:
            continue
        for reference in projection.find_all(exp.Column):
            if reference.table:
                return True
    return False


# ------------------------------------------------------------------ read side


def edges(conn: psycopg.Connection, *, limit: int = 1000) -> list[dict[str, Any]]:
    return conn.execute(
        """
        select e.upstream_id, u.name as upstream_name,
               e.downstream_id, d.name as downstream_name,
               e.run_count, e.last_seen_at
        from lineage_edges e
        join dataset_entities u on u.id = e.upstream_id
        join dataset_entities d on d.id = e.downstream_id
        order by u.name, d.name
        limit %s
        """,
        (limit,),
    ).fetchall()


def column_edges(
    conn: psycopg.Connection, *, downstream: str | None = None, entity_id: int | None = None
) -> list[dict[str, Any]]:
    where = ["1 = 1"]
    params: dict[str, Any] = {}
    if downstream:
        where.append("d.name = %(downstream)s")
        params["downstream"] = downstream
    if entity_id:
        where.append("c.downstream_id = %(entity_id)s")
        params["entity_id"] = entity_id
    return conn.execute(
        f"""
        select c.*, u.name as upstream_name, d.name as downstream_name
        from column_edges c
        join dataset_entities u on u.id = c.upstream_id
        join dataset_entities d on d.id = c.downstream_id
        where {' and '.join(where)}
        order by c.downstream_column, u.name
        """,
        params,
    ).fetchall()


_WALK = """
with recursive walk as (
    -- Cast explicitly: Postgres infers the seed's type from the parameter and
    -- then refuses the union when the recursive term yields bigint entity ids.
    select {start}::bigint as id, 0 as distance
  union
    -- `union`, never `union all`. A lineage graph is full of diamonds: two
    -- models read the same source and both feed one mart. With `union all` each
    -- distinct *path* is walked separately, so path count grows exponentially in
    -- depth and the traversal stops terminating in any useful time.
    --
    -- Measured at 10^6 runs: 140s with `union all`, milliseconds with `union`.
    -- Deduplicating bounds the work by (nodes x depth) instead of by paths, and
    -- the `distinct on ... order by distance` below still yields the shortest
    -- hop count for each node.
    select {next}, w.distance + 1
    from lineage_edges e
    join walk w on {join}
    where w.distance < %(depth)s
)
select distinct on (w.id) w.id, e.name, w.distance
from walk w
join dataset_entities e on e.id = w.id
where w.distance > 0
order by w.id, w.distance
"""


def upstream(
    conn: psycopg.Connection, entity_id: int, *, depth: int = 5
) -> list[dict[str, Any]]:
    """Everything this table depends on, with hop distance.

    Distance is what makes root cause tractable: the *nearest* failing upstream
    is almost always the cause, and the ones behind it are its consequences.
    """
    return conn.execute(
        _WALK.format(
            start="%(entity_id)s", next="e.upstream_id", join="e.downstream_id = w.id"
        ),
        {"entity_id": entity_id, "depth": min(depth, MAX_DEPTH)},
    ).fetchall()


def downstream(
    conn: psycopg.Connection, entity_id: int, *, depth: int = 5
) -> list[dict[str, Any]]:
    """Everything that depends on this table — the blast radius."""
    return conn.execute(
        _WALK.format(
            start="%(entity_id)s", next="e.downstream_id", join="e.upstream_id = w.id"
        ),
        {"entity_id": entity_id, "depth": min(depth, MAX_DEPTH)},
    ).fetchall()


def graph(
    conn: psycopg.Connection, entity_id: int, *, depth: int = 2
) -> dict[str, Any]:
    """Nodes and edges around one table, ready to render."""
    focus = conn.execute(
        "select id, name from dataset_entities where id = %s", (entity_id,)
    ).fetchone()
    if focus is None:
        return {"focus": None, "nodes": [], "edges": []}

    up = upstream(conn, entity_id, depth=depth)
    down = downstream(conn, entity_id, depth=depth)

    nodes = {focus["id"]: {**focus, "distance": 0, "direction": "focus"}}
    for node in up:
        nodes.setdefault(node["id"], {**node, "direction": "upstream"})
    for node in down:
        nodes.setdefault(node["id"], {**node, "direction": "downstream"})

    ids = list(nodes)
    connecting = conn.execute(
        """
        select e.upstream_id, e.downstream_id, e.run_count
        from lineage_edges e
        where e.upstream_id = any(%(ids)s) and e.downstream_id = any(%(ids)s)
        """,
        {"ids": ids},
    ).fetchall()

    return {"focus": nodes[focus["id"]], "nodes": list(nodes.values()), "edges": connecting}


# --------------------------------------------------------------------- layout

# Layout constants. A lineage graph is a *layered* DAG and the layers are the hop
# distances the traversal already returns, so placing nodes is arithmetic rather
# than a physics simulation. ADR-002 allowed one vendored graph library for this
# page; it turned out not to be needed, which keeps "no build step, no framework,
# no JavaScript" intact for the whole UI.
NODE_WIDTH = 168
NODE_HEIGHT = 38
COLUMN_GAP = 88
ROW_GAP = 18
MARGIN = 20


def layout(graph: dict[str, Any]) -> dict[str, Any]:
    """Assign x/y to each node and endpoints to each edge, for inline SVG.

    Columns are hop distance, signed: upstream to the left, the focus in the
    middle, downstream to the right. That ordering is the one thing a reader
    needs to be able to assume without being told, because it matches the
    direction data actually flows.
    """
    nodes = graph.get("nodes") or []
    if not nodes:
        return {**graph, "width": 0, "height": 0, "placed": [], "links": []}

    def column_of(node: dict[str, Any]) -> int:
        distance = node.get("distance") or 0
        if node.get("direction") == "upstream":
            return -distance
        if node.get("direction") == "downstream":
            return distance
        return 0

    columns: dict[int, list[dict[str, Any]]] = {}
    for node in nodes:
        columns.setdefault(column_of(node), []).append(node)
    for members in columns.values():
        members.sort(key=lambda n: n["name"])

    order = sorted(columns)
    tallest = max(len(members) for members in columns.values())
    height = MARGIN * 2 + tallest * NODE_HEIGHT + (tallest - 1) * ROW_GAP
    width = MARGIN * 2 + len(order) * NODE_WIDTH + (len(order) - 1) * COLUMN_GAP

    placed: dict[int, dict[str, Any]] = {}
    for index, column in enumerate(order):
        members = columns[column]
        block = len(members) * NODE_HEIGHT + (len(members) - 1) * ROW_GAP
        top = (height - block) / 2
        for row, node in enumerate(members):
            placed[node["id"]] = {
                **node,
                "x": MARGIN + index * (NODE_WIDTH + COLUMN_GAP),
                "y": top + row * (NODE_HEIGHT + ROW_GAP),
                "column": column,
            }

    links = []
    for edge in graph.get("edges") or []:
        source = placed.get(edge["upstream_id"])
        target = placed.get(edge["downstream_id"])
        if not source or not target:
            continue
        links.append(
            {
                "x1": source["x"] + NODE_WIDTH,
                "y1": source["y"] + NODE_HEIGHT / 2,
                "x2": target["x"],
                "y2": target["y"] + NODE_HEIGHT / 2,
            }
        )

    return {
        **graph,
        "width": width,
        "height": height,
        "node_width": NODE_WIDTH,
        "node_height": NODE_HEIGHT,
        "placed": sorted(placed.values(), key=lambda n: (n["column"], n["name"])),
        "links": links,
    }


def grouped(conn: psycopg.Connection) -> list[dict[str, Any]]:
    """Entity counts per namespace.

    Depth control alone is not enough on a real warehouse: a graph of 4,000
    tables has to collapse into something a person can actually look at, and the
    namespace is the grouping every producer already gives us for free.
    """
    return conn.execute(
        """
        select d.namespace, count(distinct i.entity_id) as entities
        from datasets d
        join dataset_identities i on i.dataset_id = d.id
        group by d.namespace
        order by count(distinct i.entity_id) desc, d.namespace
        """
    ).fetchall()
