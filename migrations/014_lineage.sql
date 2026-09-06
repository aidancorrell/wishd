-- 014: the lineage graph.
--
-- Edges join *entities* (migration 013), never raw dataset rows. dbt and Spark
-- report the same table under different names inside one run tree, so an edge
-- graph built on dataset ids has `analytics.fct_orders` and
-- `/warehouse/fct_orders` as separate nodes and every chain breaks in the middle.
--
-- Both tables are derived from `run_datasets`, `datasets.facets` and job SQL, and
-- are rebuilt by `lineage.resolve()`. Nothing here is authoritative; a wrong edge
-- is fixed by improving the resolver and re-running.

create table if not exists lineage_edges (
    upstream_id   bigint not null references dataset_entities (id) on delete cascade,
    downstream_id bigint not null references dataset_entities (id) on delete cascade,

    -- How many distinct runs have demonstrated this edge. A dependency seen once
    -- and a dependency seen nightly for a year are different claims, and the
    -- difference matters when deciding whether a vanished edge is a problem.
    run_count     int not null default 0,
    last_seen_at  timestamptz,

    primary key (upstream_id, downstream_id)
);

create index if not exists lineage_edges_downstream_idx on lineage_edges (downstream_id);

create table if not exists column_edges (
    downstream_id     bigint not null references dataset_entities (id) on delete cascade,
    downstream_column text   not null,
    upstream_id       bigint not null references dataset_entities (id) on delete cascade,
    upstream_column   text   not null,

    -- facet  from the producer's columnLineage facet. Derived from Spark's
    --        resolved logical plan, which knows the catalog.
    -- sql    parsed out of collected SQL text by SQLGlot. A fallback for
    --        producers that send SQL and no facet -- dbt, on this stack.
    --
    -- Stored rather than inferred because the two are not equally trustworthy,
    -- and a reader deciding whether to act on an edge needs to know which it is.
    source            text   not null,

    transformation_type    text,   -- DIRECT | INDIRECT
    transformation_subtype text,   -- IDENTITY | JOIN | GROUP_BY | TRANSFORMATION | ...

    -- Whether the value is masked in transit. A compliance question turns on
    -- this, and it is free -- the producer already tells us.
    masking           boolean not null default false,

    updated_at        timestamptz not null default now(),
    primary key (downstream_id, downstream_column, upstream_id, upstream_column)
);

create index if not exists column_edges_upstream_idx
    on column_edges (upstream_id, upstream_column);
