-- 016: catalog annotations and search.
--
-- The catalog is a *view* over entities already stored -- what writes a table,
-- what it depends on, which monitors watch it, when it last changed are all
-- facts something else already needed. A catalog with its own ingestion would be
-- a second source of truth, and the one that drifts is always the one nobody is
-- paged about.
--
-- So there is exactly one new table, holding the only thing no pipeline can
-- infer: who owns this and what it is for.

create table if not exists dataset_annotations (
    -- Keyed by NAME, not by entity id, and deliberately so. Declaring ownership
    -- before the first run is normal -- it is how a new model gets an owner on
    -- the PR that creates it -- and an entity does not exist until something has
    -- written the table. Keying on the name also means annotations survive
    -- re-resolution without depending on entity ids being stable.
    name        text primary key,

    owner       text,
    description text,
    tags        text[] not null default '{}',

    source      text,
    updated_at  timestamptz not null default now()
);

-- Search index.
--
-- Postgres FTS, per the roadmap's rule of only reaching for anything else if a
-- benchmark forces it. A warehouse has thousands of tables, not millions of
-- documents; this is several orders of magnitude away from where a dedicated
-- search engine starts to pay for its operational cost.
--
-- Materialised rather than computed per query because the column list comes from
-- a jsonb facet, and re-extracting it on every keystroke would make search the
-- slowest page in the product.
create table if not exists catalog_search (
    entity_id   bigint primary key references dataset_entities (id) on delete cascade,
    name        text not null,
    document    tsvector not null,
    updated_at  timestamptz not null default now()
);

create index if not exists catalog_search_document_idx
    on catalog_search using gin (document);
create index if not exists catalog_search_name_idx on catalog_search (name);
