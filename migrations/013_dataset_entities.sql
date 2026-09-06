-- 013: one node per logical table.
--
-- Phase 03 could get away with resolving a monitor target to several dataset
-- rows and *showing* the resolution rather than trusting it. Lineage cannot: an
-- edge joins two identities, and if `fct_orders` is three nodes the graph is
-- wrong in a way no amount of display honesty repairs. This is open question 4
-- coming due, exactly where the roadmap said it would.
--
-- A dataset_entity is the logical table. `dataset_identities` maps the physical
-- rows onto it, one row per dataset, carrying *why* it was merged -- a merge is
-- the one operation here that destroys information (two rows become one node),
-- so it has to keep its receipt.
--
-- Both tables are derived, never authoritative. `datasets` remains the truth and
-- `identity.resolve()` rebuilds these from it, which means a wrong merge is
-- fixed by improving the resolver and re-running rather than by surgery.

create table if not exists dataset_entities (
    id          bigserial primary key,

    -- The leaf name every member shares. Not unique: two genuinely different
    -- tables called `events` are two entities that happen to share a label, and
    -- a unique constraint here would force the resolver to merge them.
    name        text not null,

    created_at  timestamptz not null default now(),
    updated_at  timestamptz not null default now()
);

create index if not exists dataset_entities_name_idx on dataset_entities (name);

create table if not exists dataset_identities (
    dataset_id   bigint primary key references datasets (id) on delete cascade,
    entity_id    bigint not null references dataset_entities (id) on delete cascade,

    -- co-written  two producers reported this write inside one run tree. Evidence
    --             no single producer can supply; the correlator earned it.
    -- sole        nothing else claims this name. An entity of one.
    match_reason text not null,

    updated_at   timestamptz not null default now()
);

create index if not exists dataset_identities_entity_idx on dataset_identities (entity_id);
