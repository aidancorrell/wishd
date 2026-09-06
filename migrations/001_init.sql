-- dataspine 001: the spine.
--
-- Entity model follows the OpenLineage object model (spec 2-0-2):
--   Job     identified by (namespace, name)
--   Run     identified by runId (UUID), belongs to exactly one Job
--   Dataset identified by (namespace, name)
-- Everything engine-specific rides in `facets` jsonb rather than in columns,
-- which is what keeps the ingest path stack-agnostic.

create table if not exists schema_migrations (
    version     text primary key,
    applied_at  timestamptz not null default now()
);

-- ---------------------------------------------------------------- raw archive

-- Every accepted event, verbatim. This is the audit log and the rebuild source:
-- if the correlator logic changes, we replay this table rather than losing history.
create table if not exists events (
    id              bigserial primary key,
    received_at     timestamptz not null default now(),
    event_time      timestamptz not null,
    event_kind      text        not null,          -- RUN | JOB | DATASET
    event_type      text,                          -- START|RUNNING|COMPLETE|ABORT|FAIL|OTHER
    run_id          uuid,
    job_namespace   text,
    job_name        text,
    producer        text,
    payload         jsonb       not null
);

create index if not exists events_run_id_idx      on events (run_id);
create index if not exists events_received_at_idx on events (received_at desc);
create index if not exists events_job_idx         on events (job_namespace, job_name);

-- ---------------------------------------------------------------------- jobs

create table if not exists jobs (
    id              bigserial primary key,
    namespace       text not null,
    name            text not null,
    -- Denormalised from the jobType job facet. Nullable: plenty of producers
    -- omit it, and we would rather store the run than reject it.
    integration     text,                          -- AIRFLOW | DBT | SPARK | ...
    job_type        text,                          -- DAG | TASK | MODEL | SQL_JOB | ...
    processing_type text,                          -- BATCH | STREAMING | SERVICE
    description     text,
    facets          jsonb not null default '{}'::jsonb,
    first_seen_at   timestamptz not null default now(),
    updated_at      timestamptz not null default now(),
    unique (namespace, name)
);

create index if not exists jobs_integration_idx on jobs (integration);

-- ---------------------------------------------------------------------- runs

-- run_state is deliberately not an enum type: adding a state should not require
-- a migration lock on a hot table.
create table if not exists runs (
    run_id             uuid primary key,
    job_id             bigint not null references jobs (id),

    -- Correlation. parent_run_id / root_run_id are intentionally NOT foreign
    -- keys: events arrive out of order and a child frequently lands before its
    -- parent. The correlator creates a placeholder parent row from the parent
    -- facet (which carries job namespace+name), so these resolve in practice,
    -- but the schema must not depend on arrival order.
    parent_run_id      uuid,
    root_run_id        uuid,
    depth              int not null default 0,

    state              text not null default 'UNKNOWN',  -- UNKNOWN|RUNNING|COMPLETED|FAILED|ABORTED
    started_at         timestamptz,
    ended_at           timestamptz,
    nominal_start_time timestamptz,
    nominal_end_time   timestamptz,

    error_message      text,
    error_stacktrace   text,

    producer           text,
    facets             jsonb not null default '{}'::jsonb,

    -- Placeholder rows are runs we know exist only because a child referenced
    -- them. They get promoted the moment their own events arrive.
    is_placeholder     boolean not null default false,

    event_count        int not null default 0,
    first_event_at     timestamptz,
    last_event_at      timestamptz,
    created_at         timestamptz not null default now(),
    updated_at         timestamptz not null default now()
);

create index if not exists runs_job_id_idx     on runs (job_id);
create index if not exists runs_parent_idx     on runs (parent_run_id);
create index if not exists runs_root_idx       on runs (root_run_id);
create index if not exists runs_started_at_idx on runs (started_at desc nulls last);
create index if not exists runs_state_idx      on runs (state);

-- ------------------------------------------------------------------ datasets

create table if not exists datasets (
    id             bigserial primary key,
    namespace      text not null,
    name           text not null,
    facets         jsonb not null default '{}'::jsonb,
    first_seen_at  timestamptz not null default now(),
    updated_at     timestamptz not null default now(),
    unique (namespace, name)
);

-- Dataset I/O edges. This is the raw material the lineage graph is built from
-- in Phase 04; for now it is just "what did this run read and write".
create table if not exists run_datasets (
    run_id      uuid   not null references runs (run_id) on delete cascade,
    dataset_id  bigint not null references datasets (id),
    direction   text   not null check (direction in ('INPUT', 'OUTPUT')),
    facets      jsonb  not null default '{}'::jsonb,   -- inputFacets / outputFacets
    row_count   bigint,
    size_bytes  bigint,
    updated_at  timestamptz not null default now(),
    primary key (run_id, dataset_id, direction)
);

create index if not exists run_datasets_dataset_idx on run_datasets (dataset_id, direction);

-- --------------------------------------------------------------------- views

-- Orphans: runs that name a parent we have never received any event for.
-- "Reject or quarantine orphans loudly" — this view is the loudspeaker, and the
-- number it returns is the single best health metric for the whole ingest path.
create or replace view unstitched_runs as
select r.run_id,
       j.namespace as job_namespace,
       j.name      as job_name,
       r.parent_run_id,
       r.state,
       r.started_at
from runs r
join jobs j on j.id = r.job_id
where r.parent_run_id is not null
  and not exists (
      select 1 from runs p
      where p.run_id = r.parent_run_id
        and p.is_placeholder = false
  );

create or replace view run_summary as
select r.run_id,
       j.namespace as job_namespace,
       j.name      as job_name,
       j.integration,
       j.job_type,
       r.state,
       r.parent_run_id,
       r.root_run_id,
       r.depth,
       r.started_at,
       r.ended_at,
       extract(epoch from (r.ended_at - r.started_at)) * 1000 as duration_ms,
       r.error_message,
       r.event_count,
       r.is_placeholder
from runs r
join jobs j on j.id = r.job_id;
