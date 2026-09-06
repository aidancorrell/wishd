-- 007: monitors, metric history, and the evaluation audit trail.
--
-- Phase 03 turns the spine into an observability tool. The first slice deliberately
-- adds no new collection: every metric here is computed from `runs`, `run_datasets`
-- and `datasets`, which the gateway already fills. That constraint is the point --
-- a monitor you can run without handing us warehouse credentials is a monitor
-- people will actually turn on.
--
-- Three tables, because definition, observation and judgement are different things
-- with different lifetimes:
--
--   monitors         what to watch. Declarative, reconciled from YAML in the
--                    user's repo, so the definition lives in review alongside the
--                    dbt model it protects.
--   metric_points    what was observed. Append-mostly, partitioned, the training
--                    data every future detector reads.
--   monitor_results  what we concluded, and when. The audit trail that lets
--                    someone ask "why did this not alert?" six weeks later.
--
-- Keeping observation separate from judgement is what makes thresholds
-- retroactive: change a bound and re-decide against history you already have,
-- instead of waiting a week to find out whether the new number was sensible.

-- --------------------------------------------------------------------- monitors

create table if not exists monitors (
    id            bigserial primary key,

    -- The stable identity, chosen by the user in YAML. Everything reconciles on
    -- this: rename a monitor in the file and you get a new monitor with fresh
    -- history, which is the honest outcome -- a renamed monitor usually means a
    -- redefined one, and silently carrying the old baseline over would hide that.
    name          text not null unique,

    kind          text not null,   -- freshness | row_count | schema_drift | job_duration | ...

    -- What is being watched. `target` is the user's string, kept verbatim so the
    -- UI can show what they wrote; resolution to concrete dataset/job rows happens
    -- at evaluation time and is recorded in the result, not frozen here. A monitor
    -- written before its table exists must still be applyable.
    target_kind   text not null check (target_kind in ('dataset', 'job')),
    target        text not null,

    config        jsonb not null default '{}'::jsonb,  -- thresholds, windows, comparison
    schedule      text  not null default 'hourly',     -- hourly | daily | manual
    enabled       boolean not null default true,

    -- The file this came from. `dataspine apply` uses it to scope reconciliation:
    -- deleting a monitor from one file must not disable monitors defined elsewhere.
    source        text,

    -- Denormalised from the newest monitor_result. Purely so the list view is one
    -- query rather than a lateral join per row; monitor_results stays the truth.
    last_status       text,
    last_evaluated_at timestamptz,

    created_at    timestamptz not null default now(),
    updated_at    timestamptz not null default now()
);

create index if not exists monitors_enabled_idx on monitors (enabled, schedule);
create index if not exists monitors_target_idx  on monitors (target_kind, target);

-- ---------------------------------------------------------------- metric history

-- `observed_at` is when the measured thing HAPPENED, not when we looked at it.
--
-- That distinction is the whole design. A monitor evaluated hourly against a job
-- that runs nightly would otherwise store 24 copies of one number, and any
-- baseline computed from that history would be measuring our polling interval
-- rather than the pipeline. Keying on the subject's own timestamp makes repeated
-- evaluation idempotent for free, and it is what lets `dataspine apply` backfill
-- a training window out of the run archive the moment a monitor is created --
-- instead of arming a week later.
--
-- `subject` is the identity of the observation: a run id for job monitors, a
-- dataset write key for dataset monitors. It is in the primary key so two
-- distinct runs that started in the same instant are two points, not a collision.
create table metric_points (
    monitor_id  bigint      not null references monitors (id) on delete cascade,
    observed_at timestamptz not null,
    subject     text        not null,
    value       double precision,
    context     jsonb       not null default '{}'::jsonb,
    recorded_at timestamptz not null default now(),
    primary key (monitor_id, observed_at, subject)
) partition by range (observed_at);

-- Same backstop reasoning as `events` (migration 003): a metric point that lands
-- nowhere is a hole in a baseline, and Postgres rejects inserts matching no
-- partition. `dataspine maintain` provisions real months ahead so this stays empty.
create table metric_points_default partition of metric_points default;

-- The read pattern is "the last N points for one monitor, newest first" -- every
-- detector, every sparkline, every backfill dedup check.
create index if not exists metric_points_monitor_idx
    on metric_points (monitor_id, observed_at desc);

-- ----------------------------------------------------------------- evaluations

create table if not exists monitor_results (
    id           bigserial primary key,
    monitor_id   bigint not null references monitors (id) on delete cascade,
    evaluated_at timestamptz not null default now(),

    -- ok                 measured, within bounds
    -- breach             measured, outside bounds
    -- insufficient_data  nothing to measure yet. NOT a breach: a monitor on a
    --                    table that has not run yet must stay quiet, or the first
    --                    thing a new user sees is a wall of false alarms.
    -- error              the monitor itself failed. Also not a breach, and loud
    --                    in a different place, because "broken monitor" and
    --                    "broken data" need different people.
    status       text   not null check (status in ('ok', 'breach', 'insufficient_data', 'error')),

    value        double precision,

    -- What the value was compared against, stored per evaluation rather than read
    -- from the monitor at display time. Thresholds get edited; a result must still
    -- explain the decision it actually made.
    threshold    jsonb,

    message      text,
    subject      text,
    run_id       uuid,          -- the run that produced the observation, when there is one
    context      jsonb  not null default '{}'::jsonb,

    -- True when this evaluation changed the monitor's status. Alerting fires on
    -- transitions, so this column IS the dedup: a table breaching for six hours
    -- is one alert, not six.
    transitioned boolean not null default false
);

create index if not exists monitor_results_monitor_idx
    on monitor_results (monitor_id, evaluated_at desc);

-- The "what is broken right now" query, which the UI runs on every page load.
create index if not exists monitor_results_breach_idx
    on monitor_results (evaluated_at desc) where status = 'breach';
