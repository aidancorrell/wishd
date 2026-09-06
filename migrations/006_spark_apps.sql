-- 006: Spark application metrics from event logs.
--
-- Spark event logs and OpenLineage events come from two mechanisms that know
-- nothing about each other, but both carry the Spark application id:
--
--   OpenLineage  run facet spark_applicationDetails.applicationId
--   event log    SparkListenerApplicationStart."App ID"
--
-- That shared key is what turns "here are some stage metrics" into "this dbt
-- model's Spark job spilled 4GB", which is the whole point of Phase 02.
--
-- run_id is nullable and not a foreign key, for the same reason parent_run_id
-- is not: a backfill routinely covers applications whose OpenLineage events
-- have not arrived (or never will). Keeping unlinked metrics beats discarding
-- data we already parsed -- `relink_orphans` attaches them when the run shows up.

create table if not exists spark_apps (
    app_id      text primary key,
    run_id      uuid,
    app_name    text,
    started_at  timestamptz,
    ended_at    timestamptz,
    duration_ms bigint,
    metrics     jsonb not null,
    source_uri  text,
    ingested_at timestamptz not null default now(),
    updated_at  timestamptz not null default now()
);

create index if not exists spark_apps_run_idx on spark_apps (run_id);

-- The app-id lookup runs once per ingested event log; a year's backfill does it
-- thousands of times. Without this it is a seq scan over every run.
create index if not exists runs_spark_app_id_idx
    on runs ((facets #>> '{spark_applicationDetails,applicationId}'));
