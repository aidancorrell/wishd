-- 017: cluster inventory, and the link from applications to the hardware.
--
-- D4 deferred this out of Phase 02 with a stated reason: it exists to price
-- resource-seconds, and building it before the cost model meant guessing at the
-- shape the cost model needed. Phase 05 is that cost model, and this is its
-- first task -- exactly as the deferral said it should be.
--
-- Core-seconds come free from the Spark event log (executor add/remove events
-- carry timestamps and core counts). What an event log cannot know is what the
-- hardware *cost*: which instance types, how many, and whether they were Spot or
-- On-Demand. That is what this table is for, and it is the only part of cost
-- attribution that needs an AWS call.
--
-- `tags` is not decoration. AWS Cost and Usage Report rows carry
-- `resourceTags/user:...`, not cluster ids, so the tags are the join key between
-- a cluster and its bill.

create table if not exists clusters (
    cluster_id  text primary key,
    platform    text not null default 'emr',   -- emr | databricks | dataproc
    name        text,

    started_at  timestamptz,
    -- Null while the cluster is alive. Inventing an end time would stop a
    -- running cluster accruing cost the moment we first looked at it.
    ended_at    timestamptz,

    tags        jsonb not null default '{}'::jsonb,
    synced_at   timestamptz not null default now(),
    created_at  timestamptz not null default now()
);

create index if not exists clusters_window_idx on clusters (started_at, ended_at);

-- Instance groups are replaced wholesale on each sync rather than versioned.
--
-- A versioned history would be the "right" model and is not worth it yet: the
-- cost report already prices what actually ran, hour by hour, so the inventory's
-- job is to explain *shape* (what kind of hardware, Spot or not), not to be the
-- billing record. If cost attribution ever needs per-hour capacity, this becomes
-- a timeline -- and that is a migration, not a redesign.
create table if not exists cluster_instance_groups (
    cluster_id    text not null references clusters (cluster_id) on delete cascade,
    role          text not null,               -- MASTER | CORE | TASK
    instance_type text not null,
    market        text not null,               -- ON_DEMAND | SPOT
    count         int  not null default 0,
    primary key (cluster_id, role, instance_type, market)
);

-- Which cluster ran an application. Nullable and unconstrained for the same
-- reason spark_apps.run_id is: event logs are routinely backfilled from clusters
-- that no longer exist, and metrics we already parsed beat metrics we discarded.
alter table spark_apps add column if not exists cluster_id text;

create index if not exists spark_apps_cluster_idx on spark_apps (cluster_id);
