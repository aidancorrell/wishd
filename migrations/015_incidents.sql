-- 015: incidents.
--
-- One late source table breaches the freshness monitor on every one of the fifty
-- tables built from it. A tool that sends fifty pages gets muted, and then the
-- fifty-first alert -- a genuinely different problem -- is missed too. The
-- roadmap names this directly: alert fatigue is what kills these tools in month
-- three.
--
-- An incident is a set of breaches that lineage says are one event: connected in
-- the graph, close in time. One of them is the cause (nothing broken above it)
-- and the rest are consequences. Only the cause is delivered; the consequences
-- are kept, because "what else is waiting on this" is the second question
-- everybody asks.
--
-- `cause_result_id` pins the incident to the exact evaluation that opened it, so
-- the timeline cannot drift as later evaluations of the same monitor land.

create table if not exists incidents (
    id              bigserial primary key,

    cause_monitor_id bigint  not null references monitors (id) on delete cascade,
    cause_result_id  bigint  references monitor_results (id) on delete set null,

    -- The entity the cause monitor watches, when it watches one. Null for a job
    -- SLO with no dataset attached -- still a valid incident, just not one with a
    -- position in the table graph.
    cause_entity_id  bigint  references dataset_entities (id) on delete set null,

    opened_at       timestamptz not null default now(),

    -- Set when the cause recovers. Broken, fixed, broken again is deliberately
    -- two incidents: reusing the first would make the timeline lie about how
    -- long anything was down.
    resolved_at     timestamptz,

    created_at      timestamptz not null default now(),
    updated_at      timestamptz not null default now()
);

-- One open incident per cause. The partial unique index is what makes a
-- persisting breach stay the same incident across sweeps instead of opening a
-- new one every hour -- which would be the same alert fatigue, moved one level
-- up.
create unique index if not exists incidents_open_cause_idx
    on incidents (cause_monitor_id) where resolved_at is null;

create index if not exists incidents_open_idx on incidents (opened_at desc)
    where resolved_at is null;

create table if not exists incident_consequences (
    incident_id  bigint not null references incidents (id) on delete cascade,
    monitor_id   bigint not null references monitors (id) on delete cascade,
    entity_id    bigint references dataset_entities (id) on delete set null,

    -- Hops downstream of the cause. The blast radius reads better ordered by
    -- distance: the tables one step away are the ones someone will chase first.
    distance     int not null default 1,

    added_at     timestamptz not null default now(),
    primary key (incident_id, monitor_id)
);
