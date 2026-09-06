-- 003: partition `events` by month.
--
-- `events` is the append-only archive that every other table is derived from,
-- so it grows fastest and is the first thing that will hurt. Monthly range
-- partitions turn retention into `drop table` -- a metadata operation -- rather
-- than a DELETE over the largest table in the system, which would hold locks,
-- bloat the heap and lose a fight with autovacuum.
--
-- Partition key is received_at, not event_time: retention is about when WE took
-- custody of the data. event_time is producer-controlled and a backfill can
-- legitimately carry timestamps from years ago, which would scatter one ingest
-- across a dozen partitions.
--
-- A DEFAULT partition is deliberate. Postgres rejects an insert that matches no
-- partition, and a rejected event is a permanent hole in a run tree. The
-- backstop guarantees the write always lands; `dataspine maintain` provisions
-- real partitions ahead of time so the default stays empty.

alter table events rename to events_legacy;

create table events (
    id              bigserial,
    received_at     timestamptz not null default now(),
    event_time      timestamptz not null,
    event_kind      text        not null,
    event_type      text,
    run_id          uuid,
    job_namespace   text,
    job_name        text,
    producer        text,
    payload         jsonb       not null,
    -- Postgres requires the partition key in every unique constraint, so the
    -- key is (id, received_at) rather than id alone. Nothing joins on events.id,
    -- so this costs us nothing.
    primary key (id, received_at)
) partition by range (received_at);

create table events_default partition of events default;

create index if not exists events_run_id_idx      on events (run_id);
create index if not exists events_received_at_idx on events (received_at desc);
create index if not exists events_job_idx         on events (job_namespace, job_name);

-- Provision a partition for every month the existing archive spans, BEFORE
-- copying. Without this the whole archive lands in DEFAULT, and Postgres then
-- refuses to attach those months later (it would have to claim rows already in
-- the default partition) -- so retention could never drop any of it.
do $$
declare
    lo date;
    hi date;
    m  date;
    part_name text;
begin
    select date_trunc('month', min(received_at))::date,
           date_trunc('month', max(received_at))::date
      into lo, hi
      from events_legacy;

    if lo is null then
        return;  -- nothing archived yet
    end if;

    m := lo;
    while m <= hi loop
        part_name := 'events_' || to_char(m, 'YYYY_MM');
        execute format(
            'create table if not exists %I partition of events for values from (%L) to (%L)',
            part_name, m, (m + interval '1 month')::date
        );
        m := (m + interval '1 month')::date;
    end loop;
end $$;

-- Carry the archive over. It is the source of truth; losing it would make
-- `dataspine replay` a one-way door.
insert into events (id, received_at, event_time, event_kind, event_type, run_id,
                    job_namespace, job_name, producer, payload)
select id, received_at, event_time, event_kind, event_type, run_id,
       job_namespace, job_name, producer, payload
from events_legacy;

select setval(
    pg_get_serial_sequence('events', 'id'),
    coalesce((select max(id) from events), 1)
);

drop table events_legacy;
