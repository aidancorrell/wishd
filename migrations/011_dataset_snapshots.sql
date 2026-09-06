-- 011: catalog metadata for tables no producer tells us about.
--
-- Everything before this reads the run archive, which covers only the tables our
-- own pipelines write. The tables upstream of them -- loaded by Fivetran, by a
-- vendor drop, by a team that has never heard of us -- are invisible, and they
-- are exactly the ones whose silent staleness breaks a pipeline at 02:00.
--
-- A snapshot is one observation of a table's catalog metadata. It carries the
-- same facts an OpenLineage write edge does (when it changed, how many rows, what
-- columns), so `checks._dataset_writes` can union the two and every monitor kind
-- works on a polled table without knowing anything is different.
--
-- `observed_at` is the table's OWN modification time, not the poll time. Same
-- reasoning as `metric_points` (ADR-005): a table polled hourly but written
-- nightly must produce one observation a day, or every baseline built from it
-- measures our cron schedule. It also makes re-polling idempotent for free.
--
-- Not partitioned, unlike events and metric_points: one row per table per change
-- is thousands of rows a year for a large warehouse, not millions.

create table if not exists dataset_snapshots (
    dataset_id    bigint      not null references datasets (id) on delete cascade,
    observed_at   timestamptz not null,

    -- Which poller saw it. Two sources legitimately disagree about the same
    -- table (Glue's catalog lags the Iceberg metadata it describes), and the
    -- answer to "why does it say that" starts here.
    source        text        not null,

    row_count     bigint,
    size_bytes    bigint,
    columns       jsonb,
    recorded_at   timestamptz not null default now(),

    primary key (dataset_id, observed_at, source)
);

create index if not exists dataset_snapshots_dataset_idx
    on dataset_snapshots (dataset_id, observed_at desc);
