-- 012: column profiles, and results from DQ engines we do not own.
--
-- Two tables that look similar and exist for opposite reasons.
--
-- `column_profiles` is the first collection in this project that costs the user
-- money: it reads data rather than metadata. That is why profiling is opt-in and
-- budgeted, and why `sampled` and `scanned_rows` are stored alongside every
-- statistic -- a null rate from a 1% sample and one from a full table are not
-- the same claim, and presenting them identically would be dishonest.
--
-- `external_checks` is a decision NOT to build something. Snowflake DMFs and
-- Databricks DQ rules already run inside the warehouse, closer to the data and
-- already paid for. Reimplementing them would ask a team to run two systems that
-- disagree with each other about the same table. Reading their results is
-- strictly better, and it makes us the place you look rather than another thing
-- to look at.

create table if not exists column_profiles (
    dataset_id    bigint      not null references datasets (id) on delete cascade,
    observed_at   timestamptz not null,
    column_name   text        not null,

    row_count     bigint,
    null_rate     double precision,
    distinct_count bigint,
    uniqueness    double precision,   -- distinct over non-null
    min_value     double precision,
    max_value     double precision,
    mean_value    double precision,
    sum_value     double precision,
    stddev_value  double precision,
    zero_rate     double precision,
    negative_rate double precision,

    -- Provenance for the number above it. A statistic whose sampling basis is
    -- unrecorded cannot be compared with next week's.
    sampled       boolean     not null default false,
    scanned_rows  bigint,

    recorded_at   timestamptz not null default now(),
    primary key (dataset_id, observed_at, column_name)
);

create index if not exists column_profiles_lookup_idx
    on column_profiles (dataset_id, column_name, observed_at desc);

create table if not exists external_checks (
    id           bigserial primary key,
    source       text        not null,     -- snowflake | databricks | ...

    -- dataset_id is nullable and the raw name is kept regardless. A check on a
    -- table we have never received an event for is still evidence -- it is
    -- precisely the coverage gap worth knowing about, and dropping the row would
    -- hide it.
    dataset_id   bigint      references datasets (id) on delete set null,
    table_name   text        not null,

    check_name   text        not null,
    status       text        not null,     -- pass | fail | error
    value        double precision,
    measured_at  timestamptz not null,
    details      jsonb       not null default '{}'::jsonb,
    recorded_at  timestamptz not null default now(),

    -- Re-importing the same window must not multiply rows. The warehouse's own
    -- measurement time is the natural key, for the same reason `observed_at` is
    -- elsewhere: it describes when the check ran, not when we fetched it.
    unique (source, table_name, check_name, measured_at)
);

create index if not exists external_checks_failing_idx
    on external_checks (measured_at desc) where status = 'fail';
