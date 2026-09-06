-- 018: the bill, and what it bought.
--
-- Two tables, and the split between them is the point.
--
-- `cost_line_items` is what AWS actually charged: hourly rows from the Cost and
-- Usage Report, stored verbatim. It is the ground truth, and it must reconcile
-- with the AWS console -- including spend we cannot attribute to anything, which
-- is kept rather than dropped. Silently discarding unattributable rows is how a
-- cost feature loses its credibility in one meeting.
--
-- `application_costs` is what we concluded: a share of that spend, divided
-- across the applications that were running. It is derived, rebuilt by
-- `cost.attribute()`, and always less than or equal to the bill -- the remainder
-- is idle cluster time, which is real spend that no model caused.
--
-- Keeping them apart means the derivation can be improved, re-run, and argued
-- with, without ever putting our arithmetic where the invoice should be.

create table if not exists cost_line_items (
    -- CUR's own line item id. Reports are restated through the month as credits
    -- and reservations are applied, so the same hour arrives repeatedly and the
    -- later value is the true one.
    line_item_id  text primary key,

    cluster_id    text references clusters (cluster_id) on delete set null,
    service       text,
    resource_id   text,

    period_start  timestamptz not null,
    period_end    timestamptz not null,
    cost_usd      numeric(18, 6) not null default 0,

    tags          jsonb not null default '{}'::jsonb,
    imported_at   timestamptz not null default now()
);

create index if not exists cost_line_items_cluster_idx
    on cost_line_items (cluster_id, period_start);
create index if not exists cost_line_items_period_idx on cost_line_items (period_start);

create table if not exists application_costs (
    app_id        text primary key references spark_apps (app_id) on delete cascade,
    cluster_id    text references clusters (cluster_id) on delete set null,

    cost_usd      numeric(18, 6) not null,

    -- The numerator and denominator of the division, kept so the number can be
    -- defended. "Why did this model cost $34" is answered by core-seconds held
    -- versus core-seconds billed, not by trusting the total.
    core_seconds  numeric(18, 3),
    computed_at   timestamptz not null default now()
);

create index if not exists application_costs_cluster_idx on application_costs (cluster_id);
