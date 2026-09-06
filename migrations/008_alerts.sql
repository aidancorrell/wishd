-- 008: alert delivery audit.
--
-- One row per (transition, channel) attempt. This table exists to answer one
-- question, asked weeks later and usually in anger: "the table was stale for six
-- hours -- why did nobody get paged?"
--
-- Without it the answer lives in application logs that have long since rotated,
-- and the plausible causes (nothing transitioned, no channel configured, Slack
-- 500'd, the routing key was wrong) are indistinguishable. Each leaves a
-- different trace here.
--
-- Deliberately NOT an alert state machine. `monitor_results.transitioned`
-- already decides what is worth sending, and duplicating that decision into a
-- second stateful table is how the two drift apart.

create table if not exists alerts (
    id           bigserial primary key,
    monitor_id   bigint      references monitors (id) on delete cascade,
    result_id    bigint      references monitor_results (id) on delete set null,

    -- The status transitioned INTO. `ok` here is a recovery notification, which
    -- is a first-class alert rather than an afterthought.
    status       text        not null,
    channel      text        not null,   -- slack | pagerduty | webhook

    delivered    boolean     not null,
    error        text,
    created_at   timestamptz not null default now()
);

create index if not exists alerts_monitor_idx on alerts (monitor_id, created_at desc);
create index if not exists alerts_failed_idx  on alerts (created_at desc) where not delivered;
