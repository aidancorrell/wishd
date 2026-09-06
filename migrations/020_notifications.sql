-- 020: the notification ledger.
--
-- Monitor alerting already has its dedup decision made for it: `monitor_results`
-- records whether a status *transitioned*, and `alerts` audits the delivery. The
-- events this table covers -- a failed pipeline run, an incident opening, a
-- digest -- have no such transition column to lean on, because they are not
-- evaluations of a stateful thing. A run failing is an event that either has or
-- has not been told to a human.
--
-- So this is the ledger that answers "have we already said this?". Claiming a
-- key is the insert; `unique (event, dedup_key)` makes the claim atomic, which
-- matters because `dataspine notify` is meant to run from cron and two overlapping
-- runs must not both page about the same failure.
--
-- Deliberately claim-then-send rather than send-then-record. The failure mode of
-- claiming first is a lost notification when delivery fails; the failure mode of
-- recording after is a *repeated* notification every sweep for as long as Slack
-- is unhappy. The first is visible here (`delivered = false`, with the error);
-- the second is how a channel gets muted, taking every future alert with it.

create table if not exists notifications (
    id           bigserial primary key,

    -- run_failure | incident | digest. Monitor transitions are NOT written here:
    -- they are audited in `alerts`, keyed to the result that caused them, and
    -- duplicating that decision into a second table is how the two drift apart.
    event        text        not null,

    -- Stable across sweeps and derived from the thing itself (a root run id, an
    -- incident id, a digest window) rather than from the message text, so
    -- rewording a notification cannot resend every historical one.
    dedup_key    text        not null,

    title        text,

    -- Where it actually went, after routing. Plural because one event may fan out
    -- to several channels, and "which of them got it" is the question asked when
    -- one team saw the alert and another did not.
    destinations text[]      not null default '{}',

    delivered    boolean     not null default false,
    error        text,
    created_at   timestamptz not null default now(),

    unique (event, dedup_key)
);

create index if not exists notifications_recent_idx on notifications (created_at desc);
create index if not exists notifications_failed_idx on notifications (created_at desc)
    where not delivered;
