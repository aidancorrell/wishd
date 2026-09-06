-- 021: bounded retry for notification delivery.
--
-- Migration 020 claims a notification before sending it, and said the cost
-- plainly: a failure at the moment of delivery loses that notification for good.
-- Running it against a real Slack made that trade look worse than it did on
-- paper. The common failure is not "Slack is down for an hour", it is one 500,
-- or one 429 during a burst -- transient by the time anyone reads the ledger,
-- and it silently ate a page about a failed pipeline.
--
-- The repeat-forever failure mode 020 was avoiding comes from *unbounded* retry.
-- Bounding it on both axes keeps that protection and recovers the lost alert:
--
--   attempts   at most a few tries, so a permanently bad channel cannot post
--              the same message on every sweep for a week
--   age        past the retry window the notification is stale anyway -- a
--              pipeline that failed six hours ago is history, not news, and
--              delivering it late is worse than not delivering it
--
-- Both bounds live in `notify.py` next to the claim that enforces them, because
-- the claim is a single atomic upsert and reading the numbers apart from it
-- would invite exactly the drift this column exists to prevent.

alter table notifications add column if not exists attempts int not null default 0;

comment on column notifications.attempts is
    'Delivery attempts made. Bounds retry so a bad channel cannot repeat forever.';

-- No new index: `notifications_failed_idx` from 020 is already
-- `(created_at desc) where not delivered`, which is exactly the retry sweep's
-- question.
