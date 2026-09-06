-- 019: the CUR line-item key is (id, hour), not id.
--
-- Migration 018 made `line_item_id` the primary key, on the documented belief
-- that it uniquely identifies a line item and that repeats are restatements of
-- the same hour. **A real AWS bill falsified that on 2026-08-16.**
--
-- In a real hourly CUR, `identity/LineItemId` is stable for a given
-- resource + usage type and repeats *once per hour*: in the first delivered
-- report, 139 rows carried 27 distinct ids, one of them appearing 32 times. The
-- only columns that differ across a group are the time interval and its start
-- and end. So the id names a *series*, not a row, and the hour is what picks a
-- row out of it.
--
-- The consequence was severe and completely silent: `import_cur` upserted every
-- row onto the same key, kept the last hour to arrive, reported all 139 as
-- written, and stored 27. Four fifths of the bill vanished with a success
-- message. On this account the amounts were ~$0 so the totals barely moved,
-- which is precisely why it had to be caught by row count rather than by a
-- number looking wrong -- on a real bill it would under-report spend by ~97%
-- while still reconciling to a plausible-looking figure.
--
-- Restated hours still overwrite correctly, which is what 018 actually wanted:
-- the same (id, hour) arriving twice is the restatement, and it updates.

-- Rebuild the key. The old primary key has already lost data wherever it was
-- used, so there is nothing to preserve -- but the table is a cache of a report
-- that can always be re-imported, and re-importing is now the repair.
alter table cost_line_items drop constraint if exists cost_line_items_pkey;

-- Any rows imported under the old key are an unknown subset of the truth: we
-- cannot tell which hour each surviving row belongs to beyond what it stored.
-- Clearing is honest; keeping them would leave a silently short bill in place
-- with no way for anyone to notice. Re-run `dataspine import-cost`.
truncate table cost_line_items;

alter table cost_line_items
    add constraint cost_line_items_pkey primary key (line_item_id, period_start);
