-- 004: make the unstitched-runs alarm cheap.
--
-- `unstitched_runs` is the health metric that matters most -- it is how a broken
-- correlation link becomes visible -- and it renders on the run-list landing
-- page. So it runs constantly.
--
-- The original definition was an anti-join:
--
--   where r.parent_run_id is not null
--     and not exists (select 1 from runs p
--                      where p.run_id = r.parent_run_id and not p.is_placeholder)
--
-- which scans every run that has a parent: O(all runs), and measurably the
-- slowest thing in the system at only 7k runs (2.5ms of a 6.8ms health call).
--
-- The rewrite rests on an invariant the ingest path already guarantees: every
-- parent a run names gets a row, because _ensure_placeholder inserts one from
-- the parent facet. So a run is unstitched exactly when its parent row is still
-- a placeholder -- no anti-join required. Driving the query from the placeholder
-- set makes it O(placeholders), which is normally zero.
--
-- test_every_parent_reference_resolves asserts the invariant directly, so if it
-- is ever broken we find out rather than silently under-reporting.

-- Tiny partial index: placeholders are rare and transient by nature.
create index if not exists runs_placeholder_idx on runs (run_id) where is_placeholder;

create or replace view unstitched_runs as
select r.run_id,
       j.namespace as job_namespace,
       j.name      as job_name,
       r.parent_run_id,
       r.state,
       r.started_at
from runs p
join runs r on r.parent_run_id = p.run_id
join jobs j on j.id = r.job_id
where p.is_placeholder;
