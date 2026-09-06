-- 002: widen run_summary, and index the access pattern the UI actually uses.
--
-- run_summary was written for a CLI that only ever printed a tree. The web UI
-- and the job-history endpoint both need to get from a run back to its job, and
-- the detail view needs the stack trace and nominal window that were previously
-- only reachable by querying `runs` directly.

drop view if exists run_summary;

create view run_summary as
select r.run_id,
       r.job_id,
       j.namespace as job_namespace,
       j.name      as job_name,
       j.integration,
       j.job_type,
       r.state,
       r.parent_run_id,
       r.root_run_id,
       r.depth,
       r.started_at,
       r.ended_at,
       extract(epoch from (r.ended_at - r.started_at)) * 1000 as duration_ms,
       r.nominal_start_time,
       r.nominal_end_time,
       r.error_message,
       r.error_stacktrace,
       r.producer,
       r.event_count,
       r.first_event_at,
       r.last_event_at,
       r.is_placeholder
from runs r
join jobs j on j.id = r.job_id;

-- Job history ("show me the last 50 runs of this model") is the hot read on the
-- run detail page. Without this it is a seq scan over every run of every job.
create index if not exists runs_job_started_idx on runs (job_id, started_at desc nulls last);
