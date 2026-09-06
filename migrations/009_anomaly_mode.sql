-- 009: learned bounds, and the feedback that shapes them.
--
-- `mode` selects how a monitor decides:
--
--   threshold  the user states the bound. Auditable, arms instantly, and stays
--              the default -- a tool that silently starts making statistical
--              claims about production data is harder to trust, not easier.
--   anomaly    the bound is learned from the monitor's own history.
--
-- `metric_points.feedback` is the one-click correction. Two values, and the
-- difference between them is the whole point:
--
--   expected   real, and fine. Black Friday happened. Keep the point in the
--              baseline -- "expected" means normal, and the band should learn it
--              rather than be told to ignore it.
--   anomaly    confirmed bad. Drop it from the baseline, because an incident
--              left in the training data widens the band enough to hide its own
--              recurrence. Every unlabelled incident makes the detector blinder.
--
-- Deliberately a column on metric_points rather than a feedback table. The
-- feedback is a property of the observation, one row per observation at most,
-- and a side table would need the same three-part key plus a join on every
-- baseline read.

alter table monitors add column if not exists mode text not null default 'threshold';

alter table metric_points add column if not exists feedback text;

-- The baseline read filters confirmed anomalies out, and the UI lists what has
-- been labelled. Partial, because the overwhelming majority of points carry no
-- feedback and indexing those would be indexing nothing.
create index if not exists metric_points_feedback_idx
    on metric_points (monitor_id, observed_at desc) where feedback is not null;
