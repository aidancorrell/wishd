-- 010: custom SQL monitors.
--
-- A third target kind. `dataset` and `job` name something we already store;
-- `query` carries the SQL itself, because the thing being watched has no
-- identity in our schema until the query has been run.
--
-- The constraint is replaced rather than dropped. Keeping it is what makes a
-- typo in `target_kind` fail at apply time instead of producing a monitor that
-- resolves to nothing and reports insufficient_data forever.

alter table monitors drop constraint if exists monitors_target_kind_check;

alter table monitors add constraint monitors_target_kind_check
    check (target_kind in ('dataset', 'job', 'query'));
