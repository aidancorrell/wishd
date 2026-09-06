-- 005: durable artifact capture.
--
-- dbt writes manifest.json / run_results.json into target/ at the END of a run,
-- on a machine that may be gone a minute later. On ephemeral EMR nodes a run
-- that dies halfway leaves nothing -- and those are precisely the runs worth
-- inspecting. So artifacts get pushed somewhere we control.
--
-- Two tables, because content and reference are different things:
--
--   artifact_blobs  content, addressed by sha256
--   artifacts       a (run, name) pointing at a blob
--
-- Content addressing is not premature cleverness here. A dbt manifest is
-- hundreds of KB and barely changes between consecutive runs of the same
-- project; one blob per upload would multiply that by every run forever, and
-- storage cost is the reason people turn retention features off.

create table if not exists artifact_blobs (
    sha256      text primary key,
    size_bytes  bigint not null,
    storage_uri text   not null,
    created_at  timestamptz not null default now()
);

create table if not exists artifacts (
    id           bigserial primary key,
    -- Deliberately NOT a foreign key to runs. dbt can finish and upload before
    -- the orchestrator's COMPLETE event lands, and arrival order is not ours to
    -- control -- the same reasoning that keeps parent_run_id unconstrained.
    run_id       uuid   not null,
    name         text   not null,
    sha256       text   not null references artifact_blobs (sha256),
    content_type text   not null default 'application/octet-stream',
    created_at   timestamptz not null default now(),
    updated_at   timestamptz not null default now(),
    unique (run_id, name)
);

create index if not exists artifacts_run_idx on artifacts (run_id);
create index if not exists artifacts_sha_idx on artifacts (sha256);
