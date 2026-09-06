-- 022: tracking a message so it can be edited, not repeated.
--
-- Everything in the ledger so far was send-once: a thing happened, a human was
-- told, done. A live pipeline feed is the opposite shape. The pipeline is the
-- same pipeline for its whole run, and what changes is its progress -- so the
-- honest rendering is one message that keeps up, not a message per step.
--
-- Posting per step would be the alert fatigue this project keeps refusing, at
-- its most obvious: a nightly stack with eight steps would put eight lines in
-- the channel to say one pipeline ran. `chat.update` exists precisely for this,
-- and it needs two things we were throwing away.
--
--   messages       Slack addresses a message by (channel id, ts). The *id*, not
--                  the "#name" a route names -- postMessage resolves the name
--                  and returns the id, and chat.update will not take the name.
--                  A list because one notification may fan out to several
--                  channels, each with its own ts.
--
--   live           Whether this notification may still change. A finished
--                  pipeline is closed and must never be touched again, or a
--                  months-old message silently rewrites itself when a run id
--                  gets reused.
--
--   content_hash   What was last rendered. Without it every sweep would call
--                  chat.update with identical content -- burning rate limit to
--                  achieve nothing, and on a busy minute crowding out an update
--                  that did matter. The message content is therefore built to
--                  be *stable while nothing happens*: step counts and a start
--                  time rather than a live-ticking elapsed, so the hash only
--                  moves when the pipeline actually did.
--
-- Incoming webhooks cannot update a message at all. The live feed therefore
-- requires a bot token, and `dataspine track` says so rather than degrading into
-- the per-step flood this table exists to avoid.

alter table notifications add column if not exists messages     jsonb   not null default '[]'::jsonb;
alter table notifications add column if not exists live         boolean not null default false;
alter table notifications add column if not exists content_hash text;

comment on column notifications.messages is
    'Posted message addresses: [{"channel": "C0123", "ts": "170.1", "route": "#x"}]';
comment on column notifications.live is
    'True while this notification may still be edited in place.';
comment on column notifications.content_hash is
    'Hash of what was last rendered, so an unchanged sweep issues no update.';

-- The tracking sweep asks "what is still open?" every minute, and on a mature
-- install that is a handful of rows in a table of many thousands.
create index if not exists notifications_live_idx on notifications (id) where live;
