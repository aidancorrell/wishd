# Operating wish:d

Everything here is about operating the service, not using it. It exists because
the project spent seven phases proving it could *correlate* a data platform and
none proving anyone could *run* it — no backup story, no upgrade story, no way to
alert on the thing whose job is alerting.

The gaps below were found by auditing against production requirements rather
than against the roadmap, which is a closed loop: a roadmap cannot list what it
never thought to ask for.

---

## Deploying

### More than one replica

Supported, with one rule: **do not run migrations from the serving process.**

```yaml
# migrations run once, then gateways start
migrate:
  image: wishd:1.0.0rc1  # build this image locally; no registry release yet
  command: wishd migrate
  restart: "no"

gateway:
  image: wishd:1.0.0rc1  # build this image locally; no registry release yet
  command: wishd serve --host 0.0.0.0 --port 8080
  depends_on:
    migrate:
      condition: service_completed_successfully
```

`wishd migrate` takes a Postgres advisory lock, so concurrent invocations
serialise rather than race — a second one blocks, then finds the work done and
applies nothing. Verified by running three migrate containers against one
database simultaneously: no errors, no duplicate versions. That makes it safe in
a Kubernetes init container or a Nomad prestart task, where you cannot guarantee
only one copy runs.

**The bundled `docker-compose.yml` still runs one gateway**, and
`--scale gateway=3` fails on it — it publishes a fixed host port, so the second
replica cannot bind. That is deliberate: the Compose file is the single-node
install story, not a production topology. Scaling out means an orchestrator with
a load balancer in front, and the only thing wishd requires of it is the rule
above — migrate once, then start the gateways.

It was **not** safe before 0.1.0: migrations ran inside the gateway's own start
command, so two replicas booting together both applied the same migration. Some
are idempotent and survive it. Migration 019 contains a `truncate` and does not.

### What each process needs

| | Gateway | Migrate | `check` / `poll` | `notify` / `digest` |
|---|---|---|---|---|
| Postgres | yes | yes | yes | yes |
| Outbound HTTP | only for alerts | no | to warehouses/catalogs | to Slack, and dbt Cloud |
| Writable disk | artifact volume, unless using S3 | no | depends on source | no |
| Runs as | uid 10001, non-root | same | same | same |

Configure alert links in every process that sends Slack messages, including the
gateway and `pull-dbt-cloud` for dbt job alerts, and `notify` / `track` for scheduled
delivery. `WISHD_BASE_URL` supplies wishd links;
`WISHD_AIRFLOW_BASE_URL` and `WISHD_SPARK_HISTORY_URL` supply the respective
producer links. dbt Cloud links use the run URL returned by its API.
`WISHD_SNOWFLAKE_ACCOUNT_URL` (for example,
`https://app.snowflake.com/myorg/myaccount`) enables recorded-query links
in dbt failure replies and data-test alerts. Query links require an artifact query
ID and open query history; the reader can then open the SQL in Workspaces using
their own Snowflake permissions.

Alerts can also carry a row of buttons that hand the failure to a coding agent,
off by default. `WISHD_AGENT_TARGETS` (`claude-cli`, `claude-cloud`, `codex`)
turns it on and `WISHD_AGENT_SECRET` signs the handoff link — **without the
secret no button renders at all**, because the page it opens shows the failing
check's compiled SQL and an unsigned key would let anyone who could guess a table
and check name read it. `WISHD_AGENT_CWD` is an absolute path on the machine
of whoever *clicks*, not on the wishd host. Set it wherever alerts are sent,
alongside `WISHD_BASE_URL`. The Claude Code button is the `claude-cli://`
deep link itself and reaches no server; the other two address that base URL.
Slack draws a warning triangle beside any message whose buttons it cannot report
a click for, so also set `WISHD_SLACK_SIGNING_SECRET` and point the Slack
app's **Interactivity Request URL** at `https://<host>/slack/interactivity`. See
[agent handoff](agent-handoff.md). Restart long-running processes after changing
their environment. All senders and the web UI must use the same
`WISHD_DATABASE_URL`; otherwise a delivered run link can open a different
database and report that the run does not exist. The CLI keeps explicitly exported
variables ahead of `.env`, so updating the file alone does not reconfigure an
already-running gateway.

`check`, `notify` and `digest` are cron jobs, not daemons. A watcher that only
works while our process is up silently stops watching when we get OOM-killed,
and your scheduler already solves supervision better than we would.

```cron
*/15 * * * *  wishd check --schedule hourly
*/5  * * * *  wishd notify
*    * * * *  wishd track          # every minute; it is a live feed
0    8 * * *  wishd digest --hours 24
*    * * * *  wishd pull-dbt-cloud   # only if you run dbt Cloud
```

`pull-dbt-cloud` runs every minute rather than every five because it is what
feeds the live pipeline feed for dbt Cloud. dbt Cloud emits nothing, so the feed
only knows a job is running when a poll says so — at five-minute intervals a job
that takes two minutes is over before it is ever seen. If you do not run the
feed, five minutes is plenty.

`track` is cheap on a quiet minute — one query, and no Slack call at all unless a
pipeline actually moved. Run it more often than your shortest pipeline, or a run
that starts and finishes between two sweeps gets a single already-finished
message instead of a feed.

Overlapping runs are safe. `notify` and `digest` claim each notification in the
`notifications` table before sending, and the unique constraint means exactly one
of two concurrent sweeps wins the claim.

A delivery that fails is retried on the next sweep while it is under three
attempts and under an hour old, then never again — so a transient 500 does not
lose a page, and a channel that has been misconfigured for a week does not replay
the same message forever. Put `notify` on a short enough interval that a retry
still lands inside that hour.

The image sets `HEALTHCHECK`, so `docker run`, Compose and Kubernetes all get a
liveness signal without being told how to construct one. `/health` is liveness,
`/ready` checks the database, and both are unauthenticated because a load
balancer cannot present a token.

---

## Upgrading

1. **Back up first** (below). Not ceremony: one shipped migration deletes rows
   by design, and there are no down-migrations.
2. Run `wishd migrate` to completion. It is safe to run repeatedly and does
   nothing when there is nothing to do.
3. Start the new gateways.

**Migrations are immutable once applied.** Each is checksummed when it runs, and
editing an applied file afterwards is refused at startup with `MigrationDrift`
rather than silently ignored. If you need a change, add a new migration — the
old behaviour was to skip the edited file forever, leaving the database quietly
different from the repository.

A deployment that predates checksums (anything before 0.1.0) has its checksums
adopted on the first upgrade rather than being refused: there is no evidence of
drift, and blocking every existing install would be worse than trusting them.

### Rolling back

There are no down-migrations, and adding them would be worse than not: a
mechanical reverse of `truncate` cannot restore data. **Roll back by restoring
the backup**, then deploying the previous image. Plan the maintenance window
around the restore, not around the deploy.

---

## Backup and restore

Everything durable is in Postgres. There is no other state — artifacts are
content-addressed on disk or in S3, and both are re-derivable from the archive.

```bash
# back up
pg_dump --format=custom --no-owner --dbname="$WISHD_DATABASE_URL" > wishd.dump

# restore into an empty database
pg_restore --clean --if-exists --no-owner --dbname="$WISHD_DATABASE_URL" wishd.dump
```

**What matters most is `events`.** It is the raw archive every other table is
derived from: `wishd replay` rebuilds runs, datasets and write edges from it,
and `wishd resolve` rebuilds entities and lineage. If you had to choose one
table to keep, it is that one — losing the derived tables costs a replay, losing
`events` costs the history itself.

`events` is also the largest table by an order of magnitude and is partitioned by
month, so a restore of a trimmed dump is a legitimate DR strategy: keep the last
N partitions and accept a shorter history.

---

## Storage upkeep

`events` and `metric_points` are partitioned by month. Partitions must be
**provisioned ahead**. If they are not, inserts fall into the `DEFAULT` backstop
partition — nothing fails, nothing errors, and the largest table in the system
quietly stops being partitioned.

The gateway now provisions on startup and every six hours. Controls:

| Variable | Default | Meaning |
|---|---|---|
| `WISHD_UPKEEP` | `on` | Set `off` if you drive `wishd maintain` from your own scheduler |
| `WISHD_PARTITION_MONTHS_AHEAD` | `3` | How far forward to provision |
| `WISHD_RETENTION_MONTHS` | *unset* | **Dropping partitions is opt-in.** Unset means keep everything |
| `WISHD_UPKEEP_INTERVAL_SECONDS` | `21600` | Tick interval |

Retention is off by default deliberately. A tool that silently deletes last
quarter's archive because a default said three months is a tool nobody trusts
with the archive.

---

## What to alert on

`/metrics` serves Prometheus exposition. It is unauthenticated like `/health`,
because most Prometheus deployments cannot present a bearer token per target,
and it exposes counts only — no job names, no SQL, no dataset identities. Keep
it off your ingress if that trade is wrong for you; it is a separate path so
that is a one-line decision.

| Alert | Expression | Why |
|---|---|---|
| **Provisioning lapsed** | `dataspine_upkeep_healthy == 0` | Silent degradation; nothing else will tell you |
| **Shedding events** | `rate(dataspine_events_dropped_total[5m]) > 0` | Ingest is over capacity; data is being lost |
| **Queue backing up** | `dataspine_queue_depth / dataspine_queue_capacity > 0.8` | Precedes shedding |
| **Correlation gap** | `dataspine_unstitched_runs > 0` | A producer's parent handoff is broken |
| **Ingest stopped** | `rate(dataspine_events_ingested_total[15m]) == 0` | During hours you expect pipelines |
| **Processing failures** | `rate(dataspine_events_failed_total[5m]) > 0` | Malformed events, or a bug |
| **Alerts not arriving** | `dataspine_notifications_undelivered > 0` or `dataspine_alerts_undelivered > 0` | A revoked token or a lost channel; every other dashboard stays green |

A missing series means "could not be read", not zero — unreadable values are
omitted rather than reported as 0, because on a dashboard those look identical
and mean opposite things.

### Alerting on the alerting

The failure nothing else will tell you about is delivery: a revoked bot token, a
channel the bot was removed from, a route that stopped matching. Everything looks
healthy, and the channel is quiet because nothing is arriving rather than because
nothing is wrong.

Both delivery paths record every attempt, so both are queryable:

```sql
select channel, count(*) from alerts
where not delivered and created_at > now() - interval '1 day' group by channel;

select event, error, count(*) from notifications
where not delivered and created_at > now() - interval '1 day' group by event, error;
```

`error = 'no matching route'` is that query's most useful answer: the
notification was produced and dropped by the routes file, which is a config
question rather than a Slack one. `wishd slack-check --send` settles the
rest.

---

## Security posture, stated plainly

What is true today, so you can decide whether it fits:

- **Shared bearer tokens, no user accounts.** `WISHD_API_TOKENS` holds a
  comma-separated list with optional `name:` prefixes. Rotation is a redeploy.
  There is no per-user identity, no RBAC and no audit of who did what.
- **The UI session cookie is the API token**, `HttpOnly`, `SameSite=Lax`, 30
  days. `Secure` is set for HTTPS requests. **Terminate TLS at a trusted reverse
  proxy** outside local development; configure Uvicorn to trust forwarded headers
  only from that proxy (see below).
- **The gateway refuses to bind a non-loopback interface without a token**, and
  that guard is asserted in CI. `WISHD_ALLOW_INSECURE` overrides it, on purpose.
- **Request bodies are capped** at 16 MB (`WISHD_MAX_BODY_BYTES`), enforced
  against both `Content-Length` and the stream so a chunked upload cannot
  sidestep it. Multipart artifact requests have a separate 64 MiB content limit
  (`WISHD_ARTIFACT_MAX_BYTES`) plus 1 MiB for the multipart envelope.
- **The API schema requires a token** (`/api/v1/openapi.json`); the interactive
  docs are disabled.
- **The dbt Cloud webhook is the only `/api/v1` route that skips the bearer
  check.** It has to be — dbt Cloud cannot present a token — so it is declared
  outside the authenticated router, next to `/health` and `/metrics`, and
  authenticates by HMAC instead. If you add routes there, be deliberate: that
  file position is the security boundary.
- **`/handoff/<key>` is unauthenticated by bearer token and by session cookie**,
  and makes the same trade for the same reason: it is opened by a browser
  following a link in Slack, which carries neither. **The signed key is the
  authentication.** It renders the failing check's compiled SQL, so the
  signature is what stops it being an oracle for anyone who can guess a table
  and check name; with `WISHD_AGENT_SECRET` unset nothing is signed and no
  button is rendered at all. A forged signature and an unset secret both return
  404. Anyone holding the link can read that one check — which is the same
  population that can read the alert it came from. If that is not true of your
  Slack channel, leave `WISHD_AGENT_TARGETS` unset.
- **The dbt Cloud webhook authenticates by HMAC, not by bearer token**, because
  dbt Cloud cannot present one. `WISHD_DBT_CLOUD_WEBHOOK_SECRET` unset means
  the endpoint refuses everything rather than accepting anything, and a bad
  signature and a missing secret return the same 401 — telling them apart would
  hand an unauthenticated caller a configuration detail. The payload is used for
  exactly one thing, the run id to fetch; everything recorded comes from
  artifacts pulled with our own API token, so a leaked signing secret wastes our
  time rather than writing into your lineage graph. It is the only endpoint that
  makes an outbound call on an unauthenticated request — if that trade is wrong
  for you, do not create the webhook in dbt Cloud and run `pull-dbt-cloud`
  alone, which needs no inbound exposure at all.
- **No rate limiting.** Put a proxy in front if you need it — this is the right
  layer for it and we would only reimplement it worse.
- **This service stores commercially sensitive metadata**: SQL text, table
  names, error stack traces. It is a map of your data estate. There is no
  redaction, no field-level encryption and no deletion API. Treat the database
  with the same care as the warehouse it describes.

---

## Deliberately not built

Recorded so their absence is a decision rather than an oversight, in the same
form as the roadmap's other deferrals.

**Multi-tenancy and RBAC.** The threat model is a single small team reading a
shared deployment. Per-user identity is a real feature with a real schema, and
building it speculatively before anyone has asked for a specific access boundary
produces the wrong boundary. Reopen when a concrete one is named.

**Rate limiting.** Belongs at the proxy, which already does it better and can see
all traffic rather than one process's share.

**Automatic failover.** Postgres HA is a solved problem owned by whoever runs
Postgres. Point `WISHD_DATABASE_URL` at your HA endpoint.

## Reverse proxy and persistent artifacts

Build the local image with `docker build -t wishd:1.0.0rc1 .`; there is no published
registry image yet. Run the gateway behind TLS on a private upstream network. Uvicorn
recognizes `X-Forwarded-Proto` only from trusted addresses: set its standard
`FORWARDED_ALLOW_IPS` environment variable to the proxy IP or private proxy network.
Do not use `*` on an upstream reachable by untrusted clients. Ensure the proxy preserves
the external Host and scheme so redirects, origin checks and Secure cookies work.

Only expose the routes your producers and users require. Keep `/metrics` and readiness
probes on the internal network where possible; configure proxy rate/connection/time limits.
Do not log Authorization headers, cookies, raw bodies, or signed `/handoff/` URL paths.

The supplied Compose gateway persists artifacts in `/var/lib/wishd/artifacts` through a
named volume. That volume must be backed up along with Postgres. Before upgrading an old
container without this volume, copy its local artifact directory into the new volume with
ownership writable by uid 10001. Otherwise metadata will remain while file downloads fail.
For multiple gateway replicas, use shared S3 artifact storage or a shared writable filesystem;
a per-container local volume does not make artifacts available to another replica.

`WISHD_POSTGRES_PASSWORD=dataspine` is only the local demo default. Configure a random,
URL-safe password before creating a shared Postgres volume, and use matching connection
strings. Changing Compose's environment does not rotate an existing database role password.
