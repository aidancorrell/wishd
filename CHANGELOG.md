# Release candidate: 1.0.0rc1 — wish:d

- Rename the distribution and preferred CLI to `wishd`; add `WISHD_*` configuration
  with legacy compatibility. Keep storage identities, migration checksums and metrics stable.
- Bundle migrations in wheels; exercise sdist/wheel installation outside the checkout.
- Bound unsigned webhook/form/upload bodies, validate login redirects and form credentials,
  secure HTTPS cookies, reject cross-origin browser submissions and disable metadata caching.
- Persist container artifacts; include AWS support in the image and exclude private build inputs.
- Remove a captured credential from the current Spark fixture and stop tracking generated logs.
  Writable local/remote history has since been cleaned and the exposed local token rotated.
  Retained GitHub PR/cache references still require Support review before publication.
- Make dependency audits blocking, pin CI actions, add secret scans and a Python test matrix.
- Add a concise quickstart, configuration/contribution/security guidance and release gates.

The frontend redesign is merged; final verification and publication remain pending.
See [release checklist](docs/releasing.md).

---

# Changelog

Notable changes per release. Dates are the release date, not the merge date.

## Unreleased

### Added

- **dbt Cloud CLI invocations.** `wishd ingest-dbt-run` records a finished dbt
  invocation from the `target/` directory it leaves behind, synthesising the run
  tree the way the dbt Cloud reader does. It closes the one dbt path nothing
  reported: the Cloud CLI runs on dbt Cloud, so `dbt-ol` never sees it, and it
  never appears in the Admin API's run list, so `pull-dbt-cloud` cannot find it.
  Keyed off dbt's `invocation_id`, so a second pass rewrites one run. Not for a
  stack already running `dbt-ol`, which would then record every run twice.

- **Slack, past one webhook.** `DATASPINE_SLACK_BOT_TOKEN` posts through
  `chat.postMessage`, so one credential reaches any channel — the only shape
  that can honour a routing file. Routing itself is committed YAML
  (`DATASPINE_SLACK_ROUTES`), matching on event, status, a glob over the monitor
  or job name, and `integration` (which producers reported the failure), first
  match wins. A channel name is not a secret and
  belongs in review; the token is and stays in the environment.
  `DATASPINE_SLACK_WEBHOOK` keeps working unchanged.
- **One Slack message per dbt job, failures threaded under it.** A job that
  failed posts a summary — how many models, how many tests, which command, how
  long — and one thread reply per failure. The channel keeps one line per job;
  each failure is its own message somebody can react to, claim or answer under.
  Warnings ride in the thread but never count toward the total on the front.
  Models sort before tests, because a model that did not build is *why* its
  tests did not run. A clean job says nothing. Posted the moment the run is
  ingested, so a webhook that has already fetched and parsed does not then wait
  for a poller. Thread replies are capped, because Slack allows roughly one
  message per second per channel and a wide `dbt build` would otherwise spend a
  minute of rate limit that a different alert queues behind.
- **A dbt failure now produces exactly one story.** `run_failure` stands down for
  any root a `dbt_job` message already covers, and the flat `data_test` alert is
  reserved for sources that have no invocation to thread under — Snowflake DMFs,
  Databricks rules. Two notifications for one event is how a reader learns the
  second one is never worth opening.
- **dbt tests reach Slack, from dbt-core and dbt Cloud alike.**
  `run_results.json` is the same file in both worlds — dbt-core writes it to
  `target/`, dbt Cloud serves it from the Admin API — so one reader
  (`dbt_artifacts.py`) serves both. dbt-core needs no new integration at all:
  `push-artifacts` has been uploading it since Phase 02 and it was sitting
  unread. Failures notify on *transition*, the same rule monitor alerting uses,
  so a test failing hourly for three days is one message rather than 72.
  Recoveries are delivered. A test with `severity: warn` is recorded and never
  notified in either direction — its author said in writing that they did not
  want waking.
- **The live feed now works for dbt Cloud.** Each poll records runs still in
  flight as well as finished ones, from run metadata alone — artifacts only
  exist once a run ends, so without this a dbt Cloud job could never appear as
  running and the feed only ever received a finished job. Both halves key off
  dbt Cloud's own run id rather than dbt's `invocation_id` (which lives inside
  the artifacts, and so does not exist yet while a job is running), so the row
  the feed has been showing becomes the finished tree rather than a second run
  beside it.
- **`dataspine dbt-cloud-check`** — the dbt Cloud twin of `slack-check`: host,
  account, namespace, the jobs it can see, and whether webhooks are reachable.
  On a rejected request it names the **host** before the token, because dbt
  Cloud is multi-cell and a wrong host returns a 401 indistinguishable from a
  bad credential — the failure that cost the most time building this.
- **dbt Cloud becomes an ordinary producer (`dataspine pull-dbt-cloud`, plus an
  HMAC-verified webhook).** dbt Cloud emits no OpenLineage, so dataspine
  synthesises the run tree from `run_results.json` + `manifest.json` — what
  `dbt-ol` does client-side, done server-side, and named identically so a shop
  running both sees one vocabulary. dbt Cloud on Snowflake, Databricks or
  BigQuery therefore gets the run timeline, the live feed, run failures, lineage
  and tests with nothing downstream special-cased. Webhook and poll can both
  run: run ids derive from dbt's own `invocation_id`, so ingesting twice writes
  the same rows twice and changes nothing. The webhook body is used for exactly
  one thing — which run to fetch — because a valid signature proves someone
  holds the secret, not that the payload is true.
- **A live pipeline feed (`dataspine track`).** One Slack message per pipeline
  execution, posted when the feed first sees it and *edited in place* as steps
  complete — eight steps is one message, not eight, and a sweep where nothing
  moved issues no edit at all. The message content is deliberately stable while
  nothing happens (step counts and a start time, not a ticking elapsed), so the
  content hash only moves when the pipeline did. A run still going quiet is
  surfaced rather than called healthy: OpenLineage has no heartbeat, so a cluster
  that dies mid-run would otherwise sit at "running" forever. Requires
  `DATASPINE_SLACK_BOT_TOKEN` — an incoming webhook cannot edit a message it
  already sent, and `track` refuses that transport rather than degrading into the
  per-step flood it exists to avoid.
- **Run failures are notifications.** `dataspine notify`, from cron next to
  `check`, tells Slack about failed pipeline runs and newly opened incidents.
  One message per pipeline rather than per task — a failed dbt model fails the
  Airflow task above it and the Spark job below it, and the message names the
  deepest failure that actually carried an error. A root run that has since
  completed was a retry that worked, and is not a message at all.
- **`dataspine digest`** — a summary of the window, including the quiet one. A
  channel that only speaks when something is wrong gives nobody a way to tell
  "quiet" from "broken and silent". Its failure count is derived the same way
  the alerts derive theirs, so the digest cannot report zero on a morning when
  three alerts went out.
- **`dataspine slack-check`** — what is configured, what each route would catch,
  and `--send` to prove the bot can actually reach the channel.
- **`notifications`** — the send-once ledger, claimed before sending, with a
  bounded retry so one bad response does not eat a page. A failed delivery is
  re-attempted on the next sweep while it is under three attempts and under an
  hour old, and never again after that: the alert-fatigue failure comes from
  *unbounded* retry, and a pipeline that failed and was fixed hours ago is
  history rather than news. "Why did nobody get told?" is answerable weeks
  later, including the answer "no route matched".
- **Slack rate limits are waited out.** `chat.postMessage` allows roughly one
  message per second per channel, so a sweep fanning out to several channels is
  the ordinary case that trips it. A 429 is retried once after `Retry-After`;
  anything longer than ten seconds is failed immediately rather than blocking
  every remaining channel in the sweep to rescue one message.
- **`dataspine_alerts_undelivered` and `dataspine_notifications_undelivered`** —
  delivery is the failure nothing else reports. A revoked token or a channel the
  bot was removed from leaves every dashboard green while the channel is quiet
  because nothing is arriving, not because nothing is wrong.
- **`DATASPINE_BASE_URL`** — when set, every notification links back to the run,
  monitor or incident it is about. Unset means no link rather than a guessed
  one, for the reason `links.py` states.

### Changed

- **The web UI was rebuilt from an imported design.** Navigation moves from a
  seven-link top bar to a left rail grouped **Operate / Data / Govern**, which is
  the structure the seven links could not carry. Every page now opens the same
  way — where you are, what this is, and one sentence of why it is shaped that
  way — because several of these views answer a question by deliberately *not*
  showing something, and that needs somewhere to be said. Titles are set in
  Newsreader; the serif marks what a person reads, as the mono marks what a
  machine emitted. Pills, dashed empty-state boxes and rounded cards are gone:
  an empty state is a sentence, not a container, and "nothing running" is the
  good case most of the time.
- **Status reads as a word, not a filled chip.** In a table of a dozen checks the
  chips tiled into a column of coloured blocks louder than anything else on the
  page, and most of them said `ok`. The wash moved to the breaching row, where
  it marks the one line that is actually wrong.
- **Completed is green again, on dots and bars only.** `--ok-fill` is a
  teal-leaning sage (`#56967a` light, `#7cbb9d` dark); `--ok`, the text role,
  stays neutral so a page of finished work still reads as calm prose. The design
  canvas's own sage (`#3f6b45`) could not be used as drawn — it collapses against
  crit to ΔE 0.052 under protanopia, below the line where two states become one
  colour and worse than the green this project had already removed. Rotating the
  hue from ~132 to ~152 buys the separation back at ΔE 0.105 while still reading
  as sage, and that lean is load-bearing.
- **The imported `--rule` was not taken either**: `#d4d4d9` missed the measured
  1.5:1 floor by 0.02, so it ships two steps darker. The full contrast and
  colour-deficiency contract was re-measured against the served stylesheet.
- **IBM Plex Sans was dropped** in favour of Hanken Grotesk, and its files
  removed rather than left in the tree — an unreferenced face still ships to
  everyone who clones the repo.

### Fixed

- **`pull-dbt-cloud` silently lost older runs.** It fetched a single page of 50
  and stopped, so an account with more runs than that inside the window kept
  receiving recent ones while quietly dropping the rest — the worst way to lose
  data, because nothing looks wrong. It now pages to the window's edge, bounded.
- **The dbt Cloud webhook could never work in a real deployment.** It was
  declared on the `/api/v1` router, which carries `Depends(require_token)` — and
  dbt Cloud cannot present a bearer token. Any deployment with
  `DATASPINE_API_TOKENS` set (i.e. every real one) returned 401 from the auth
  layer before the handler ran. Every test passed because the test fixture
  leaves that variable unset, so the dependency never fired. Found by pointing
  real dbt Cloud at a real tunnel; the route now sits beside `/health` and
  `/metrics` and authenticates by HMAC alone, which is what its docstring
  always claimed.
- **Every dbt Cloud job in a project was the same job.** The synthesised root run
  was named `<project>.run` regardless of which job produced it, so a nightly
  build and an hourly incremental collapsed into one identity — job-level
  monitors averaging unrelated workloads, one row in the live feed for all of
  them, and no way for a route to tell them apart. The job's real name is now
  fetched and used.
- **dbt relation names are quoted, and nothing matched them.** dbt hands back
  `"db"."schema"."table"` exactly as the adapter renders it, and every table
  name in dataspine is matched on its final segment — which reduced that to
  `table"`, quote attached, matching nothing any other producer reports. Found
  against real dbt output, not a handwritten fixture.
- **External check tables now resolve case-insensitively.** Snowflake reports
  `FCT_ORDERS` and everything else reports `fct_orders`; an unquoted SQL
  identifier means the same table either way. Without this every dbt Cloud test
  on Snowflake attached to no dataset at all.
- **A Slack error dressed as an HTTP 200 was recorded as a delivery.**
  `chat.postMessage` answers 200 with `{"ok": false, "error":
  "channel_not_found"}`, and an unknown channel, a revoked token and a missing
  scope all arrive that way. Slack responses are now checked in the body, not
  only the status line — the alert audit was otherwise confidently wrong about
  the one case it exists for.

## 0.1.0 — 2026-08-23

The first version with an operability story. Everything before this was built to
be *correct*; this is the first release built to be *run*.

The gaps below came from auditing against production requirements rather than
against the roadmap — a roadmap cannot list what it never thought to ask for.

### Fixed — defects, not gaps

- **Migrations were unsafe with more than one replica.** They ran inside the
  gateway's start command, so two booting together both applied the same
  migration; one shipped migration contains a `truncate`. `dataspine migrate`
  now takes a Postgres advisory lock, and Compose runs it as a separate one-shot
  service the gateway waits on. Verified with three concurrent migrate
  containers against one database.
- **Partition provisioning depended on a cron that was never shipped.**
  `ensure_partitions` provisions months ahead and its own docstring assumed an
  hourly cron; nothing called it. A deployment left alone eventually wrote
  everything into the `DEFAULT` backstop partition — silently, because nothing
  fails when that happens. The gateway now provisions on startup and every six
  hours, and the remaining headroom is reported in health and metrics.
- **Editing an applied migration was silent.** Migrations are checksummed when
  applied; changing one afterwards is refused with `MigrationDrift` instead of
  skipped forever. Deployments predating checksums are adopted, not blocked.

### Added

- **`/metrics`** — Prometheus exposition, no new dependency: ingest throughput,
  queue depth and shed count, unstitched runs, partition headroom, breach count.
  Unreadable values are omitted rather than reported as `0`, because on a
  dashboard those look identical and mean opposite things.
- **`docs/operations.md`** — deploying, upgrading, backup and restore, storage
  upkeep, what to alert on, and the security posture stated plainly.
- **Storage upkeep controls** — `DATASPINE_UPKEEP`,
  `DATASPINE_RETENTION_MONTHS`, `DATASPINE_PARTITION_MONTHS_AHEAD`. Retention is
  opt-in: a tool that silently deletes last quarter's archive because a default
  said three months is a tool nobody trusts with the archive.
- **Upgrade tests.** Migrations had no test against a populated database — the
  one operation that can destroy an operator's data was the one with no
  coverage.

### Security

- **The API schema was public.** FastAPI mounts `/openapi.json` and `/docs` on
  the app, not the router, so the token dependency never applied. The docs are
  disabled and the schema is served authenticated from `/api/v1/openapi.json`.
- **Request bodies had no ceiling** on the endpoint deliberately reachable by
  every producer in a data platform. Capped at 16 MB
  (`DATASPINE_MAX_BODY_BYTES`), enforced against `Content-Length` *and* the
  stream, since a chunked upload declares no length.
- **The container ran as root** on an unpinned base with no healthcheck. Now
  multi-stage, non-root (uid 10001), pinned by digest, with a `HEALTHCHECK`.
- **Dependencies were unbounded `>=` with no lockfile.** `constraints.txt` pins
  the image build; Dependabot and `pip-audit` watch it.

### Known limitations

Stated because their absence should be a decision, not a discovery: no
multi-tenancy or RBAC, no rate limiting (belongs at the proxy), no redaction of
the SQL and stack traces this service stores. See `docs/operations.md`.
