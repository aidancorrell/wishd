# Integration and feature guide

Start with the [quickstart](../README.md). This guide covers the optional integrations.

## The problem

dbt Cloud on Snowflake feels observable. dbt-core on EMR with SparkSQL under Airflow does
not — not because telemetry is missing, but because it lands in four places that don't know
about each other:

| Layer | Signal that exists | Where it dies |
|---|---|---|
| Airflow | Task state, retries, OpenLineage events, OTel traces | Airflow's UI |
| dbt-core | `manifest.json`, `run_results.json`, structured JSON logs | Overwritten in `target/`, on an ephemeral node |
| Spark/EMR | Event logs, `SparkListener`, executor metrics | History Server, if the cluster still exists |
| AWS | Instance-seconds, Spot pricing, CUR line items | Cost Explorer, at cluster granularity |

**Nothing correlates them by a shared run identity.** wish:d builds that correlation
first; everything else is a query over it.

## What works today

One OpenLineage endpoint, one Postgres database, and a correlator that assembles this from
events arriving in arbitrary order:

```
analytics_daily  AIRFLOW  FAILED  10m11s
└── analytics_daily.dbt_run_marts  AIRFLOW  FAILED  10m08s
    └── analytics.run  DBT  FAILED  10m01s
        ├── analytics.model.analytics.fct_orders  DBT  COMPLETED  5m50s
        │   └── dbt_spark_analytics.fct_orders  SPARK  COMPLETED  5m48s
        │       └── ...execute_insert_into_hadoop_fs_relation_command  SPARK  COMPLETED  5m45s
        └── analytics.model.analytics.fct_order_items  DBT  FAILED  4m05s
            ├── org.apache.spark.SparkException: Job aborted due to stage failure:
            │   Task 137 in stage 12.0 failed 4 times... Container killed by YARN
            └── dbt_spark_analytics.fct_order_items  SPARK  FAILED  4m03s
                └── ...execute_insert_into_hadoop_fs_relation_command  SPARK  FAILED  4m00s

datasets touched
 output  s3://acme-lakehouse/warehouse/marts/fct_orders       1,189,034
 input   s3://acme-lakehouse/warehouse/staging/stg_orders             –
```

That tree is reachable from **any** run id in it — including the Spark SQL execution id,
which is the one you'd actually have in hand when something breaks.

## Monitors

Monitors are YAML in your repo, reconciled with `wishd apply`. Every one of them reads
only what the gateway already collects from Airflow, dbt and Spark — **no warehouse
credentials, no connection string, no scan.**

```yaml
# monitors/marts.yml
monitors:
  - name: fct_orders_freshness
    kind: freshness
    dataset: fct_orders
    max_age_minutes: 90

  - name: fct_orders_schema      # deterministic: no threshold, no training window
    kind: schema_drift
    dataset: fct_orders

  - name: nightly_dbt_reliability
    kind: job_failure_rate
    job: dbt-run-analytics
    max_rate: 0.2
    window_hours: 24
```

```bash
wishd apply monitors/          # reconcile, and backfill history so they arm immediately
wishd check --schedule hourly  # from cron, or POST /api/v1/monitors/check
wishd monitors --resolve       # what each target actually matched
```

Alerts go to whichever channels are configured, and only on a **status change** — a table
broken since 02:00 is one alert, not one an hour. Recoveries are delivered too.

```bash
export WISHD_SLACK_BOT_TOKEN=xoxb-...          # a Slack app with chat:write
export WISHD_PAGERDUTY_ROUTING_KEY=...
export WISHD_ALERT_WEBHOOK=https://...
```

### Slack

Slack gets more than monitor transitions, because a monitor breach is not the thing most
teams find out about last. A **failed pipeline run** is, and wish:d already holds the
correlated tree — so it can say which task failed and what it said, rather than that a DAG
went red.

```bash
wishd track                   # live feed of what is running — from cron, every minute
wishd notify                  # failed runs and new incidents — from cron, next to `check`
wishd digest --hours 24       # the morning summary, including the quiet one
wishd slack-check --send      # what is configured, and does it actually reach the channel
```

`wishd track` is a live feed without the flood that usually means. A pipeline
gets **one** message: posted when the feed first sees it, edited in place as steps
complete, edited once more to say how it ended. Eight steps is one message, not
eight — and a sweep where nothing moved issues no edit at all. It needs a bot
token, because an incoming webhook cannot edit a message it already sent, and it
refuses that transport rather than degrading into the per-step flood it exists to
avoid.

Six things reach Slack: the **live pipeline feed**, **monitor transitions**, **dbt jobs**
(one message, failures threaded under it), **run failures**, **incidents** (the cause with its blast radius, not the fifty
tables downstream of it) and the **digest**. Apart from the feed, each is
sent once — a pipeline is one message however many of its tasks failed, and a failure a
retry then fixed is not a message at all. A delivery that fails is re-attempted on the next
sweep for up to three tries within an hour, so one bad response from Slack does not eat a
page. Every decision is recorded in `notifications`, so "why did nobody get told?" stays
answerable weeks later.

Routing is a file you commit, because a channel name is not a secret and "why does finance
get paged for this?" is a question that deserves a diff. Credentials stay in the
environment.

```yaml
# monitors/slack.yml — WISHD_SLACK_ROUTES=monitors/slack.yml
routes:                              # read top-down; the first match wins
  - match: {monitor: "finance_*"}    # a team's tables, wherever the news comes from
    channel: "#finance-data"
  - match: {event: [monitor, incident]}      # is the data right?
    channel: "#data-alerts"
  - match: {event: [pipeline, digest]}       # is the machinery running?
    channel: "#data-pipelines"
  - channel: "#data-alerts"          # catch-all; delete it to make the rest silence
```

Two streams is the default shape because the two questions have different
audiences: the platform on-call wants failed pipelines, the table owners want
breaches. Routes match on `event`, `status`, `monitor`, `job` and `integration`
(`AIRFLOW` / `DBT` / `SPARK` — which producers reported the failure). Note that on
a dbt-on-Spark stack essentially every failure reports all three, so `integration`
separates a hand-written Spark job from an orchestration failure — it does not
separate "a dbt model broke" from "the pipeline broke", because those are one
event.

Set `WISHD_BASE_URL` and every message links back to the run, monitor or incident it is
about. Pipeline messages and dbt job alerts also link to the producer when its run metadata
provides a URL: dbt Cloud's run page, Spark UI, or Airflow. Airflow requires
`WISHD_AIRFLOW_BASE_URL`; Spark History requires `WISHD_SPARK_HISTORY_URL`.
External links work independently of `WISHD_BASE_URL`; missing configuration omits
the corresponding link.

For Snowflake investigation links, set `WISHD_SNOWFLAKE_ACCOUNT_URL` to your Snowsight
account URL, for example `https://app.snowflake.com/myorg/myaccount`. dbt failure thread
replies and data-test alerts include, when dbt's `adapter_response`
contains a `query_id`, a **Snowflake query** link. This opens query history; use **Open in
Workspaces** there to load the recorded SQL into an editor. It does not create a worksheet
directly. Your Snowflake login and query permissions still apply. SQL text stays out of
Slack messages. Without a query ID, the query link is omitted. dbt Cloud job titles are
clickable when the run metadata provides a URL. Parent alerts include a failure or warning
emoji, and Slack link/media previews are disabled to keep messages compact.

`WISHD_SLACK_WEBHOOK` still works and needs no Slack app, but an incoming webhook is
bound to one channel chosen in Slack: routing can then only decide *whether* to send, never
where. That combination looks like it is working — messages arrive, just never where the
file says — so `wishd slack-check` calls it out by name rather than leaving three teams
to wonder why their channel is quiet.

### dbt tests, and dbt Cloud

The monitors watch freshness, volume, schema and column statistics. A `not_null` on a column
nobody thought to profile is invisible to all of them — and it is usually the assertion the
analytics team actually wrote down. dbt records those in `run_results.json`, and wishd
reads them.

A dbt job that failed gets **one message, with each failure as a thread reply** — so the
channel stays one line per job, and each failure is its own message somebody can react to,
claim or answer under.

```
❌  Nightly Production — 2 failures
    1 model(s) failed · 1 test(s) failed · 1 warning(s)
    dbt build · 2 model(s) · 4 test(s) · 6m12s
 💬 3 replies
    ├─ ❌  model  fct_order_items
    │      Database Error: column "customer_id" does not exist
    ├─ ❌  test  not_null_fct_order_items_order_id
    │      1 failing row(s) in `order_id` on `fct_order_items`
    └─ ⚠️  test  unique_fct_order_items_item_id   severity: warn
```

A clean job says nothing — a green line from each of several jobs is a channel nobody reads
by the second week, and the live feed is where "everything ran" belongs. Warnings ride in the
thread but never count toward the failure total on the front, because their authors said in
writing they were not worth waking anyone. Models sort before tests: a model that did not
build is *why* its tests did not run.

Because most teams run several jobs against one project, the job's real name becomes the run's
name (`analytics.Nightly Production`), so job monitors, the live feed and `job:` routes can all
tell them apart.

**dbt-core needs no new integration.** `wishd push-artifacts` has been uploading
`run_results.json` and `manifest.json` since Phase 02; they are now parsed on arrival, and get
the same job message.

```bash
dbt build && wishd push-artifacts $WISHD_RUN_ID   # tests recorded, failures alerted
```

**dbt Cloud serves the identical file** from its Admin API, so the same reader handles it —
and because dbt Cloud emits no OpenLineage, wish:d builds the run tree *from* those
artifacts. The result is an ordinary run tree with an ordinary failing test in it:

```
analytics.run  DBT  FAILED
├── analytics.model.analytics.fct_orders  DBT  COMPLETED
├── analytics.test.analytics.not_null_fct_order_items_order_id  DBT  FAILED
│   └── Got 1 result, configured to fail if != 0
└── ...
```

So dbt Cloud on Snowflake, Databricks or BigQuery gets the run timeline, the live pipeline
feed, run failures, lineage and tests — with nothing downstream special-cased.

```bash
export WISHD_DBT_CLOUD_TOKEN=dbtc_...
export WISHD_DBT_CLOUD_ACCOUNT=12345
export WISHD_DBT_CLOUD_HOST=abc123.us1.dbt.com   # see below — this one bites
wishd dbt-cloud-check           # what is configured, and what it can see
wishd pull-dbt-cloud            # from cron, every minute
```

**Set the host.** dbt Cloud is multi-cell: newer accounts live at something like
`abc123.us1.dbt.com`, not `cloud.getdbt.com`, and the wrong one returns a bare 401 that
looks exactly like a bad token. Take the hostname from the URL you sign in with.
`wishd dbt-cloud-check` names this first when a request is rejected, because it is the
more common cause and the cheaper one to rule out.

Each poll records two things: runs that **finished** (their full tree, their tests) and runs
still **in flight**. The second is what lets the live feed show a dbt Cloud job while it is
running — artifacts only exist once a run ends, so without it the feed would only ever
receive a finished job. Both halves key off dbt Cloud's own run id, so the row the feed has
been showing becomes the finished tree rather than a second run beside it.

A webhook (`POST /api/v1/dbt-cloud/webhook`, HMAC-verified) does the same within seconds of a
job finishing. Run both: ingest is idempotent, so the overlap costs nothing and the poll
catches whatever the webhook dropped.

**The dbt Cloud CLI is neither of those cases.** It runs on dbt Cloud's infrastructure, so
`dbt-ol` never sees it, and its invocations are absent from the Admin API's run list, which
holds scheduled and API-triggered job runs only — so `pull-dbt-cloud` cannot find one however
long it looks. What it does leave behind is `target/`, downloaded when the invocation
finishes, and that is the same `run_results.json` the Admin API serves:

```bash
dbt run && dbt test                     # dbt Cloud CLI
wishd ingest-dbt-run --directory target/ --job-name "Hourly Run and Test" \
  --dbt-cloud-url https://abc123.us1.dbt.com/deploy/<account>/projects/<project>/
```

`--dbt-cloud-url` is stated by you because nothing else can state it. A Cloud CLI
invocation has no address of its own — it is missing from the run list, no invocations
endpoint exists to ask, and the artifacts name neither the account nor the project — so the
project page is usually what you want the run's **dbt Cloud** button to open. Left unset,
the run carries no link rather than a guessed one that 404s.

That synthesises the run tree the way the dbt Cloud reader does, keyed off dbt's own
`invocation_id`, so running it twice rewrites one run rather than making two. Do not run it on
a stack that already emits OpenLineage through `dbt-ol`: that stack reports its own tree, and
this would record a second copy of every run beside it. Use `push-artifacts` there instead.

Creating the subscription needs a **service token**, which some dbt Cloud plans do not offer —
on those, every webhook endpoint returns 404 and polling is the supported path.
`wishd dbt-cloud-check` reports which case you are in. Set
`WISHD_DBT_CLOUD_WEBHOOK_SECRET` to the secret dbt Cloud returns when the subscription is
created; unset, the endpoint refuses every request rather than accepting any.

> **⚠ The signature format is unverified against real dbt Cloud.** The endpoint has been
> exercised end to end over the public internet — HMAC checked, artifacts fetched, alert
> delivered — but with a request signed by us, because no plan we have access to can create a
> subscription. If dbt Cloud's encoding differs from `dbt_cloud.verify()`, the first real
> webhook will 401 and `notifications` will show nothing. Polling is unaffected.

A test with `severity: warn` is recorded and **never** notified. Its author said in writing
that they did not want waking, and delivering it anyway is how a team mutes the channel that
also carries the errors.

### Watching tables nothing of yours writes

Source tables loaded by Fivetran or dropped by a vendor emit no lineage, and their silent
staleness is what breaks a pipeline at 02:00. Pollers read catalog metadata — never table data
— and store it in the same shape a run event produces, so the monitors above work on them
unchanged.

```yaml
# sources.yml — credentials come from the environment, never the committed file
sources:
  - name: warehouse
    type: postgres          # or snowflake | databricks | bigquery | redshift
    dsn: ${WAREHOUSE_DSN}
    namespace: pg://warehouse
  - name: lake
    type: delta             # or iceberg
    path: /mnt/lake/events
    namespace: s3://lake
    dataset: raw_events
```

```bash
wishd poll sources.yml
```

Column statistics are the one thing that needs a real scan, so profiling is opt-in and
budgeted. Above the budget it samples and records that it did — a null rate from a 1% sample
is not the same claim as one from a full table.

```bash
wishd profile fct_orders --max-rows 1000000   # 0 refuses to scan at all
```

Snowflake DMFs and Databricks DQ rules already run inside your warehouse. Forward their
results rather than running them twice: `POST /api/v1/dq/snowflake`.

Kinds: `freshness`, `row_count`, `schema_drift`, `column_stats`, `custom_sql`,
`job_duration`, `job_failure_rate`, `queue_delay`, `job_retries`, `spark_spill`.

Set `mode: anomaly` on any numeric kind and the bound is learned from the monitor's own
history instead of stated — robust to outliers, and seasonal, so weekend volumes are not an
incident every Saturday. Static thresholds stay the default: a stated number is auditable and
arms instantly.

Three things worth knowing, all of which come from what real producers actually emit:

- **A new monitor arms on day one.** `apply` builds its metric history out of the run archive
  rather than waiting a week to collect a baseline it already has the data for.
- **Targets match on the final name segment,** because one table arrives under several
  identities — dbt reports `analytics.fct_orders`, the Spark job underneath it reports
  `/warehouse/fct_orders`, and the physical files are `s3://…/marts/fct_orders`. Writes are
  then deduplicated by run tree, so those three reports of one nightly build count as one
  write and not three. Run `wishd monitors --resolve` to see exactly what matched.
- **Row counts and schemas come from Spark only.** dbt sends no `outputStatistics` and no
  `schema` facet, so on a dbt-only stack those monitors report `insufficient_data` rather
  than a misleading pass.

## Lineage, catalog and incidents

Everything above produces signals. Lineage decides which of them are the same problem.

```bash
wishd resolve              # rebuild identity, the graph and the search index
wishd lineage fct_orders --depth 3 --columns
wishd incidents            # the cause, and what it took down with it
```

Three things worth knowing:

- **One table, one node.** dbt calls it `analytics.fct_orders`, Spark calls it
  `/warehouse/fct_orders`, and the files are `s3://…/marts/fct_orders`. They merge into one
  entity when a run tree shows them co-written — evidence no single producer can supply. A
  shared name alone is never enough, and where the resolver declines it says so on the catalog
  entry rather than leaving you with a graph that quietly disagrees with your monitors.
- **Column lineage comes from the producer where possible.** openlineage-spark emits it from
  Spark's resolved logical plan, complete with transformation type and a masking flag. SQLGlot
  fills in for dbt, which sends SQL and no facet — and declines rather than guessing on a
  `select *` or an ambiguous unqualified column.
- **One incident, one alert.** A late source table breaching fifty downstream monitors sends
  one page naming the cause and how far it reached. The other forty-nine are recorded as the
  blast radius. Alert fatigue is what kills these tools in month three.

The lineage graph renders as server-side SVG — no build step, no framework, no JavaScript, the
same as every other page (ADR-002 permitted one vendored graph library here; it turned out not
to be needed).

## Cost

What a pipeline cost, and which model to blame.

```bash
wishd import-cost cur.csv --tag-key team   # AWS Cost and Usage Report
wishd costs                                # cost per dbt model
```

Three divisions get from an AWS bill to a dbt model:

1. **Bill → cluster**, by the tags on your clusters — CUR rows carry `resourceTags/…`, never
   cluster ids.
2. **Cluster → application**, by core-seconds. Those come free from the Spark event log, which
   records when each executor arrived and left, so an autoscaled job is charged for what it
   actually held.
3. **Application → model**, by query attribution. The Spark application is a *sibling* of the
   dbt models — one long-lived session serves many — so its cost is split across the models by
   the work each drove.

Two things it refuses to do. **Idle cluster time is reported separately**, never spread across
models: it is real spend that no model caused, and it is usually the largest thing a team can
actually act on. And **an unpriced run says so** rather than showing $0.

Databricks (`system.billing.usage`) and Dataproc (BigQuery billing export) normalise into the
same table, so attribution never learns there is more than one cloud.

Spend regression alerting needs no new machinery — it is a monitor:

```yaml
  - name: fct_orders_spend
    kind: cost_per_run
    job: model.analytics.fct_orders
    mode: anomaly        # "up 4x from last week", from the Phase 03 baseline
```

## Impact on a pull request

The one place the answer arrives *before* the damage. On a dbt PR, changed models are resolved
against the lineage graph and commented with their blast radius and cost:

```bash
wishd pr-impact $(git diff --name-only origin/main...HEAD)
```

Or use the shipped Action (`deploy/github-action/`), which posts one comment per PR and edits
it in place. Impact comes from lineage we have actually observed — so it includes the Spark job
somebody wired up by hand, and excludes edges that exist in the project but never ran. It fails
open: an observability outage never blocks a merge.

## What has been proven, and what has not

This project's core has been validated against **real producers, not our own simulator** —
sanitized captures live in `tests/fixtures/` and are asserted against. Some integrations have
not, and they are labelled rather than quietly shipped:

| | |
|---|---|
| **Proven** | Airflow 3.0.2, dbt-core (postgres, spark-over-thrift *and* Iceberg), Spark 3.5.7, Iceberg REST catalog 1.9.1, Iceberg and Delta metadata, column lineage from the Spark facet, AWS CUR 2.0 and legacy column shapes |
| **Written, never run against a real account** | EMR clusters, AWS Glue catalog, Snowflake / Databricks / BigQuery / Redshift pollers, Databricks and Dataproc billing |

Every real producer so far has falsified something we believed, so treat the second row as
likely wrong until a capture says otherwise. `ROADMAP.md` tracks each one.

If your dbt models materialise as **Iceberg**, wishd reads the format from what Spark
already reports: tables are identified by path rather than catalog name, and the `symlinks` facet
carries their catalog identity, so both views resolve to one table. A `catalog:` block on an
Iceberg source resolves a table's *current* metadata through an Iceberg REST catalog (validated)
or AWS Glue (written, unvalidated) — necessary because Iceberg writes a new metadata file on every
commit, so a configured path silently goes stale. See [live validation](validation.md) for the remaining Glue checks.

On dbt-spark over a *shared* Thrift Server, dbt and the Spark listener emit disjoint run trees —
the parent cannot be propagated, because the server outlives every invocation. wishd
reconnects them using the `node_id` dbt already embeds in its query comment, so this works out
of the box; `wishd resolve` reports anything it could not reconnect. See [ADR-003](architecture.md#adr-003--dbt-spark-over-thrift-is-the-reference-integration).

You can check how much column lineage you actually have:

```bash
wishd lineage-coverage
```

It reports coverage *and why the rest declined* — a `star` gap is a catalog problem,
`unparseable` is a dialect problem, and they have different fixes.

## How it works

```
producers ──OpenLineage──► gateway ──► correlator ──► Postgres ──► UI / API / CLI
 Airflow                   fail-open   parent-chain    jobs
 Spark/EMR                 ingest      stitching       runs
 dbt-core                  + auth      + repair        datasets
                                                       events (replay source)
```

Design commitments, in order of importance:

1. **OpenLineage is the only ingest contract.** Engine-specific data (Spark stage metrics,
   EMR instance-seconds) rides as custom *facets*, never a side channel. This is what keeps
   it stack-agnostic instead of accumulating N bespoke pipelines.
2. **Run identity is the product.** The `parentRunId` chain — DAG run → task → dbt invocation
   → model → Spark app → SQL execution — is what turns four log streams into one timeline.
3. **Arrival order is irrelevant.** Children routinely land before parents. Placeholder rows
   are built from the parent facet, and a recursive subtree repair corrects depth and root
   when the middle of the chain shows up last.
4. **Fail open, always.** The gateway sits in the hot path of production Spark drivers. A
   malformed event returns `accepted: false` without a retry-inducing 4xx; it is not archived.
   Authentication failures still return 401, and queue saturation can return 503.
5. **Metadata-first, scan-never by default.** (Phase 03.) Freshness and volume come from
   catalog metadata, not `SELECT count(*)`. A tool that generates warehouse bills doesn't stay
   installed.
6. **Boring storage.** Postgres, recursive CTEs, numbered SQL migrations. No Kafka, no Neo4j,
   no Elasticsearch until a benchmark forces it.

## Auth

Set `WISHD_API_TOKENS` and every API route requires a bearer token; the UI asks for it
once and keeps a cookie:

```bash
export WISHD_API_TOKENS="airflow:$(openssl rand -hex 24),spark:$(openssl rand -hex 24)"
```

With no tokens set, wishd runs open on loopback for convenience but **refuses to start on
any other interface**, including `0.0.0.0`. Override with `WISHD_ALLOW_INSECURE=true` only
if the port is genuinely private. This service carries SQL text, table names and stack traces.

## Rebuilding after a logic change

`events` is the source of truth; `runs`, `jobs` and `datasets` are a projection of it. When
the correlator changes — or a producer was misconfigured for a week — re-derive rather than
live with it:

```bash
wishd replay          # rebuild everything from the archive
```

