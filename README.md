# wish:d

**what is happening:data** — self-hosted data observability for Airflow, dbt and Spark.

Follow a failed pipeline from the Airflow task to the dbt model to the Spark execution,
with its errors, datasets and timing in one place. wish:d correlates OpenLineage events
into a run tree, then uses that history for monitors, lineage, incidents and cost analysis.

One Python service and Postgres. A server-rendered web UI, an HTTP API and a CLI.
Apache-2.0 licensed.

**Status: v1 release candidate (`1.0.0rc1`).** The frontend redesign is merged; final release checks
are pending. Packages and container images have not been published; install from source
below. See the [release checklist](docs/releasing.md) for outstanding gates.

## Try it locally

Requirements: Git, [uv](https://docs.astral.sh/uv/getting-started/installation/), Make,
OpenSSL, and a running Docker Engine with Compose v2. The Makefile selects Python 3.12;
`uv` can install that interpreter. Package metadata requires Python 3.11 or newer.
Use macOS or Linux; Windows users can use WSL2.

```bash
git clone https://github.com/aidancorrell/wishd.git
cd wishd
make install
make demo
```

Open **http://localhost:8080**. `make demo` creates `.env`, builds the gateway, starts
Postgres, and seeds a simulated Airflow → dbt → Spark pipeline. At login, use the token
from `.env`: copy the value after `local:` in `WISHD_API_TOKENS`. On an existing checkout,
the key may still be `DATASPINE_API_TOKENS`.

Explore the pipeline and add the example monitors:

```bash
.venv/bin/wishd runs --roots
.venv/bin/wishd tree <run-id-or-prefix>
.venv/bin/wishd apply monitors/example.yml
.venv/bin/wishd check --schedule hourly
```

The demo is synthetic and needs no cloud account. Slack and other external notifications
are off until you configure credentials. `make down` stops the stack and keeps its data.
Postgres and uploaded artifacts have persistent Docker volumes.

**Existing local deployment?** The Compose project, database, artifact paths, cookies,
Python imports and Prometheus metric names retain their `dataspine` names so upgrades can
find existing data. [Rename compatibility](docs/configuration.md#rename-compatibility)
explains the new names and precedence. The GitHub URL will change only when the repository
itself is renamed.

### Without Docker

The development setup uses an embedded Postgres, the same one used by the tests:

```bash
make install
make dev-db
make migrate
make seed
.venv/bin/wishd serve --host 127.0.0.1 --port 8080
```

Open the same URL and use the token in `.env`. Stop the gateway with Ctrl+C, then run
`make dev-down` to stop Postgres. Development data lives in `.dev/` and `.dataspine/`.

### Bring your own Postgres

From the cloned repository, install the package and use a dedicated database:

```bash
uv venv --python 3.12
uv pip install . -c constraints.txt
export WISHD_DATABASE_URL='postgresql://USER:PASSWORD@HOST:5432/DATABASE'
export WISHD_API_TOKENS="local:$(openssl rand -hex 24)"
.venv/bin/wishd migrate
.venv/bin/wishd serve --host 127.0.0.1 --port 8080
```

Migrations and web assets are included in the installed package. Use `uv pip install '.[aws]'`
with the constraints file if you need S3 or Glue; the Docker image includes that extra.

## What you can do

- **Investigate a run:** correlated parent/child trees, errors, SQL, artifacts and Spark metrics.
- **Monitor data:** freshness, volume, schema drift, anomalies and dbt test failures, declared in YAML.
- **Understand impact:** table and column lineage, catalog search, incidents and downstream PR impact.
- **Notify a team:** Slack pipeline updates, monitor transitions and digests; optional PagerDuty/webhooks.
- **Inspect cost:** import AWS Cost and Usage Reports and attribute observed compute usage to runs.

Monitoring uses collected metadata by default. Optional profiling and warehouse pollers need
explicit configuration. The [integration guide](docs/integrations.md) covers setup, examples,
and the limits of each feature. Start producers at `POST /api/v1/lineage`; configure their
OpenLineage HTTP transport with the gateway URL and a bearer token. A `name:` token label is
for your configuration only: send the secret after the colon.

## Validation and limits

Tests include sanitized captures from Airflow 3.0.2, dbt-core, dbt Cloud on Snowflake,
Spark 3.5.7, dbt-spark over Thrift, Iceberg and AWS CUR. These establish parser and
correlation behavior for those captures, not compatibility with every deployment.

**Real EMR cluster deployment, AWS Glue, and the warehouse pollers still need live
validation.** dbt Cloud artifact ingestion on Snowflake does not validate the Snowflake
warehouse poller. The [validation plan](docs/validation.md) tracks that distinction.

This is a single-team service with shared tokens, without per-user roles or tenant isolation.
Queued ingestion acknowledges events before database commit; a process crash can lose queued
events. Set `WISHD_INGEST_ASYNC=false` for synchronous ingest. Monitor evaluation and notification
sweeps need a scheduler; only storage upkeep runs automatically in the gateway.

## Running beyond your laptop

The local Compose setup binds to loopback and uses demo Postgres credentials. Before a shared
deployment, configure a dedicated database password, TLS, trusted proxy headers, token rotation,
backups, persistent artifact storage and the scheduled checks. API tokens grant full application
access. Pipeline metadata can contain SQL, credentials embedded by producers, and business data.

Read [configuration](docs/configuration.md), [operations](docs/operations.md), and the
[security policy](SECURITY.md) before exposing the service.

## Contributing

```bash
make install
make lint
make test
```

Tests boot real Postgres without Docker or cloud credentials. Slow load/scale tests and live AWS
tests are opt-in. See [CONTRIBUTING.md](CONTRIBUTING.md) for development, fixture sanitation,
package checks, and submitting changes. See [architecture decisions](docs/architecture.md) for design rationale and
[ROADMAP.md](ROADMAP.md) for current priorities.

Source code is under [Apache-2.0](LICENSE). Bundled fonts have their own
[license notices](THIRD_PARTY_NOTICES.md).
