# CLAUDE.md

Product name: **wish:d** (what is happening:data). Preferred CLI/package: `wishd`;
internal imports remain `dataspine`, and `DATASPINE_*` settings remain compatible.
`WISHD_*` is preferred. See `docs/releasing.md` for publication gates.
Do not change visibility, publish packages, or rewrite shared history without explicit
owner approval. The repository is public as of the initial `1.0.0rc1` release; `main` is
protected and every change lands through a reviewed PR. `docs/releasing.md` records which
publication gates were actually verified and which are still open.

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
make install          # uv venv + editable install with dev extras
make test             # full suite; boots its own embedded Postgres
make lint             # ruff check src tests
make fmt              # ruff format + --fix
make coverage         # what the suite never reaches
```

Everything runs out of `.venv/bin/` — the venv is **not** on `PATH`, so invoke tools
explicitly (`.venv/bin/pytest`, `.venv/bin/wishd`).

Running one test, or one file:

```bash
.venv/bin/pytest tests/test_incidents.py -q
.venv/bin/pytest tests/test_slack.py::test_first_matching_route_wins -q
```

Three marker groups are **excluded by default** (`addopts` in `pyproject.toml`) and must be
asked for by name: `-m load` (performance floors), `-m scale` (generates 10^6 rows, minutes),
`-m aws` (needs real AWS credentials).

Two ways to get a database. Docker is the supported deployment; the embedded server exists so
the suite and local work need neither Docker nor a running service:

```bash
make up               # docker compose: Postgres + gateway. Also writes .env
make dev-db           # embedded Postgres in .dev/pgdata, no Docker
make seed             # push a simulated Airflow -> dbt -> Spark pipeline through it
```

## Architecture

The whole system is one idea: **correlate run events from four producers that know nothing
about each other, then answer everything else as a query over that.** Read `ingest.py` and
`queries.py` before anything else.

**The spine.** OpenLineage events arrive at `/api/v1/lineage` (`api.py`) and land in
`events.py` → `ingest.py` → the `runs` / `jobs` / `datasets` tables. Two properties there are
load-bearing and easy to break:

- **Arrival order is not ours to control.** A child run routinely arrives before its parent, so
  `parent_run_id` and `root_run_id` are deliberately *not* foreign keys, and the correlator
  creates placeholder parent rows from the parent facet. `unstitched_runs` (a view) is the
  alarm for when that fails; it is the single best health metric in the system.
- **Run state only moves forward** (`STATE_RANK` in `events.py`), so a late `START` cannot
  resurrect a run that already failed.

**The layers above it**, each a query over the layer below: `identity.py` merges the several
names one physical table arrives under → `lineage.py` builds table and column edges →
`monitors.py` / `checks.py` evaluate declared monitors → `incidents.py` groups breaches by
lineage into cause + blast radius → `alerts.py` / `notify.py` / `slack.py` deliver.

**Detection and delivery are separated on purpose.** `checks.py` splits `collect` (observation)
from `judge` (verdict); `incidents.suppress` sits between evaluation and delivery so every
breach is still recorded and visible even when only its cause is sent.

**dbt is one parser serving two worlds.** `run_results.json` is byte-identical whether dbt-core
wrote it to `target/` or dbt Cloud served it from the Admin API, so `dbt_artifacts.py` handles
both and only the courier differs (`push-artifacts` vs `dbt_cloud.py`). dbt Cloud emits no
OpenLineage, so its run tree is *synthesised* from the artifacts — named the way `dbt-ol` names
things, so a shop running both sees one vocabulary and nothing downstream is special-cased.

**Notifications are keyed, not fired.** `notify.py` holds a ledger (`notifications`) claimed
*before* sending, with bounded retry. Read the migration comments in `020`–`022` before
changing any of it: the dedup, retry and live-message-editing rules each encode a specific
failure that was hit in practice.

## Conventions that are load-bearing

**Ingest fails open; configuration fails closed.** An event we cannot parse is data already
generated that we would otherwise lose, so it is kept. A monitor or routes file we cannot parse
has produced nothing yet, and accepting it half-formed means one that silently never fires.
`monitors.parse_spec` and `slack.parse_routes` are strict for that reason; `ingest.py` is not.

**Delivery must never break detection.** Every alert path swallows and records its failures. A
500 from Slack is a lost alert; letting it raise turns that into a lost monitoring sweep.

**Docstrings explain *why*, not what.** The prose carries the reasoning — what the alternative
was and why it loses. Match that register; a comment restating the code is worse than none.

**Migrations are checksummed once applied.** Editing an applied migration raises
`MigrationDrift`. Add a new one; never edit in place.

**Tests run against a real Postgres** (`pgserver`, booted by the session fixture), because the
correlator's job is expressed in SQL and testing it against anything else tests a different
program. Fixtures in `tests/fixtures/` are **real captures** — real OpenLineage from real
producers, a real `dbt build`, a real AWS CUR. Prefer regenerating one over hand-editing it:
two defects in the dbt work came from behaviour a handwritten fixture would not have had.

**ADR-001 through ADR-008 live in `docs/architecture.md`**, and are cited by name
throughout the source. `ROADMAP.md` records current priorities and validation gaps.
When shipping an unvalidated integration, label its limits there and in `docs/integrations.md`.

## Things that will cost you an afternoon

- **`dbt` on PATH is the dbt Cloud CLI**, a different tool. dbt-core lives only at
  `.venv/bin/dbt` and is used for regenerating fixtures.
- **dbt Cloud is multi-cell.** Accounts live at hosts like `abc123.us1.dbt.com`, and
  `cloud.getdbt.com` returns a 401 indistinguishable from a bad token. `dataspine
  dbt-cloud-check` diagnoses it.
- **The dbt Cloud webhook must stay off the `/api/v1` router.** That router carries
  `Depends(auth.require_token)` and dbt Cloud cannot present a bearer token; it is declared on
  `app` beside `/health` and `/metrics`, authenticating by HMAC instead. Tests pass either way
  because the fixture leaves `DATASPINE_API_TOKENS` unset.
- **Slack's `chat.postMessage` returns HTTP 200 with `{"ok": false}`** for an unknown channel or
  a revoked token. Status-code-only checking records those as delivered.

## Reference

`README.md` for what the system does and how it is run; `docs/operations.md` for deploying,
upgrading, retention and what to alert on; `docs/design.md` for the interface rules and the
measured accessibility contract; `docs/architecture.md` for ADRs; `ROADMAP.md` for priorities and validation gaps.
