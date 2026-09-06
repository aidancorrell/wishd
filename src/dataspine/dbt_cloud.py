"""Fetching what dbt Cloud already has, and making it look like everything else.

dbt Cloud emits no OpenLineage. What it does have is an Admin API serving the
same `run_results.json` and `manifest.json` that dbt-core writes to `target/` --
so the work here is not an integration so much as a courier: collect two files,
hand them to `dbt_artifacts`, and let the existing spine do the rest.

That is why dbt Cloud gets the run tree, the live pipeline feed, run failures,
lineage and tests without a single conditional anywhere downstream. Nothing below
this module knows dbt Cloud exists.

**Two ways in, on purpose.** A webhook fires seconds after a job finishes, which
is what makes the live feed worth having. A poll catches whatever the webhook
dropped -- a redeploy, a network blip, a webhook someone disabled and forgot.
Running both is only sane because ingest is idempotent: run ids are derived from
dbt's own `invocation_id`, and check rows key on `(source, table, check,
measured_at)`, so the same invocation arriving twice writes the same rows twice
and changes nothing.

**The webhook is a doorbell, not a delivery.** Its payload is used for exactly
one thing: the run id to go and fetch. Everything recorded comes from artifacts
pulled with our own API token. A signed body still only proves someone holds the
signing secret, and a leaked secret should let an attacker waste our time rather
than write whatever they like into the lineage graph.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

import psycopg

from . import dbt_artifacts
from .config import env

log = logging.getLogger("dataspine.dbt_cloud")

TOKEN_ENV = "DATASPINE_DBT_CLOUD_TOKEN"
ACCOUNT_ENV = "DATASPINE_DBT_CLOUD_ACCOUNT"
HOST_ENV = "DATASPINE_DBT_CLOUD_HOST"
SECRET_ENV = "DATASPINE_DBT_CLOUD_WEBHOOK_SECRET"
NAMESPACE_ENV = "DATASPINE_DBT_CLOUD_NAMESPACE"

DEFAULT_HOST = "cloud.getdbt.com"

# dbt Cloud run statuses. 10 = success, 20 = error, 30 = cancelled. Anything
# below 10 is still in progress and has no artifacts to fetch yet.
FINISHED = (10, 20, 30)
IN_PROGRESS = (1, 2, 3)  # queued, starting, running

DEFAULT_SINCE_HOURS = 24
DEFAULT_LIMIT = 50

# One page of the runs API. `finished_runs` pages until it reaches the window's
# edge rather than trusting a single request: an account busier than one page
# inside the window would otherwise lose its *older* runs silently, which is the
# worst way to lose them -- the recent ones keep arriving, so nothing looks wrong.
PAGE_SIZE = 50
MAX_PAGES = 20


class DbtCloudError(RuntimeError):
    """Configuration or API problems worth surfacing to a human."""


def configured() -> bool:
    return bool(env.get(TOKEN_ENV) and env.get(ACCOUNT_ENV))


def _config() -> tuple[str, str, str]:
    token = env.get(TOKEN_ENV, "").strip()
    account = env.get(ACCOUNT_ENV, "").strip()
    if not token or not account:
        raise DbtCloudError(
            f"dbt Cloud is not configured — set {TOKEN_ENV} and {ACCOUNT_ENV}"
        )
    host = env.get(HOST_ENV, "").strip() or DEFAULT_HOST
    return token, account, host.rstrip("/").removeprefix("https://")


def namespace() -> str | None:
    """The dataset namespace to record tables under.

    Worth setting when the same warehouse is also reported by something else --
    a Spark job, or a `sources.yml` poller -- because matching on the final
    segment will merge them anyway, but a shared namespace makes the catalog read
    as one system rather than two.
    """
    return env.get(NAMESPACE_ENV, "").strip() or None


# ------------------------------------------------------------------ API client


def _client(client: Any = None) -> tuple[Any, bool]:
    if client is not None:
        return client, False
    import httpx

    token, _, host = _config()
    return (
        httpx.Client(
            base_url=f"https://{host}",
            headers={"Authorization": f"Token {token}", "Accept": "application/json"},
            timeout=60,
        ),
        True,
    )


# A fixed root for deriving run ids from dbt Cloud run ids. Separate from the
# invocation-derived id in `dbt_artifacts` on purpose: this one is available the
# moment a run starts, which is what makes a live feed possible at all.
CLOUD_RUN_ROOT = uuid5(NAMESPACE_URL, "https://dataspine.dev/dbt-cloud/run")


def cloud_run_id(run_id: Any) -> UUID:
    """The dataspine run id for a dbt Cloud run.

    Keyed on dbt Cloud's own run id rather than dbt's `invocation_id`, because
    the invocation id lives inside `run_results.json` and therefore does not
    exist until the run has finished. Using it would make the placeholder posted
    while a job is running a *different* run from the tree posted when it ends.
    """
    return uuid5(CLOUD_RUN_ROOT, str(run_id))


def _list_runs(client: Any, *, since: datetime, statuses: tuple[int, ...],
               limit: int) -> list[dict[str, Any]]:
    """Runs in `statuses` created since `since`, newest first.

    The window is on when a run *started*, not when it finished, because a run
    still in progress has no finish time at all -- and one field that means the
    same thing for both is worth more here than precision that only applies to
    half the cases. For a 24-hour window and jobs measured in minutes the two are
    the same; the case it gives up is a run that began before the window and
    ended inside it.

    The v2 API has no time filter, so this orders and pages until it reaches the
    window's edge. `MAX_PAGES` bounds it: a misconfigured window should cost a
    slow poll, not an unbounded walk of the account's whole history.
    """
    _, account, _ = _config()
    out: list[dict[str, Any]] = []
    for page in range(MAX_PAGES):
        response = client.get(
            f"/api/v2/accounts/{account}/runs/",
            params={"order_by": "-created_at", "limit": PAGE_SIZE,
                    "offset": page * PAGE_SIZE},
        )
        _raise_for(response, "listing runs")
        batch = (response.json() or {}).get("data") or []
        if not batch:
            return out

        for run in batch:
            # Ordered newest-first by creation, so once we are past the window
            # everything remaining is older still.
            created = _time(run.get("created_at"))
            if created is not None and created < since:
                return out
            if run.get("status") in statuses:
                out.append(run)
                if len(out) >= limit:
                    return out
        if len(batch) < PAGE_SIZE:
            return out
    log.warning("stopped paging dbt Cloud runs after %s pages", MAX_PAGES)
    return out


def finished_runs(
    client: Any, *, since: datetime, limit: int = DEFAULT_LIMIT
) -> list[dict[str, Any]]:
    """Runs that finished inside the window, newest first."""
    return _list_runs(client, since=since, statuses=FINISHED, limit=limit)


def running_runs(
    client: Any, *, since: datetime, limit: int = DEFAULT_LIMIT
) -> list[dict[str, Any]]:
    """Runs still queued, starting or running.

    These have no artifacts -- artifacts are written when a run ends -- so
    nothing here can produce a tree or a test result. What they can produce is
    the knowledge that a job is *underway*, which is the whole of what a live
    feed needs to show a row before the outcome is known.
    """
    return _list_runs(client, since=since, statuses=IN_PROGRESS, limit=limit)


def project_name(client: Any, project_id: Any) -> str | None:
    """The dbt project's name, for naming a run before its manifest exists."""
    _, account, _ = _config()
    response = client.get(f"/api/v3/accounts/{account}/projects/{project_id}/")
    if response.status_code != 200:
        return None
    name = ((response.json() or {}).get("data") or {}).get("name")
    return str(name).strip() or None if name else None


def run_detail(client: Any, run_id: Any) -> dict[str, Any] | None:
    """One run's own metadata, or None when dbt Cloud has never heard of it.

    Worth the API call for two things at once, which is why it is fetched as a
    whole rather than as a `job_name` lookup. Most teams run several jobs against
    one project — a build, an hourly incremental, a test-only pass — and without
    the name every one of them is `<project>.run`: one job as far as dataspine is
    concerned, durations averaged together and no way for a route to tell them
    apart. And the payload carries `href`, the run's page in the dbt Cloud UI.
    """
    _, account, _ = _config()
    response = client.get(
        f"/api/v2/accounts/{account}/runs/{run_id}/",
        params={"include_related": '["job"]'},
    )
    if response.status_code == 404:
        return None
    _raise_for(response, f"fetching run {run_id}")
    data = (response.json() or {}).get("data")
    return data if isinstance(data, dict) else None


def job_name(client: Any, run_id: Any) -> str | None:
    """What the team calls this job in dbt Cloud."""
    return _job_name_from(run_detail(client, run_id) or {})


def _job_name_from(data: dict[str, Any]) -> str | None:
    job = data.get("job") if isinstance(data.get("job"), dict) else {}
    name = job.get("name") or data.get("job_name")
    return str(name).strip() or None if name else None


def run_facet(run_id: Any, data: dict[str, Any] | None = None) -> dict[str, Any]:
    """Where this run lives in dbt Cloud, as a run facet.

    **`href` is taken from dbt Cloud, never assembled here.** The account's cell
    is in the hostname (`abc123.us1.dbt.com`), the path carries an account and a
    project id, and a link built from the wrong one of those 404s or — worse —
    lands on a stranger's run. dbt Cloud states the URL in every run payload, so
    the only correct move is to keep the one it gave us. When it did not give us
    one, the alert has no dbt Cloud link and says nothing, per `links.py`.
    """
    data = data or {}
    href = data.get("href")
    facet: dict[str, Any] = {
        "_producer": dbt_artifacts.PRODUCER,
        "_schemaURL": f"{dbt_artifacts.SPEC}#/$defs/RunFacet",
        "runId": str(run_id),
    }
    if isinstance(href, str) and href.startswith("http"):
        facet["href"] = href
    for key, field in (("jobId", "job_definition_id"), ("projectId", "project_id")):
        if data.get(field) is not None:
            facet[key] = str(data[field])
    return {"dbt_cloud": facet}


def artifact(client: Any, run_id: Any, name: str) -> dict[str, Any] | None:
    """One artifact for one run, or None when the run never produced it.

    A 404 is ordinary rather than exceptional: a run that failed during `dbt
    parse` has no `run_results.json`, and a cancelled one may have neither.
    """
    _, account, _ = _config()
    response = client.get(f"/api/v2/accounts/{account}/runs/{run_id}/artifacts/{name}")
    if response.status_code == 404:
        return None
    _raise_for(response, f"fetching {name} for run {run_id}")
    return response.json()


def _raise_for(response: Any, what: str) -> None:
    status = getattr(response, "status_code", 0)
    if 200 <= status < 300:
        return
    if status in (401, 403):
        raise DbtCloudError(
            f"dbt Cloud rejected the token while {what} (HTTP {status}) — "
            f"check {TOKEN_ENV} and that it has read access to the account"
        )
    raise DbtCloudError(f"dbt Cloud returned HTTP {status} while {what}")


# -------------------------------------------------------------------- ingestion


def ingest_run(conn: psycopg.Connection, client: Any, run_id: Any) -> dict[str, Any]:
    """Fetch one dbt Cloud run's artifacts and record everything in them.

    Idempotent, which is what lets the webhook and the poll both run: the
    synthesised run ids come from dbt's `invocation_id`, and check rows key on
    their measurement time, so a second pass rewrites the same rows.
    """
    from . import dq
    from .events import RunEvent
    from .ingest import ingest_run_event

    result = {"run_id": str(run_id), "events": 0, "checks": 0, "skipped": None}

    run_results = artifact(client, run_id, dbt_artifacts.RUN_RESULTS)
    if run_results is None:
        # A run that died before executing anything. Not an error, and not worth
        # retrying on the next poll either.
        result["skipped"] = "no run_results.json"
        return result
    manifest = artifact(client, run_id, dbt_artifacts.MANIFEST)

    invocation = dbt_artifacts.parse(run_results, manifest)

    # Best-effort: a job we cannot name is still a job worth recording, and
    # failing the whole ingest over a label would be the wrong trade. The same
    # call carries the run's `href`, so a lost lookup costs the link too.
    try:
        detail = run_detail(client, run_id)
    except Exception as exc:  # noqa: BLE001
        log.warning("could not read the detail for run %s: %s", run_id, exc)
        detail = None
    name = _job_name_from(detail or {})
    result["job"] = name

    events = dbt_artifacts.events(
        invocation, namespace=namespace(), job_name=name,
        # The same id the in-progress placeholder used, so the run the feed has
        # been showing becomes this tree rather than a second one beside it.
        root_run_id=cloud_run_id(run_id),
        run_facets=run_facet(run_id, detail),
    )
    for event in events:
        ingest_run_event(conn, RunEvent.model_validate(event))
    result["events"] = len(events)

    rows = dbt_artifacts.test_results(invocation)
    result["checks"] = dq.import_results(conn, source="dbt", rows=rows) if rows else 0

    # Announced here rather than from a cron sweep: the webhook has already done
    # the fetching and parsing, and making it then wait five minutes for a poller
    # to notice would give up the one thing a webhook is for.
    from . import notify

    sent = notify.announce_dbt_job(
        conn, invocation, job_name=name, run_id=cloud_run_id(run_id)
    )
    result["announced"] = bool(sent.get("sent"))
    return result


def ingest_running(
    conn: psycopg.Connection, client: Any, run: dict[str, Any],
    *, project: str | None = None,
) -> dict[str, Any]:
    """Record a dbt Cloud run that is still going, from its metadata alone.

    There are no artifacts yet -- dbt writes them when it finishes -- so this
    cannot say which models ran or which tests failed. It says the one thing a
    live feed needs before the outcome is known: this job is underway, and it
    started at this time.

    When the run ends, `ingest_run` emits the full tree under the *same* run id
    and the feed edits its existing message rather than posting a second one.
    """
    from .events import RunEvent
    from .ingest import ingest_run_event

    run_id = run.get("id")
    started = _time(run.get("started_at")) or _time(run.get("created_at")) \
        or datetime.now(UTC)
    name = (run.get("job") or {}).get("name") if isinstance(run.get("job"), dict) else None
    if not name:
        try:
            name = job_name(client, run_id)
        except Exception as exc:  # noqa: BLE001 - a label must not fail the ingest
            log.warning("could not read the job name for run %s: %s", run_id, exc)
    # The listing payload already carries `href`, so the live feed can link to
    # dbt Cloud from the first message rather than only once the job finishes --
    # which is the half of the run someone actually wants to go and watch.
    if project is None:
        project = project_name(client, run.get("project_id")) or "dbt"

    job = f"{project}.{name}" if name else f"{project}.run"
    event = {
        "eventTime": started.isoformat(),
        "producer": dbt_artifacts.PRODUCER,
        "schemaURL": f"{dbt_artifacts.SPEC}#/$defs/RunEvent",
        "eventType": "START",
        "run": {"runId": str(cloud_run_id(run_id)), "facets": run_facet(run_id, run)},
        "job": {"namespace": "dbt", "name": job,
                "facets": dbt_artifacts._job_type("DBT", "JOB")},
        "inputs": [], "outputs": [],
    }
    ingest_run_event(conn, RunEvent.model_validate(event))
    return {"run_id": str(run_id), "job": job, "state": "RUNNING"}


def pull(
    conn: psycopg.Connection,
    *,
    since: datetime | None = None,
    limit: int = DEFAULT_LIMIT,
    client: Any = None,
    now: datetime | None = None,
    with_running: bool = True,
) -> list[dict[str, Any]]:
    """The backstop sweep. Returns what each run contributed.

    One run failing must not stop the others: a single malformed artifact would
    otherwise hold up every run behind it, on every poll, forever.
    """
    now = now or datetime.now(UTC)
    since = since or now - timedelta(hours=DEFAULT_SINCE_HOURS)

    api, owns = _client(client)
    out = []
    try:
        # Finished runs first. A run that finished between the two calls would
        # otherwise be reported as still running by the second, and the feed
        # would show a job as live seconds after it had already ended.
        for run in finished_runs(api, since=since, limit=limit):
            try:
                out.append(ingest_run(conn, api, run["id"]))
            except Exception as exc:  # noqa: BLE001 - see docstring
                log.warning("dbt Cloud run %s could not be ingested: %s", run.get("id"), exc)
                out.append({"run_id": str(run.get("id")), "error": str(exc)})

        if with_running:
            project: str | None = None
            for run in running_runs(api, since=since, limit=limit):
                try:
                    if project is None:
                        project = project_name(api, run.get("project_id")) or "dbt"
                    out.append(ingest_running(conn, api, run, project=project))
                except Exception as exc:  # noqa: BLE001 - see docstring
                    log.warning("dbt Cloud run %s in flight: %s", run.get("id"), exc)
    finally:
        if owns:
            api.close()
    return out


# --------------------------------------------------------------------- webhook


def verify(body: bytes, signature: str | None) -> bool:
    """Check dbt Cloud's HMAC over the raw request body.

    Compared with `compare_digest` rather than `==`: a plain comparison returns
    early on the first differing byte, and that timing is enough to recover a
    signature one byte at a time.

    Returns False when no secret is configured. Refusing is the safe default —
    an unauthenticated endpoint that ingests on demand is one anybody can use to
    make us fetch on their behalf.
    """
    secret = env.get(SECRET_ENV, "").strip()
    if not secret or not signature:
        return False
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature.strip())


def run_id_from(payload: dict[str, Any]) -> str | None:
    """The one thing taken from a webhook body: which run to go and fetch."""
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    for key in ("runId", "run_id", "id"):
        if data.get(key):
            return str(data[key])
    return None


def _time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
