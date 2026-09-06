"""The ingest gateway.

`POST /api/v1/lineage` is the OpenLineage HTTP transport path, so the stock
Airflow provider and the Spark listener can point at this service with no custom
client:

    OPENLINEAGE__TRANSPORT__TYPE=http
    OPENLINEAGE__TRANSPORT__URL=http://dataspine:8080

Design rule for this endpoint: **fail open**. It sits in the hot path of
production Spark drivers, and a gateway that 500s or hangs must never be the
reason someone's pipeline dies. Malformed events are archived and counted, not
rejected with a stack trace.

Auth is the one exception to fail-open: a 401 is a configuration error the
operator can fix, and silently accepting unauthenticated writes would let anyone
who can reach the port poison the lineage graph.
"""

from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import (
    APIRouter,
    Depends,
    FastAPI,
    File,
    HTTPException,
    Query,
    Request,
    Response,
    UploadFile,
)
from fastapi.openapi.utils import get_openapi
from fastapi.responses import HTMLResponse, PlainTextResponse
from pydantic import ValidationError

from . import (
    __version__,
    agents,
    artifacts,
    auth,
    catalog,
    checks,
    cost,
    dbt_artifacts,
    dbt_cloud,
    dq,
    identity,
    incidents,
    lineage,
    pr,
    queries,
    resources,
    slack,
    upkeep,
)
from . import (
    metrics as metrics_mod,
)
from . import monitors as monitors_mod
from . import queue as ingest_queue
from .config import env
from .db import connection
from .events import RunEvent
from .http_security import SecurityMiddleware
from .ingest import ingest_run_event

log = logging.getLogger("dataspine.api")


def parse_timestamp(value: str | None) -> datetime | None:
    """Lenient ISO parse. A hand-edited URL should narrow nothing, not 500."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        log.debug("ignoring unparseable timestamp: %r", value)
        return None

# One request may carry a buffered flush from a Spark listener. Cap it so a
# single client cannot occupy a worker unboundedly.
MAX_BATCH = 1000


@asynccontextmanager
async def lifespan(app: FastAPI):
    if ingest_queue.async_enabled():
        ingest_queue.get_queue().start()
    # Storage upkeep. Partition provisioning used to depend on someone running
    # `dataspine maintain` from a cron the project never shipped, so a
    # deployment left alone eventually wrote everything into the DEFAULT
    # backstop partition -- silently, because nothing fails when that happens.
    upkeep_worker = upkeep.Upkeep()
    upkeep_worker.start()
    yield
    upkeep_worker.stop()
    # Drain before exiting: a deploy must not vaporise in-flight events.
    if ingest_queue.async_enabled():
        ingest_queue.get_queue().drain_and_stop()


# The interactive docs and the schema they read are disabled by default and
# served, authenticated, from `/api/v1/openapi.json` instead. FastAPI mounts
# them on the *app*, not the router, so the router's token dependency never
# applied to them: an unauthenticated GET returned the full API surface,
# including every path the deployment exposes. That is not a data leak, but it
# is a free map for anyone deciding what to try next.
app = FastAPI(
    title="wish:d",
    version=__version__,
    description="OpenLineage ingest gateway and run-tree correlator.",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)
app.add_middleware(SecurityMiddleware)
api = APIRouter(prefix="/api/v1", dependencies=[Depends(auth.require_token)])


@app.get("/health")
def health() -> dict[str, Any]:
    """Unauthenticated on purpose: load balancers and `docker compose` health
    checks need it, and it reveals nothing but liveness."""
    return {"status": "ok", "auth_enabled": auth.auth_enabled()}


@app.get("/ready")
def ready() -> dict[str, str]:
    with connection() as conn:
        conn.execute("select 1")
    return {"status": "ready"}


# ---------------------------------------------------------------------- ingest


def _validate(payload: Any) -> tuple[RunEvent | None, dict[str, Any] | None]:
    """Cheap validation on the request thread, before anything is queued.

    Done here rather than in the worker so a producer still gets told its event
    was malformed -- the queue would swallow that feedback entirely.
    """
    # DatasetEvent / JobEvent (design-time, no run attached) are valid spec
    # events we do not model yet. Accept and archive rather than reject.
    if not isinstance(payload, dict) or "run" not in payload:
        return None, {"accepted": False, "reason": "not a RunEvent (no run object)"}
    try:
        return RunEvent.model_validate(payload), None
    except ValidationError as exc:
        log.warning("rejected malformed event: %s", exc.errors()[:3])
        return None, {
            "accepted": False,
            "reason": "schema validation failed",
            "errors": exc.errors()[:3],
        }


MAX_BODY_ENV = "DATASPINE_MAX_BODY_BYTES"
# 16 MB. A single OpenLineage event with a large Spark logical plan runs to a few
# hundred KB; a batch of them, comfortably under this. The number exists to stop
# an unbounded body, not to be a tight quota.
DEFAULT_MAX_BODY = 16 * 1024 * 1024


def max_body_bytes() -> int:
    try:
        value = int(env.get(MAX_BODY_ENV, DEFAULT_MAX_BODY))
        return value if value > 0 else DEFAULT_MAX_BODY
    except ValueError:
        return DEFAULT_MAX_BODY


async def _read_json(request: Request) -> tuple[Any, str | None]:
    """Read a JSON body with a size ceiling. Returns (payload, error).

    `await request.json()` buffers the whole body into memory with no limit, on
    the one endpoint deliberately exposed to every producer in a data platform.
    A single large POST was enough to exhaust the process.

    The ceiling is enforced against Content-Length where a client sends one and
    against the accumulated stream where it does not, because a chunked upload
    reports no length and is exactly how the limit would be sidestepped.
    """
    limit = max_body_bytes()
    declared = request.headers.get("content-length")
    if declared is not None:
        try:
            if int(declared) > limit:
                return None, f"body exceeds {limit} bytes"
        except ValueError:
            return None, "malformed content-length"

    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > limit:
            # Stop reading rather than draining: there is no reason to spend
            # memory finishing a body already known to be over the limit.
            return None, f"body exceeds {limit} bytes"
        chunks.append(chunk)

    try:
        return json.loads(b"".join(chunks) or b""), None
    except Exception:
        return None, "body is not valid JSON"


def _ingest_one(conn, payload: Any) -> dict[str, Any]:
    """Validate and ingest a single payload synchronously. Never raises."""
    event, error = _validate(payload)
    if error is not None:
        return error
    return {"accepted": True, **ingest_run_event(conn, event)}


@api.post("/lineage", status_code=201)
async def post_lineage(request: Request, response: Response) -> dict[str, Any]:
    """Accept one OpenLineage event.

    Returns 201 on success, matching what OpenLineage clients expect. A payload
    we cannot parse gets 200 + `{"accepted": false}` rather than a 4xx: the
    producer cannot fix it at runtime anyway, and we would rather it kept
    running than started retrying in a loop against a Spark driver's heartbeat.
    """
    payload, error = await _read_json(request)
    if error is not None:
        # Still a 200: the fail-open contract is about not pushing producers
        # into a retry loop, and an oversized body is no more fixable at runtime
        # than a malformed one.
        response.status_code = 200
        return {"accepted": False, "reason": error}

    if not ingest_queue.async_enabled():
        with connection() as conn:
            result = _ingest_one(conn, payload)
        if not result["accepted"]:
            response.status_code = 200
        return result

    event, error = _validate(payload)
    if error is not None:
        response.status_code = 200
        return error

    # 201 = handed off durably. 202 = received but shed under saturation, which
    # is a 2xx on purpose: a 5xx would make clients retry into an already
    # overloaded gateway while a Spark driver waits on each attempt.
    if ingest_queue.get_queue().submit(payload):
        return {"accepted": True, "queued": True}
    response.status_code = 202
    return {"accepted": True, "queued": False, "shed": True}


@api.post("/lineage/batch", status_code=201)
async def post_lineage_batch(request: Request, response: Response) -> dict[str, Any]:
    """Accept an array of events in one request.

    The Spark listener buffers and flushes; one HTTP request per event wastes a
    round trip per Spark stage. Partial success is normal here -- one malformed
    event must not discard the 999 good ones alongside it, so the whole batch is
    ingested in a single transaction and the failures are reported per index.
    """
    payload, error = await _read_json(request)
    if error is not None:
        response.status_code = 200
        return {"accepted": 0, "rejected": 0, "reason": error}

    if not isinstance(payload, list):
        response.status_code = 200
        return {"accepted": 0, "rejected": 0, "reason": "expected a JSON array"}
    if len(payload) > MAX_BATCH:
        raise HTTPException(status_code=413, detail=f"batch larger than {MAX_BATCH} events")

    accepted, shed, failures = 0, 0, []

    if ingest_queue.async_enabled():
        q = ingest_queue.get_queue()
        for index, item in enumerate(payload):
            _, error = _validate(item)
            if error is not None:
                failures.append({"index": index, "reason": error["reason"]})
                continue
            if q.submit(item):
                accepted += 1
            else:
                shed += 1
    else:
        with connection() as conn:
            for index, item in enumerate(payload):
                result = _ingest_one(conn, item)
                if result["accepted"]:
                    accepted += 1
                else:
                    failures.append({"index": index, "reason": result["reason"]})

    if shed:
        response.status_code = 202
    return {
        "accepted": accepted,
        "shed": shed,
        "rejected": len(failures),
        # Bounded: a pathological batch should not produce a megabyte of errors.
        "failures": failures[:20],
    }


# ----------------------------------------------------------------------- reads


@api.get("/runs")
def get_runs(
    integration: str | None = None,
    state: str | None = None,
    job: str | None = None,
    roots_only: bool = False,
    since: str | None = None,
    until: str | None = None,
    limit: int = Query(50, le=500),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    with connection() as conn:
        runs = queries.list_runs(
            conn,
            integration=integration,
            state=state,
            job_name=job,
            roots_only=roots_only,
            since=parse_timestamp(since),
            until=parse_timestamp(until),
            limit=limit,
            offset=offset,
        )
    return {"runs": runs, "count": len(runs), "offset": offset}


@api.get("/runs/{run_id}")
def get_run(run_id: UUID) -> dict[str, Any]:
    with connection() as conn:
        run = queries.get_run(conn, run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="run not found")
        run["datasets"] = queries.run_datasets(conn, run_id)
    return run


@api.get("/runs/{run_id}/tree")
def get_run_tree(run_id: UUID, from_root: bool = True) -> dict[str, Any]:
    """The run tree. `from_root=true` (default) walks up to the root first, so
    any run id in the pipeline returns the whole execution, not just the part
    below where you happened to look."""
    with connection() as conn:
        start = run_id
        if from_root:
            root = queries.root_of(conn, run_id)
            if root is None:
                raise HTTPException(status_code=404, detail="run not found")
            start = root
        nodes = queries.run_tree(conn, start)
        datasets = queries.tree_datasets(conn, start)
    return {"root_run_id": str(start), "nodes": nodes, "datasets": datasets}


@api.get("/jobs/{job_id}/history")
def get_job_history(job_id: int, limit: int = Query(50, le=200)) -> dict[str, Any]:
    """Run-over-run history for one job — the shape every duration and volume
    regression question starts from."""
    with connection() as conn:
        return {"runs": queries.job_history(conn, job_id, limit=limit)}


# ------------------------------------------------------------------ artifacts


@api.post("/runs/{run_id}/artifacts", status_code=201)
async def upload_artifact(run_id: UUID, file: UploadFile = File(...)) -> dict[str, Any]:
    """Store a run artifact (dbt manifest.json / run_results.json, or anything
    else worth keeping past the node that produced it)."""
    content = await file.read(artifacts.max_bytes() + 1)
    try:
        with connection() as conn:
            store = artifacts.get_store()
            stored = artifacts.put_artifact(
                conn,
                store,
                run_id,
                file.filename or "",
                content,
                file.content_type or "application/octet-stream",
            )
            # dbt's own test results arrive in these two files and nowhere else.
            # Reading them here means a dbt-core stack needs no new integration
            # at all -- `push-artifacts` has been uploading them since Phase 02,
            # and they have been sitting unread. Never raises: a rejected upload
            # loses the artifact, and this can always be retried.
            if file.filename in (dbt_artifacts.RUN_RESULTS, dbt_artifacts.MANIFEST):
                stored["checks_imported"] = dbt_artifacts.import_stored(
                    conn, store, run_id
                )
            return stored
    except artifacts.ArtifactTooLarge as exc:
        raise HTTPException(status_code=413, detail=str(exc)) from exc
    except artifacts.ArtifactNameInvalid as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# Deliberately on `app` rather than the `api` router, and that placement is the
# whole point: the router carries `Depends(auth.require_token)`, and dbt Cloud
# cannot present a bearer token. Declared here it keeps the same URL while
# authenticating by HMAC instead — the same reasoning that puts `/health` and
# `/metrics` out here.
#
# This was found by pointing real dbt Cloud at a real tunnel. Every test passed
# beforehand because the test fixture leaves `DATASPINE_API_TOKENS` unset, so
# auth was open and the router dependency never fired. With auth on — which is
# every real deployment — the webhook returned 401 before its handler ever ran.
@app.post("/api/v1/dbt-cloud/webhook", status_code=202)
async def dbt_cloud_webhook(request: Request) -> dict[str, Any]:
    """Doorbell for a finished dbt Cloud job.

    The payload is used for exactly one thing — which run to go and fetch —
    and everything recorded comes from artifacts pulled with our own API token.
    A valid signature only proves someone holds the signing secret; it is not a
    reason to let the body write into the lineage graph.

    Unauthenticated by dataspine's own bearer scheme on purpose: dbt Cloud
    cannot present one. The HMAC *is* the authentication, so an unset secret
    means the endpoint refuses everything rather than accepting anything.

    It is the only route outside `/health`, `/ready` and `/metrics` that skips
    the bearer check, and it pays for that with a signature check instead.
    """
    body = await request.body()
    if not dbt_cloud.verify(body, request.headers.get("authorization")):
        # Deliberately the same answer for a bad signature and no secret at all.
        # Distinguishing them tells an unauthenticated caller which of the two
        # they are facing, which is a configuration detail they have no business
        # learning.
        raise HTTPException(status_code=401, detail="invalid signature")

    try:
        payload = json.loads(body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="body is not JSON") from exc

    run_id = dbt_cloud.run_id_from(payload if isinstance(payload, dict) else {})
    if not run_id:
        raise HTTPException(status_code=400, detail="no run id in payload")

    try:
        client, owns = dbt_cloud._client()
        try:
            with connection() as conn:
                result = dbt_cloud.ingest_run(conn, client, run_id)
                conn.commit()
        finally:
            if owns:
                client.close()
    except dbt_cloud.DbtCloudError as exc:
        # 502, not 500: the failure is upstream, and dbt Cloud retries on 5xx —
        # which is the behaviour we want while a token is being fixed.
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return result


@api.get("/runs/{run_id}/artifacts")
def get_artifacts(run_id: UUID) -> dict[str, Any]:
    with connection() as conn:
        return {"artifacts": artifacts.list_artifacts(conn, run_id)}


@api.get("/runs/{run_id}/artifacts/{name}")
def download_artifact(run_id: UUID, name: str) -> Response:
    try:
        with connection() as conn:
            content, meta = artifacts.get_artifact(conn, artifacts.get_store(), run_id, name)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="artifact not found") from exc
    except artifacts.ArtifactCorrupt as exc:
        # 500, not 404: the row exists and we failed to honour it. Silently
        # 404ing would hide storage corruption.
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="artifact content missing") from exc
    return Response(content=content, media_type=meta["content_type"])


# ------------------------------------------------------------------- monitors


@api.get("/monitors")
def get_monitors() -> dict[str, Any]:
    with connection() as conn:
        return {"monitors": monitors_mod.list_monitors(conn)}


@api.get("/monitors/{name}")
def get_monitor(name: str) -> dict[str, Any]:
    with connection() as conn:
        monitor = monitors_mod.get_monitor(conn, name)
        if monitor is None:
            raise HTTPException(status_code=404, detail="monitor not found")
        return {
            "monitor": monitor,
            "points": monitors_mod.recent_points(conn, monitor["id"]),
            "results": monitors_mod.recent_results(conn, monitor["id"]),
        }


@api.post("/monitors/check")
def post_monitor_check(
    monitor: str = Query("", description="Evaluate one monitor by name."),
    schedule: str = Query("", description="Only monitors on this schedule."),
) -> dict[str, Any]:
    """API-triggered evaluation, alongside the cron-driven `dataspine check`.

    Exists so an Airflow DAG can check a table the moment the task that builds it
    finishes, rather than waiting up to an hour for the next sweep — which is the
    difference between catching a bad load before downstream consumes it and
    catching it afterwards.
    """
    with connection() as conn:
        results = checks.check_all(conn, schedule=schedule or None, name=monitor or None)
        conn.commit()
    return {"checked": len(results), "results": results}


@api.get("/monitors/breaches/open")
def get_open_breaches() -> dict[str, Any]:
    with connection() as conn:
        return {"breaches": monitors_mod.open_breaches(conn)}


# ------------------------------------------------- lineage, catalog, incidents


@api.get("/entities")
def get_entities() -> dict[str, Any]:
    with connection() as conn:
        return {"entities": identity.entities(conn)}


# Named /graph, not /lineage: `/api/v1/lineage` is the OpenLineage ingest path
# that producers POST to, and sharing the prefix between "send me events" and
# "show me the graph" would be a confusion in the one place a stock producer is
# configured.
@api.get("/graph/{entity_id}")
def get_lineage(entity_id: int, depth: int = Query(2, le=lineage.MAX_DEPTH)) -> dict[str, Any]:
    with connection() as conn:
        graph = lineage.graph(conn, entity_id, depth=depth)
        if graph["focus"] is None:
            raise HTTPException(status_code=404, detail="entity not found")
        graph["columns"] = lineage.column_edges(conn, entity_id=entity_id)
        return graph


@api.get("/catalog")
def get_catalog() -> dict[str, Any]:
    with connection() as conn:
        return {"entries": catalog.list_entries(conn)}


@api.get("/catalog/search")
def get_catalog_search(q: str = Query("", description="Search term.")) -> dict[str, Any]:
    with connection() as conn:
        return {"results": catalog.search(conn, q)}


@api.get("/catalog/{entity_id}")
def get_catalog_entry(entity_id: int) -> dict[str, Any]:
    with connection() as conn:
        entry = catalog.get_entry(conn, entity_id)
        if entry is None:
            raise HTTPException(status_code=404, detail="entity not found")
        return {"entry": entry}


@api.post("/pr/impact")
async def post_pr_impact(request: Request) -> dict[str, Any]:
    """Downstream impact of changed dbt files, as a Markdown comment.

    Serves the GitHub Action, which runs on a CI runner with no dataspine
    install -- so the rendering happens here rather than shipping the lineage
    logic into a workflow script.
    """
    payload, error = await _read_json(request)
    if error:
        raise HTTPException(status_code=400, detail=error)
    paths = payload.get("paths") if isinstance(payload, dict) else payload
    if not isinstance(paths, list):
        raise HTTPException(status_code=400, detail="expected {'paths': [...]}")
    try:
        depth = (
            int(payload.get("depth", pr.DEFAULT_DEPTH))
            if isinstance(payload, dict) else pr.DEFAULT_DEPTH
        )
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=400, detail="depth must be an integer") from exc
    if not 1 <= depth <= 20:
        raise HTTPException(status_code=400, detail="depth must be between 1 and 20")

    with connection() as conn:
        return {
            "comment": pr.comment(conn, [str(p) for p in paths], depth=depth),
            "impact": pr.impact(conn, [str(p) for p in paths], depth=depth),
        }


@api.get("/costs")
def get_costs(limit: int = Query(100, le=500)) -> dict[str, Any]:
    with connection() as conn:
        return {
            "by_job": cost.by_job(conn, limit=limit),
            "unattributed": cost.unattributed(conn),
            "clusters": resources.list_clusters(conn),
        }


@api.get("/incidents")
def get_incidents() -> dict[str, Any]:
    with connection() as conn:
        return {"incidents": incidents.detect(conn)}


@api.post("/graph/resolve")
def post_resolve_lineage() -> dict[str, Any]:
    """Rebuild identity, lineage and the search index.

    Exposed so an Airflow DAG can refresh the graph at the end of a pipeline run
    rather than waiting for the next cron tick — the same reasoning as the
    monitor check endpoint.
    """
    with connection() as conn:
        stats = {**identity.resolve(conn), **lineage.resolve(conn)}
        stats["indexed"] = catalog.reindex(conn)
        conn.commit()
    return stats


# -------------------------------------------------- external DQ engine results


@api.post("/dq/{source}")
async def post_dq_results(source: str, request: Request) -> dict[str, Any]:
    """Accept results from a DQ engine we do not own (Snowflake DMFs, Databricks
    DQ rules).

    A push endpoint rather than a poller on purpose: whatever already runs those
    checks on a schedule can forward them, and we never need to hold warehouse
    credentials to read something the customer already has.
    """
    payload, error = await _read_json(request)
    if error:
        raise HTTPException(status_code=400, detail=error)
    rows = payload.get("results") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        raise HTTPException(status_code=400, detail="expected {'results': [...]}")
    with connection() as conn:
        imported = dq.import_results(conn, source=source, rows=rows)
        conn.commit()
    return {"imported": imported}


@api.get("/dq/failing")
def get_failing_dq() -> dict[str, Any]:
    with connection() as conn:
        return {"failing": dq.failing(conn)}


@api.get("/openapi.json", include_in_schema=False)
def openapi_schema() -> dict[str, Any]:
    """The schema, for anyone holding a token.

    Removing it entirely would punish the legitimate use -- generating a client,
    checking a field name -- to close an exposure that authentication already
    closes properly.
    """
    return get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )


# On `app` rather than the `api` router or the web UI's, and for the union of
# both their reasons. The router carries `Depends(auth.require_token)` and a
# browser following a Slack link presents no bearer token; the web router
# expects a session cookie the reader of a Slack alert very likely does not
# have. The signed key in the path *is* the authentication here, the same trade
# the dbt Cloud webhook makes above.
#
# What that key protects is worth naming: this page renders the compiled SQL of
# a failing check. The signature is what stops it being an oracle that anyone
# who can guess a `dedup_key` may read, which is why `agents.configured()`
# refuses to render a single button until a secret is set.
@app.get("/handoff/{key}", response_class=HTMLResponse)
def handoff(key: str, request: Request, target: str = Query(default=agents.CLAUDE_CLI)):
    """Hand one failing check to a coding agent.

    Deliberately a page rather than a redirect, even for the one target that is
    a real URL. A redirect to a scheme with no registered handler is a blank tab
    and no explanation, and the reader who most needs the briefing — the one
    without the agent installed — is exactly the one it would fail. The page
    always shows what dataspine knows, so the click is never wasted.
    """
    from .web import templates

    if target not in agents.KNOWN_TARGETS:
        raise HTTPException(status_code=400, detail="unknown target")

    unsigned = agents.unsign(key)
    if not unsigned:
        # The same answer for a forged signature and an unconfigured secret, for
        # the reason the webhook gives: which of the two they are facing is a
        # configuration detail an unauthenticated caller has no business
        # learning.
        raise HTTPException(status_code=404, detail="not found")

    event, dedup_key = unsigned
    with connection() as conn:
        briefing = agents.briefing_for(conn, event, dedup_key)

    context: dict[str, Any] = {"target": target, "briefing": briefing}
    if briefing:
        context["prompt"] = agents.prompt(briefing)
        context["deep_link"] = agents.claude_cli_url(briefing)
        context["app_link"] = agents.codex_app_url()
        context["command"] = agents.codex_command(briefing)
    return templates.TemplateResponse(request, "handoff.html", context)


# Also on `app`, and for a third variation on the same reason: Slack presents
# neither a bearer token nor a session cookie, and signs its callbacks instead.
@app.post("/slack/interactivity")
async def slack_interactivity(request: Request) -> Response:
    """Acknowledge a Slack button click. Does nothing else, on purpose.

    Every button in an `actions` block sends Slack an interaction payload when
    clicked — **including a `url` button that Slack itself is already opening**.
    An app with no Request URL configured cannot answer, so Slack renders a
    warning triangle beside the message. The buttons work; the alert just looks
    broken, which for an alerting product is its own kind of broken.

    So this endpoint exists to say 200 and nothing more. It deliberately does not
    act on the payload: the click's actual effect is the URL Slack is opening in
    the reader's browser, and a second, server-side effect fired from the same
    click would be a surprise nobody asked for. If cloud dispatch ever lands
    (D15), this is where it hangs — behind a real decision, not by accident.

    The signature is the authentication, and an unset signing secret refuses
    everything rather than accepting anything — the rule the dbt Cloud webhook
    already follows.
    """
    body = await request.body()
    if not slack.verify_signature(
        body,
        request.headers.get("x-slack-request-timestamp"),
        request.headers.get("x-slack-signature"),
    ):
        raise HTTPException(status_code=401, detail="invalid signature")

    # Slack retries on anything that is not a prompt 200, and a retried click is
    # a second notification for a reader who clicked once.
    return Response(status_code=200)


@app.get("/metrics", response_class=PlainTextResponse)
def metrics() -> str:
    """Prometheus exposition.

    Unauthenticated, like `/health`, and for the same reason: a scraper is
    infrastructure, not a user, and most Prometheus deployments cannot present a
    bearer token per target. What it exposes is counts and health — no job
    names, no SQL, no dataset identities — so it reveals volume, not content.
    Operators who disagree can keep it off the ingress; it is a separate path
    precisely so that is a one-line decision.

    Never raises. A metrics endpoint that fails with the database has removed
    the signal at the exact moment it was needed, so anything unreadable is
    omitted and the rest is still served.
    """
    health: dict[str, Any] | None = None
    storage: dict[str, Any] | None = None
    breaches: int | None = None
    undelivered: dict[str, Any] | None = None
    try:
        with connection() as conn:
            health = queries.ingest_health(conn)
            storage = upkeep.status(conn)
            row = conn.execute(
                "select count(*) as n from monitors "
                "where enabled and last_status = 'breach'"
            ).fetchone()
            breaches = row["n"] if row else 0
            undelivered = {
                "alerts": conn.execute(
                    "select count(*) as n from alerts where not delivered "
                    "and created_at > now() - interval '24 hours'"
                ).fetchone()["n"],
                "notifications": conn.execute(
                    "select count(*) as n from notifications where not delivered "
                    "and created_at > now() - interval '24 hours'"
                ).fetchone()["n"],
            }
    except Exception as exc:  # noqa: BLE001 - see docstring
        log.warning("metrics: database unreadable, serving process counters only: %s", exc)

    values = metrics_mod.collect(
        queue_stats=ingest_queue.get_queue().stats(),
        health=health,
        storage=storage,
        breaches=breaches,
        undelivered=undelivered,
        version=__version__,
    )
    return metrics_mod.render(values, version=__version__)


@api.get("/health/ingest")
def get_ingest_health() -> dict[str, Any]:
    with connection() as conn:
        health = queries.ingest_health(conn)
    # Queue stats live alongside the storage counts on purpose: a deployment
    # that is shedding events is exactly as important to notice as one whose
    # runs are unstitched, and a number you have to navigate to is a number
    # nobody looks at.
    health.update(ingest_queue.get_queue().stats())
    # Partition headroom. A lapse here is the failure that has no symptom:
    # writes keep succeeding into the DEFAULT backstop while the table quietly
    # stops being partitioned, so it has to be reported rather than inferred.
    health["storage"] = upkeep.status()
    return health


app.include_router(api)

# The web UI lives in the same process and the same container (ADR-002).
from . import web  # noqa: E402  -- imported last to avoid a circular import

web.mount(app)
