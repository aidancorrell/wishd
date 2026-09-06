"""Reading dbt's own record of what it did.

`run_results.json` is the same file in every dbt deployment there is. dbt-core
writes it to `target/` on whatever machine ran the job; dbt Cloud serves that
identical artifact from its Admin API. So one reader serves both worlds, and the
only thing that differs is how the bytes arrive.

That is the whole reason this module exists as a parser rather than as two
integrations. A shop running dbt-core on EMR and a shop running dbt Cloud on
Snowflake are, from here, the same shop.

It produces two different things from one parse, and they are wanted in
different combinations:

  **Test results** (`test_results`) — the assertions dbt made about the data.
  Wanted by *everyone*, because nothing else in dataspine knows about them: the
  monitors watch freshness, volume, schema and column statistics, and a
  `not_null` on a column nobody thought to profile is invisible to all of them.

  **Run events** (`events`) — the invocation as an OpenLineage run tree. Wanted
  only where no such tree already exists. A dbt-core stack already emits these
  from `dbt-ol`, and synthesising a second copy would double every run. dbt Cloud
  emits nothing, so for that world this is what makes it an ordinary producer:
  the run tree, the live feed, run failures and lineage all start working with
  no special case anywhere downstream.

Three things about dbt's format that are not guessable and are each a test:

  **A test result names no table.** `relation_name` is null on test nodes. The
  table under test is reachable only through `attached_node` (or `depends_on`)
  into the *manifest*, which is why the manifest is worth having and why a
  manifest-less parse degrades to tests it cannot attribute.

  **`severity` arrives in two casings.** dbt writes the default as `"ERROR"` and
  a user-specified one as they typed it, so the real fixture in `tests/fixtures`
  contains both `"ERROR"` and `"warn"`. Comparing without normalising works
  until the first project that sets `severity: error` in lower case.

  **`warn` is not a failure to page about.** A test whose severity is `warn` is
  one whose author said, in writing, that they did not want waking. It failed —
  it is recorded as a failure — but it must not become a notification, or the
  first thing a team does is mute the channel.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

log = logging.getLogger("dataspine.dbt_artifacts")

PRODUCER = "https://github.com/aidancorrell/wishd"
SPEC = "https://openlineage.io/spec/2-0-2/OpenLineage.json"
FACET_BASE = "https://openlineage.io/spec/facets"

# A fixed root for deriving run ids, so re-ingesting the same artifacts is
# idempotent rather than duplicating an entire invocation. dbt's own
# `invocation_id` is the natural seed: it is a uuid dbt already generated per
# run, and it is in both artifacts.
UUID_ROOT = uuid5(NAMESPACE_URL, "https://dataspine.dev/dbt")

# dbt's node status vocabulary, mapped onto run state. `warn` is COMPLETED
# because the *node* ran fine; what warned is the assertion it made.
NODE_STATE = {
    "success": "COMPLETED",
    "pass": "COMPLETED",
    "warn": "COMPLETED",
    "error": "FAILED",
    "fail": "FAILED",
    "runtime error": "FAILED",
    "skipped": "ABORTED",
    "partial success": "COMPLETED",
}

# The same vocabulary mapped onto a check result. Note `warn` is a *failure*
# here and COMPLETED above, and the difference is the point: the test ran
# perfectly well and the data did not satisfy it.
CHECK_STATUS = {
    "pass": "pass",
    "success": "pass",
    "fail": "fail",
    "warn": "fail",
    "error": "error",
    "runtime error": "error",
    # A skipped test is not evidence about the data either way, and recording it
    # as a pass would let an upstream failure look like a clean bill of health.
    "skipped": None,
}

TEST_TYPES = ("test", "unit_test")
MODEL_TYPES = ("model", "snapshot", "seed")


class DbtArtifactError(ValueError):
    """Artifacts that cannot be read at all."""


@dataclass(frozen=True)
class Node:
    """One thing dbt did, joined across both artifacts."""

    unique_id: str
    resource_type: str
    name: str
    status: str
    started_at: datetime | None
    ended_at: datetime | None
    execution_time: float | None
    message: str | None
    failures: int | None
    relation: str | None
    depends_on: tuple[str, ...]
    attached_node: str | None
    test_type: str | None
    column: str | None
    severity: str
    # The SQL dbt actually ran, from the manifest. For a test this *is* the
    # investigation query -- `select ... where order_id is null` names the
    # offending rows -- which is why it survives into the alert rather than
    # being reconstructed from the test's name and column by a worse guess.
    compiled_code: str | None = None
    # The warehouse's own id for the statement, from `adapter_response`. Worth
    # far more than it looks on Snowflake: it deep-links to a page that already
    # holds the SQL, the profile and an "Open in Workspaces" button, so the
    # reader gets the query loaded into an editor without dataspine ever holding
    # a warehouse credential or writing anything into their account.
    query_id: str | None = None

    @property
    def state(self) -> str:
        return NODE_STATE.get(self.status, "UNKNOWN")

    @property
    def is_test(self) -> bool:
        return self.resource_type in TEST_TYPES

    @property
    def warn_only(self) -> bool:
        """The author said in writing that they did not want waking for this."""
        return self.severity == "warn"


@dataclass(frozen=True)
class Invocation:
    invocation_id: str
    project: str
    adapter: str
    dbt_version: str
    generated_at: datetime
    started_at: datetime
    nodes: tuple[Node, ...]
    # `dbt build` vs `dbt run` vs `dbt test` -- worth surfacing because teams
    # split their pipeline differently and "3 failures" reads very differently
    # for a test-only job than for a build.
    command: str = ""
    elapsed: float | None = None

    @property
    def failed(self) -> tuple[Node, ...]:
        """Everything worth putting in front of a human, worst first.

        Models before tests: a model that did not build is why its tests did not
        run, and reading the other way round means scrolling past the symptoms to
        find the cause.
        """
        rank = {"model": 0, "snapshot": 0, "seed": 0}
        worth = [
            n for n in self.nodes
            if n.state == "FAILED" or (n.is_test and n.status in ("fail", "warn"))
        ]
        return tuple(
            sorted(worth, key=lambda n: (n.warn_only, rank.get(n.resource_type, 1), n.name))
        )

    @property
    def tests(self) -> tuple[Node, ...]:
        return tuple(n for n in self.nodes if n.is_test)

    @property
    def models(self) -> tuple[Node, ...]:
        return tuple(n for n in self.nodes if n.resource_type in MODEL_TYPES)


# ------------------------------------------------------------------- parsing


def parse(run_results: dict[str, Any], manifest: dict[str, Any] | None = None) -> Invocation:
    """Join `run_results.json` with `manifest.json` into one invocation.

    The manifest is optional but wanted. Without it a test result cannot name the
    table it asserted on -- `relation_name` is null on test nodes and the link
    lives only in the manifest -- so those tests are parsed and then dropped by
    `test_results` rather than being attributed to a guess.
    """
    if not isinstance(run_results, dict) or "results" not in run_results:
        raise DbtArtifactError("not a dbt run_results.json: no `results` key")

    meta = run_results.get("metadata") or {}
    manifest_meta = (manifest or {}).get("metadata") or {}
    manifest_nodes = (manifest or {}).get("nodes") or {}

    generated_at = _time(meta.get("generated_at")) or datetime.now(UTC)

    nodes = []
    for result in run_results["results"]:
        if not isinstance(result, dict) or not result.get("unique_id"):
            continue
        nodes.append(
            _node(result, manifest_nodes.get(result["unique_id"]) or {}, fallback=generated_at)
        )

    starts = [n.started_at for n in nodes if n.started_at]
    return Invocation(
        invocation_id=str(meta.get("invocation_id") or generated_at.isoformat()),
        # `project_name` lives only in the manifest; the unique_id's middle
        # segment carries it too, which is what makes a manifest-less parse still
        # produce sensibly-named jobs rather than "unknown".
        project=str(manifest_meta.get("project_name") or _project_from(nodes) or "dbt"),
        adapter=str(manifest_meta.get("adapter_type") or "dbt"),
        dbt_version=str(meta.get("dbt_version") or ""),
        generated_at=generated_at,
        started_at=min(starts) if starts else generated_at,
        nodes=tuple(nodes),
        command=str((run_results.get("args") or {}).get("which") or ""),
        elapsed=_number(run_results.get("elapsed_time")),
    )


def _project_from(nodes: list[Node]) -> str | None:
    for node in nodes:
        parts = node.unique_id.split(".")
        if len(parts) >= 3:
            return parts[1]
    return None


def _node(result: dict[str, Any], manifest_node: dict[str, Any], *, fallback: datetime) -> Node:
    unique_id = str(result["unique_id"])
    timings = {
        t.get("name"): t for t in (result.get("timing") or []) if isinstance(t, dict)
    }
    # `execute` is when the warehouse did the work; `compile` brackets the whole
    # node. Preferring execute's start would report a model as beginning after
    # dbt had already spent time on it.
    started = _time((timings.get("compile") or timings.get("execute") or {}).get("started_at"))
    ended = _time((timings.get("execute") or timings.get("compile") or {}).get("completed_at"))

    config = manifest_node.get("config") or {}
    test_metadata = manifest_node.get("test_metadata") or {}
    kwargs = test_metadata.get("kwargs") or {}

    return Node(
        unique_id=unique_id,
        # The unique_id prefix is a stable dbt convention, so resource_type
        # survives a missing manifest.
        resource_type=str(manifest_node.get("resource_type") or unique_id.split(".")[0]),
        name=str(manifest_node.get("name") or unique_id.rsplit(".", 1)[-1]),
        status=str(result.get("status") or "").lower(),
        started_at=started,
        ended_at=ended or fallback,
        execution_time=_number(result.get("execution_time")),
        message=(str(result["message"]) if result.get("message") else None),
        failures=int(result["failures"]) if isinstance(result.get("failures"), int) else None,
        relation=_model_relation(result, manifest_node),
        depends_on=tuple((manifest_node.get("depends_on") or {}).get("nodes") or ()),
        attached_node=manifest_node.get("attached_node"),
        test_type=(str(test_metadata["name"]) if test_metadata.get("name") else None),
        column=(
            str(manifest_node.get("column_name") or kwargs.get("column_name"))
            if (manifest_node.get("column_name") or kwargs.get("column_name"))
            else None
        ),
        # dbt writes the default as "ERROR" and a user-set one as they typed it.
        severity=str(config.get("severity") or "error").lower(),
        # Present only when the manifest was written after a compile, which is
        # every manifest a finished run produces. Absent when only
        # run_results.json was uploaded, and the alert simply omits the query.
        compiled_code=_compiled(manifest_node),
        query_id=_query_id(result),
    )


def _query_id(result: dict[str, Any]) -> str | None:
    """The warehouse statement id, when the adapter reported one.

    Snowflake and Databricks do; Postgres does not, and dbt still writes an
    `adapter_response` for it with no id inside. Absent is the ordinary case
    rather than a fault, and the alert simply carries one link fewer.
    """
    response = result.get("adapter_response")
    if not isinstance(response, dict):
        return None
    value = response.get("query_id")
    return str(value).strip() or None if value else None


def _compiled(manifest_node: dict[str, Any]) -> str | None:
    """dbt's compiled SQL, stripped of the blank lines its templating leaves.

    A rendered test opens with several empty lines where the `{% test %}` block
    and its config were, and pasting that into a warehouse editor buries the
    query below the fold for no reason.
    """
    raw = manifest_node.get("compiled_code")
    if not isinstance(raw, str) or not raw.strip():
        return None
    return "\n".join(line for line in raw.splitlines() if line.strip()).strip()


# Every adapter quotes identifiers its own way -- Postgres and Snowflake with
# double quotes, Spark, Databricks and BigQuery with backticks, SQL Server with
# brackets -- and dbt hands the relation back exactly as the adapter renders it.
QUOTES = '"`[]'


def _model_relation(result: dict[str, Any], manifest_node: dict[str, Any]) -> str | None:
    """The table a node *writes*, and deliberately nothing for a test.

    dbt gives every test node a relation in a `dbt_test__audit` schema -- where
    `store_failures` would write the offending rows -- and it is emphatically not
    the table under test. Carrying it here would invite exactly one wrong use,
    and that use creates a dataset entity per test: a catalog with a hundred
    `not_null_*` tables in it. `tested_relation` is the only correct way to ask
    what a test asserted on, so it is made the only way.
    """
    # Inferred the same way the Node is, so the guard still holds when only
    # run_results.json was uploaded and there is no manifest to consult.
    resource_type = manifest_node.get("resource_type") or str(
        result.get("unique_id") or ""
    ).split(".")[0]
    if str(resource_type) in TEST_TYPES:
        return None
    raw = result.get("relation_name") or manifest_node.get("relation_name")
    if not raw:
        parts = [
            manifest_node.get("database"),
            manifest_node.get("schema"),
            manifest_node.get("alias") or manifest_node.get("name"),
        ]
        present = [str(p) for p in parts if p]
        raw = ".".join(present) if len(present) >= 2 else None
    return _unquote(raw) if raw else None


def _unquote(relation: str) -> str:
    """`"db"."schema"."table"` -> `db.schema.table`.

    Not cosmetic. Every table name in this system is matched on its final
    segment, and that rule reduces the quoted form to `table"` -- with the quote
    still attached, so a dbt test would never match the table every other
    producer reports. Real dbt output is quoted; the fixture in `tests/fixtures`
    is where that was found.
    """
    return ".".join(part.strip().strip(QUOTES) for part in str(relation).split("."))


def _time(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ------------------------------------------------------------- test results


def tested_relation(invocation: Invocation, test: Node) -> str | None:
    """The table a test asserted on.

    Test nodes carry no `relation_name`, so this walks `attached_node` (or, on
    older dbt, the first model in `depends_on`) into the invocation's own nodes.
    A test whose model was not in this run -- `dbt test --select` on its own --
    resolves to nothing, and is dropped rather than attributed to a guess.
    """
    by_id = {n.unique_id: n for n in invocation.nodes}
    candidates = [test.attached_node, *test.depends_on]
    for candidate in candidates:
        node = by_id.get(candidate or "")
        if node is not None and node.relation:
            return node.relation
    # The model is often not in `results` (it was not rebuilt), but its unique_id
    # still names it, and the leaf-segment rule downstream matches on that.
    for candidate in candidates:
        if candidate and candidate.startswith("model."):
            return candidate.rsplit(".", 1)[-1]
    return None


def test_results(invocation: Invocation) -> list[dict[str, Any]]:
    """dbt's assertions, in the shape `dq.import_results` stores.

    Skipped tests are omitted entirely: a test that did not run is not evidence
    about the data, and recording it as a pass would let an upstream failure read
    as a clean bill of health.
    """
    rows = []
    for test in invocation.tests:
        status = CHECK_STATUS.get(test.status)
        if status is None:
            continue
        table = tested_relation(invocation, test)
        if not table:
            log.debug("dbt test %s asserts on nothing we can name", test.unique_id)
            continue
        rows.append(
            {
                "table": table,
                "check": test.name,
                "status": status,
                # The failing row count, which is the number anyone asks for
                # first: "how bad?" before "which rows?".
                "value": test.failures,
                "measured_at": test.ended_at or invocation.generated_at,
                "details": {
                    "unique_id": test.unique_id,
                    "test_type": test.test_type,
                    "column": test.column,
                    "severity": test.severity,
                    "message": test.message,
                    "dbt_version": invocation.dbt_version,
                    "invocation_id": invocation.invocation_id,
                    # Carried into the alert so the reader is handed the query
                    # that found the bad rows rather than being told to go and
                    # write it. Only tests get one stored: a model's compiled
                    # SQL can be thousands of lines, and it is not an assertion.
                    "compiled_sql": test.compiled_code,
                    # What turns the alert into a warehouse deep link.
                    "query_id": test.query_id,
                },
            }
        )
    return rows


# ------------------------------------------------------- synthesised events


def run_id_for(invocation_id: str, unique_id: str = "") -> UUID:
    """Deterministic, so re-ingesting the same artifacts changes nothing."""
    return uuid5(UUID_ROOT, f"{invocation_id}/{unique_id}")


def root_job_name(invocation: Invocation, job_name: str | None = None) -> str:
    """What the invocation is called in the run tree.

    `job_name` matters more than it looks. Most teams run several dbt Cloud jobs
    against one project -- a build, an hourly incremental, a test-only pass --
    and naming every one of them `<project>.run` makes them a single job here.
    Job-level monitors would then average unrelated workloads together, the live
    feed would show one row for all of them, and a `job:` route could not tell
    them apart. The project stays as a prefix so two projects may each have a
    job called "Nightly".
    """
    return f"{invocation.project}.{job_name}" if job_name else f"{invocation.project}.run"


def events(
    invocation: Invocation,
    *,
    namespace: str | None = None,
    job_namespace: str = "dbt",
    job_name: str | None = None,
    root_run_id: UUID | None = None,
    run_facets: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """The invocation as OpenLineage events: one root run, one run per node.

    Named the way `dbt-ol` names things — the root is `<project>.run` and a node
    is `<project>.<unique_id>` — because a shop running dbt-core *and* dbt Cloud
    should see one vocabulary, not two. That naming is what the run tree in the
    README already shows, so nothing downstream has to learn a second convention.

    Only for producers that emit no OpenLineage of their own. Calling this for a
    dbt-core stack that already runs `dbt-ol` would double every run.

    `root_run_id` overrides the identity derived from `invocation_id`, and dbt
    Cloud needs it. An invocation id only exists once artifacts do -- that is,
    once the run has finished -- so a run reported as *in progress* has no way to
    compute the same id. Keying dbt Cloud runs off their dbt Cloud run id instead
    means the placeholder emitted while the job is running and the full tree
    emitted when it finishes are the same run, rather than two.

    `run_facets` is how a courier attaches what only it knows. The artifacts say
    nothing about where they were produced -- a `run_results.json` from dbt Cloud
    is byte-identical to one from `dbt-core` -- so the dbt Cloud run's own URL
    can only come from the module that fetched it. It goes on the root run, and
    `links.py` turns it back into "open in dbt Cloud".
    """
    namespace = namespace or f"{invocation.adapter}://{invocation.project}"
    root_run = root_run_id or run_id_for(invocation.invocation_id)
    root_name = root_job_name(invocation, job_name)

    failed = any(n.state == "FAILED" for n in invocation.nodes)
    out: list[dict[str, Any]] = [
        _event(
            "START", invocation.started_at, root_run, job_namespace, root_name,
            job_facets=_job_type("DBT", "JOB"),
            run_facets={**_engine(invocation), **(run_facets or {})},
        )
    ]

    for node in invocation.nodes:
        out += _node_events(
            node, invocation,
            root_run=root_run, root_name=root_name,
            namespace=namespace, job_namespace=job_namespace,
        )

    out.append(
        _event(
            "FAIL" if failed else "COMPLETE",
            invocation.generated_at, root_run, job_namespace, root_name,
            job_facets=_job_type("DBT", "JOB"),
        )
    )
    return out


def _node_events(
    node: Node,
    invocation: Invocation,
    *,
    root_run: UUID,
    root_name: str,
    namespace: str,
    job_namespace: str,
) -> list[dict[str, Any]]:
    if node.status == "skipped":
        # A skipped node did not run. Emitting START/ABORT for it would put a
        # run in the tree that never existed, and every duration over that tree
        # would then be measuring dbt's scheduling rather than any work.
        return []

    run = run_id_for(invocation.invocation_id, node.unique_id)
    name = f"{invocation.project}.{node.unique_id}"
    started = node.started_at or invocation.started_at
    ended = node.ended_at or invocation.generated_at
    parent = _parent(root_run, job_namespace, root_name)
    job_facets = _job_type("DBT", "TEST" if node.is_test else "MODEL")

    inputs: list[dict[str, Any]] = []
    outputs: list[dict[str, Any]] = []
    if node.is_test:
        table = tested_relation(invocation, node)
        if table:
            inputs = [{"namespace": namespace, "name": table, "facets": {}}]
    elif node.relation:
        outputs = [{"namespace": namespace, "name": node.relation, "facets": {}}]

    terminal = {"COMPLETED": "COMPLETE", "FAILED": "FAIL", "ABORTED": "ABORT"}.get(
        node.state, "COMPLETE"
    )
    run_facets: dict[str, Any] = {}
    if node.state == "FAILED" and node.message:
        run_facets = {
            "errorMessage": {
                **_base(f"{FACET_BASE}/1-0-0/ErrorMessageRunFacet.json"),
                "message": node.message,
                "programmingLanguage": "SQL",
            }
        }

    return [
        _event("START", started, run, job_namespace, name,
               run_facets=parent, job_facets=job_facets,
               inputs=inputs, outputs=outputs),
        _event(terminal, ended, run, job_namespace, name,
               run_facets={**parent, **run_facets}, job_facets=job_facets,
               inputs=inputs, outputs=outputs),
    ]


def _base(schema_url: str) -> dict[str, str]:
    return {"_producer": PRODUCER, "_schemaURL": schema_url}


def _job_type(integration: str, job_type: str) -> dict[str, Any]:
    return {
        "jobType": {
            **_base(f"{FACET_BASE}/2-0-4/JobTypeJobFacet.json"),
            "processingType": "BATCH",
            "integration": integration,
            "jobType": job_type,
        }
    }


def _engine(invocation: Invocation) -> dict[str, Any]:
    return {
        "processing_engine": {
            **_base(f"{FACET_BASE}/1-1-1/ProcessingEngineRunFacet.json"),
            "name": "dbt",
            "version": invocation.dbt_version,
            "openlineageAdapterVersion": "dataspine-synthesised",
        }
    }


def _parent(root_run: UUID, namespace: str, name: str) -> dict[str, Any]:
    return {
        "parent": {
            **_base(f"{FACET_BASE}/1-2-0/ParentRunFacet.json"),
            "run": {"runId": str(root_run)},
            "job": {"namespace": namespace, "name": name},
            "root": {
                "run": {"runId": str(root_run)},
                "job": {"namespace": namespace, "name": name},
            },
        }
    }


def _event(
    event_type: str,
    when: datetime,
    run_id: UUID,
    namespace: str,
    name: str,
    run_facets: dict[str, Any] | None = None,
    job_facets: dict[str, Any] | None = None,
    inputs: list[dict[str, Any]] | None = None,
    outputs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "eventTime": when.isoformat(),
        "producer": PRODUCER,
        "schemaURL": f"{SPEC}#/$defs/RunEvent",
        "eventType": event_type,
        "run": {"runId": str(run_id), "facets": run_facets or {}},
        "job": {"namespace": namespace, "name": name, "facets": job_facets or {}},
        "inputs": inputs or [],
        "outputs": outputs or [],
    }


# ----------------------------------------------------------- stored artifacts

RUN_RESULTS = "run_results.json"
MANIFEST = "manifest.json"


def import_stored(conn: Any, store: Any, run_id: Any) -> int:
    """Read the dbt artifacts already stored for a run and record their tests.

    The dbt-core path, and it needs no new integration at all: `dataspine
    push-artifacts` has been uploading both files since Phase 02, and they have
    been sitting in the artifact store unread. Only the *tests* are taken —
    dbt-core stacks already emit their run tree through `dbt-ol`, and
    synthesising a second copy would double every run.

    Returns rows written; 0 when there is nothing to read. Never raises: an
    unreadable artifact must not fail the upload that carried it, because a
    rejected upload loses the artifact entirely and this can always be retried.
    """
    from . import artifacts as artifacts_mod
    from . import dq

    try:
        run_results = _stored_json(conn, store, artifacts_mod, run_id, RUN_RESULTS)
        if run_results is None:
            return 0
        manifest = _stored_json(conn, store, artifacts_mod, run_id, MANIFEST)
        invocation = parse(run_results, manifest)
        rows = test_results(invocation)
        written = dq.import_results(conn, source="dbt", rows=rows) if rows else 0

        # dbt-core gets the same job message dbt Cloud does. The invocation is
        # the job here -- there is no dbt Cloud job to name it after, so the
        # project name stands in.
        from . import notify

        notify.announce_dbt_job(conn, invocation)
        return written
    except Exception as exc:  # noqa: BLE001 - see docstring
        log.warning("could not read dbt artifacts for run %s: %s", run_id, exc)
        return 0


def _stored_json(conn: Any, store: Any, artifacts_mod: Any, run_id: Any, name: str):
    import json

    try:
        found = artifacts_mod.get_artifact(conn, store, run_id, name)
    except Exception:  # noqa: BLE001 - a missing manifest is normal, not an error
        return None
    if not found:
        return None
    # `get_artifact` returns (bytes, row); the bytes are what we came for, and
    # they have already been checked against their digest.
    content = found[0] if isinstance(found, tuple) else found
    return json.loads(content)
