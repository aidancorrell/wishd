"""Deep links from a run back to the system that produced it.

dataspine will never hold every detail — the Spark UI owns the stage breakdown,
Airflow owns the task log — so the run page's job is to hand you straight there.

Everything below is derived from facets real producers actually send, verified
against the captured fixtures. Notably we do **not** invent URLs: if a producer
gives us a hostname we use it, and if it does not, a link requires explicit
configuration. A guessed link that 404s is worse than no link, because it costs
someone a click and their trust in the page.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from .config import env

AIRFLOW_BASE_ENV = "DATASPINE_AIRFLOW_BASE_URL"
SPARK_HISTORY_ENV = "DATASPINE_SPARK_HISTORY_URL"

# The Snowsight account URL, e.g. `https://app.snowflake.com/myorg/ab12345`.
# Configuration rather than derivation: the organisation and account names are
# not in any facet a producer sends, and the account *locator* that does appear
# in some of them addresses a different URL shape.
SNOWFLAKE_ACCOUNT_ENV = "DATASPINE_SNOWFLAKE_ACCOUNT_URL"


def _dig(facets: Any, *path: str) -> Any:
    """Walk nested dicts, tolerating anything that is not shaped as expected."""
    current = facets
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _link(label: str, url: str | None = None, value: str | None = None) -> dict[str, Any]:
    return {"label": label, "url": url, "value": value}


def run_links(integration: str | None, run_facets: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Links and copyable references for one run. Never raises."""
    facets = run_facets if isinstance(run_facets, dict) else {}
    integration = (integration or "").upper()

    if integration == "SPARK":
        return _spark_links(facets)
    if integration == "AIRFLOW":
        return _airflow_links(facets)
    if integration == "DBT":
        return _dbt_links(facets)
    return []


def _spark_links(facets: dict[str, Any]) -> list[dict[str, Any]]:
    details = facets.get("spark_applicationDetails")
    out: list[dict[str, Any]] = []

    ui = _dig(details, "uiWebUrl")
    if isinstance(ui, str) and ui.startswith("http"):
        out.append(_link("Spark UI", url=ui))

    app_id = _dig(details, "applicationId")
    if isinstance(app_id, str) and app_id:
        # Not a link by itself, but it is what you paste into the EMR console
        # and what names the event log in S3.
        out.append(_link("Application ID", value=app_id))

        history = env.get(SPARK_HISTORY_ENV, "").rstrip("/")
        if history:
            # Survives the cluster: the live UI dies with the application, the
            # History Server is where a failed EMR run is actually autopsied.
            out.append(_link("Spark History", url=f"{history}/history/{app_id}"))

    master = _dig(details, "master")
    if isinstance(master, str) and master:
        out.append(_link("Master", value=master))
    return out


def _airflow_links(facets: dict[str, Any]) -> list[dict[str, Any]]:
    airflow = facets.get("airflow") or facets.get("airflowDagRun") or {}
    dag_id = _dig(airflow, "dag", "dag_id") or _dig(airflow, "dagRun", "dag_id")
    run_id = _dig(airflow, "dagRun", "run_id")

    out: list[dict[str, Any]] = []
    if isinstance(run_id, str) and run_id:
        out.append(_link("DAG run", value=run_id))

    base = env.get(AIRFLOW_BASE_ENV, "").rstrip("/")
    if base and isinstance(dag_id, str) and dag_id:
        # Airflow 3 grid URL. Deliberately links the DAG run rather than a task
        # instance: the run-level page works whether or not this event came from
        # a task, and Airflow redirects sensibly from there.
        # Percent-encode: Airflow run ids routinely contain ':' and '+'
        # (`manual__2026-08-07T23:28:41.949748+00:00_...`), which silently
        # produce a wrong or 404ing link if dropped into a path raw.
        url = f"{base}/dags/{quote(dag_id, safe='')}"
        if isinstance(run_id, str) and run_id:
            url = f"{url}/runs/{quote(run_id, safe='')}"
        out.append(_link("Airflow", url=url))

    try_number = _dig(airflow, "taskInstance", "try_number")
    if isinstance(try_number, int) and try_number > 1:
        # Surfaced only when it is interesting: a retried task is a signal.
        out.append(_link("Attempt", value=str(try_number)))
    return out


def _dbt_links(facets: dict[str, Any]) -> list[dict[str, Any]]:
    dbt_run = facets.get("dbt_run") or {}
    out: list[dict[str, Any]] = []

    # dbt Cloud first: it is the only dbt link that opens a UI, so it belongs
    # ahead of the references someone would otherwise paste into a search box.
    cloud = facets.get("dbt_cloud") or {}
    href = _dig(cloud, "href")
    if isinstance(href, str) and href.startswith("http"):
        # Stated by dbt Cloud in the run payload, never assembled here -- the
        # account's cell lives in the hostname and a guessed one 401s.
        out.append(_link("dbt Cloud", url=href))
    cloud_run = _dig(cloud, "runId")
    if isinstance(cloud_run, str) and cloud_run and not href:
        # No URL to be had, but the id is what the Admin API and the support
        # ticket both ask for.
        out.append(_link("dbt Cloud run", value=cloud_run))

    invocation = _dig(dbt_run, "invocation_id")
    if isinstance(invocation, str) and invocation:
        # dbt has no UI to link to. The invocation id is what you grep logs and
        # artifact buckets for, so it earns its place as a copyable reference.
        out.append(_link("dbt invocation", value=invocation))

    project = _dig(dbt_run, "project_name")
    if isinstance(project, str) and project:
        out.append(_link("Project", value=project))

    version = _dig(facets, "dbt_version", "version")
    if isinstance(version, str) and version:
        out.append(_link("dbt version", value=version))
    return out


# ------------------------------------------------------------------ warehouse


def snowflake_table(relation: str | None) -> dict[str, Any] | None:
    """A Snowsight link to the table a check asserted on, or None.

    **Opt-in, and currently called from nowhere.** `notify.py` prefers
    `snowflake_query` — which carries the SQL, the results and the profile, so it
    answers strictly more — and offering both put two Snowflake links on one
    context line for no gain. The call sites are commented in place rather than
    deleted because this is the only warehouse link available on an adapter that
    reports no `query_id`; see the notes there before wiring it back.

    Requires `DATASPINE_SNOWFLAKE_ACCOUNT_URL`, because nothing a producer sends
    names the Snowsight account. Without it there is no link, which is the rule
    the rest of this module follows.

    **Identifiers are upper-cased unless dbt quoted them.** This is measured, not
    assumed: the same URL with `fct_orders` renders "Table not found" and with
    `FCT_ORDERS` renders the table. Snowflake folds unquoted identifiers to upper
    case and Snowsight addresses the *stored* name, while dbt reports the
    relation as it was written — so an unquoted `analytics.public.fct_orders`
    must be folded here, and a quoted `"fct_orders"` must emphatically not be.
    """
    base = env.get(SNOWFLAKE_ACCOUNT_ENV, "").strip().rstrip("/")
    if not base or not relation:
        return None

    parts = [p for p in str(relation).split(".") if p.strip()]
    if len(parts) != 3:
        # A bare table name has no database or schema to address, and guessing
        # either produces a link to a table that does not exist.
        return None

    database, schema, table = (_snowflake_identifier(p) for p in parts)
    url = (
        f"{base}/#/data/databases/{quote(database, safe='')}"
        f"/schemas/{quote(schema, safe='')}/table/{quote(table, safe='')}"
    )
    return _link(f"{table} in Snowsight", url=url)


def _snowflake_identifier(part: str) -> str:
    part = part.strip()
    if len(part) >= 2 and part[0] == '"' and part[-1] == '"':
        # Quoted: dbt preserved the case on purpose, so we must too.
        return part[1:-1]
    return part.upper()


def snowflake_query(query_id: str | None) -> dict[str, Any] | None:
    """A Snowsight link to the statement the warehouse actually ran, or None.

    This is the most useful link dataspine can produce about a failing test, and
    it costs nothing to produce. dbt records the warehouse's `query_id` in
    `adapter_response`; Snowsight's query page addresses that id directly and
    already offers **Copy SQL text** and **Open in Workspaces** — so the reader
    lands on the failing query, with its profile and its results, and is one
    click from an editor with the SQL loaded.

    Snowsight's "Open in Workspaces" button first stores SQL in its own
    sessionStorage, then navigates with a workspaces_sql_transfer_key. That
    key is not a portable URL: a Slack browser tab has no corresponding stored
    SQL. A one-click editor link requires a separately provisioned workspace
    file; do not substitute a made-up create_file URL here.

    Verified against a real account, as is the identifier casing in
    `snowflake_table`: the route is `#/compute/history/queries/<id>/detail`.
    """
    base = env.get(SNOWFLAKE_ACCOUNT_ENV, "").strip().rstrip("/")
    if not base or not query_id:
        return None
    return _link(
        "Snowflake query",
        url=f"{base}/#/compute/history/queries/{quote(str(query_id), safe='')}/detail",
    )
