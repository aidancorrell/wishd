"""Deep links back to the systems that produced a run.

The point of Phase 01 is that debugging a failed nightly run requires no SSH and
no S3 log digging. dataspine will not hold every detail — the Spark UI has the
stage breakdown, Airflow has the task log — so the run page has to hand you
straight to them.

What is actually possible here is dictated by the facets real producers send,
which is why this was built from the captured fixtures rather than from
guesswork:

  Spark   `spark_applicationDetails.uiWebUrl` is a genuine URL. No config needed.
  Airflow carries `dag_id` and `run_id` but no base URL, so a link requires
          DATASPINE_AIRFLOW_BASE_URL. Without it we must render nothing rather
          than a guessed hostname.
  dbt     has `invocation_id` and no URL at all. We surface the id as a
          copyable reference — it is what you grep logs for — and do not
          pretend it is a link.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dataspine import links

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture()
def spark_facets() -> dict:
    events = json.loads((FIXTURES / "spark_openlineage_1.52.0.json").read_text())
    return next(
        e["run"]["facets"] for e in events
        if "spark_applicationDetails" in e["run"]["facets"]
    )


@pytest.fixture()
def airflow_facets() -> dict:
    events = json.loads((FIXTURES / "airflow_dbt_end_to_end.json").read_text())
    return next(e["run"]["facets"] for e in events if "airflow" in e["run"]["facets"])


@pytest.fixture()
def dbt_facets() -> dict:
    events = json.loads((FIXTURES / "airflow_dbt_end_to_end.json").read_text())
    return next(e["run"]["facets"] for e in events if "dbt_run" in e["run"]["facets"])


# ---------------------------------------------------------------------- spark


def test_spark_link_comes_from_the_real_facet(spark_facets):
    """No configuration required: openlineage-spark hands us a working URL."""
    found = links.run_links("SPARK", spark_facets)
    urls = {link["label"]: link["url"] for link in found if link.get("url")}
    assert "Spark UI" in urls
    assert urls["Spark UI"].startswith("http")


def test_spark_application_id_is_surfaced(spark_facets):
    """The application id is what you paste into the EMR console or use to find
    the event log in S3, so it is worth showing even though it is not a link."""
    refs = {link["label"]: link["value"] for link in links.run_links("SPARK", spark_facets)
            if not link.get("url")}
    assert "Application ID" in refs
    assert refs["Application ID"]


# -------------------------------------------------------------------- airflow


def test_airflow_link_requires_a_configured_base_url(airflow_facets, monkeypatch):
    """Without a base URL we render nothing.

    Guessing a hostname would produce links that 404 for everyone whose Airflow
    is not where we assumed, which is worse than no link at all.
    """
    monkeypatch.delenv(links.AIRFLOW_BASE_ENV, raising=False)
    found = links.run_links("AIRFLOW", airflow_facets)
    assert not [link for link in found if link.get("url")]


def test_airflow_link_is_built_when_configured(airflow_facets, monkeypatch):
    monkeypatch.setenv(links.AIRFLOW_BASE_ENV, "https://airflow.internal")
    found = links.run_links("AIRFLOW", airflow_facets)
    urls = {link["label"]: link["url"] for link in found if link.get("url")}

    assert "Airflow" in urls
    url = urls["Airflow"]
    assert url.startswith("https://airflow.internal")
    assert "analytics_daily" in url  # the dag id from the real facet


def test_trailing_slash_on_the_base_url_is_tolerated(airflow_facets, monkeypatch):
    monkeypatch.setenv(links.AIRFLOW_BASE_ENV, "https://airflow.internal/")
    url = next(
        link["url"] for link in links.run_links("AIRFLOW", airflow_facets) if link.get("url")
    )
    assert "//dags" not in url.replace("https://", "")


# ------------------------------------------------------------------------ dbt


def test_dbt_invocation_id_is_shown_but_not_as_a_link(dbt_facets):
    found = links.run_links("DBT", dbt_facets)
    refs = {link["label"]: link for link in found}
    assert "dbt invocation" in refs
    assert refs["dbt invocation"].get("url") is None
    assert refs["dbt invocation"]["value"]


# -------------------------------------------------------------------- safety


@pytest.mark.parametrize(
    "integration,facets",
    [
        ("SPARK", {}),
        ("AIRFLOW", {}),
        ("DBT", {}),
        (None, {}),
        ("SPARK", {"spark_applicationDetails": "not-a-dict"}),
        ("AIRFLOW", {"airflow": {"dag": {}}}),
        ("UNKNOWN", {"whatever": {"a": 1}}),
    ],
)
def test_missing_or_malformed_facets_never_raise(integration, facets):
    """A run page must render even when a producer sent something unexpected.
    A link is a nicety; a 500 on the debugging screen is not acceptable."""
    assert isinstance(links.run_links(integration, facets), list)


def test_no_links_for_an_unknown_integration():
    assert links.run_links("COBOL", {"parent": {}}) == []


def test_airflow_url_encodes_the_run_id(airflow_facets, monkeypatch):
    """Airflow run ids contain `:` and `+` (e.g.
    `manual__2026-08-07T23:28:41.949748+00:00_eAf6WiPh`).

    Dropped into a URL path unencoded, `+` and `:` produce a link that resolves
    to the wrong run or 404s. Caught by eyeballing a real rendered page, not by
    the earlier tests, because the synthetic dag ids had no awkward characters.
    """
    monkeypatch.setenv(links.AIRFLOW_BASE_ENV, "https://airflow.internal")
    url = next(
        link["url"] for link in links.run_links("AIRFLOW", airflow_facets) if link.get("url")
    )
    run_id_part = url.split("/runs/", 1)[1]
    assert "+" not in run_id_part, f"unencoded '+' in {run_id_part}"
    assert ":" not in run_id_part, f"unencoded ':' in {run_id_part}"
    assert "%3A" in run_id_part or "%2B" in run_id_part


# ------------------------------------------------------------------ dbt Cloud


DBT_CLOUD_HREF = (
    "https://abc123.us1.dbt.com/deploy/11111111111110"
    "/projects/22222222222220/runs/55555555555551/"
)


def test_dbt_cloud_link_is_the_href_dbt_cloud_gave_us():
    """Never assembled from parts.

    dbt Cloud is multi-cell: the account's cell is in the hostname, and a URL
    built from `cloud.getdbt.com` plus the right ids still lands nowhere. The
    run payload states the URL, so the only safe move is to keep it verbatim.
    """
    found = links.run_links("DBT", {"dbt_cloud": {"href": DBT_CLOUD_HREF}})
    assert ("dbt Cloud", DBT_CLOUD_HREF) in [
        (link["label"], link["url"]) for link in found
    ]


def test_dbt_cloud_run_id_is_a_reference_when_there_is_no_href():
    """A run we could not get a URL for still has an id worth pasting."""
    found = links.run_links("DBT", {"dbt_cloud": {"runId": "55555555555551"}})
    entry = next(link for link in found if link["label"] == "dbt Cloud run")
    assert entry["url"] is None
    assert entry["value"] == "55555555555551"


def test_a_non_http_href_is_refused():
    """Whatever that is, it is not a link, and rendering it as one is worse
    than saying nothing."""
    found = links.run_links("DBT", {"dbt_cloud": {"href": "javascript:alert(1)"}})
    assert not [link for link in found if link.get("url")]


# ------------------------------------------------------------------ snowflake


@pytest.fixture()
def snowsight(monkeypatch):
    monkeypatch.setenv(
        links.SNOWFLAKE_ACCOUNT_ENV, "https://app.snowflake.com/myorg/ab12345"
    )


def test_no_snowflake_links_without_the_account_url(monkeypatch):
    """The organisation and account names are in no facet any producer sends."""
    monkeypatch.delenv(links.SNOWFLAKE_ACCOUNT_ENV, raising=False)
    assert links.snowflake_query("01c6df00-3204") is None
    assert links.snowflake_table("ANALYTICS_DB.PUBLIC.fct_orders") is None


def test_snowflake_query_links_the_statement_that_ran(snowsight):
    """The most valuable link available, and it costs no credential.

    Snowsight's query page holds the SQL, the profile and an `Open in
    Workspaces` button, so the reader gets the failing query loaded into an
    editor without dataspine ever writing to their warehouse.
    """
    link = links.snowflake_query("01c6df00-3204-7ff8-0008-28e60001b1fa")
    assert link["url"] == (
        "https://app.snowflake.com/myorg/ab12345/#/compute/history/queries/"
        "01c6df00-3204-7ff8-0008-28e60001b1fa/detail"
    )


def test_snowflake_table_upper_cases_unquoted_identifiers(snowsight):
    """Measured against a real account: the lower-cased URL renders
    "Table not found" and the upper-cased one renders the table.

    Snowflake folds unquoted identifiers to upper case and Snowsight addresses
    the stored name, while dbt reports the relation as it was written.
    """
    link = links.snowflake_table("ANALYTICS_DB.PUBLIC.fct_orders")
    assert link["url"].endswith(
        "/#/data/databases/ANALYTICS_DB/schemas/PUBLIC/table/FCT_ORDERS"
    )
    assert link["label"] == "FCT_ORDERS in Snowsight"


def test_snowflake_table_preserves_a_quoted_identifier(snowsight):
    """dbt quoted it because the case is significant; folding it would break
    the very link the quoting exists to make possible."""
    link = links.snowflake_table('ANALYTICS_DB.PUBLIC."fct_orders"')
    assert link["url"].endswith("/table/fct_orders")


def test_snowflake_table_refuses_a_name_it_cannot_qualify(snowsight):
    """A bare table name has no database or schema, and inventing either
    produces a link to a table that does not exist."""
    assert links.snowflake_table("fct_orders") is None
    assert links.snowflake_table("PUBLIC.fct_orders") is None
