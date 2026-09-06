"""dbt Cloud: a courier, not an integration.

dbt Cloud serves the same `run_results.json` and `manifest.json` that dbt-core
writes to `target/`, so the whole job is to collect two files and hand them to
`dbt_artifacts`. The tests that matter are about the seams:

  **Both ways in are safe together.** A webhook and a poll will see the same run,
  and the only reason running both is sane is that ingesting twice changes
  nothing.
  **The webhook is a doorbell.** A signed body proves someone holds the secret,
  not that its contents are true, so nothing is recorded from it.
  **No secret means refuse.** An endpoint that ingests on demand and checks
  nothing is one anybody can point at anything.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from dataspine import dbt_cloud, links, notify

FIXTURES = Path(__file__).parent / "fixtures"
NOW = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
SECRET = "whsec-test"


@pytest.fixture(autouse=True)
def cloud_env(monkeypatch):
    monkeypatch.setenv(dbt_cloud.TOKEN_ENV, "dbtc_test")
    monkeypatch.setenv(dbt_cloud.ACCOUNT_ENV, "12345")
    monkeypatch.delenv(dbt_cloud.NAMESPACE_ENV, raising=False)
    monkeypatch.delenv(dbt_cloud.SECRET_ENV, raising=False)


class Cloud:
    """Stands in for the Admin API, answering the way it does."""

    def __init__(self, *, runs=None, missing=(), status=200):
        # Newest first, as `order_by=-created_at` returns them. The order is not
        # decoration: the walk stops at the first run older than the window, so a
        # fixture in the wrong order tests something the real API never does.
        self.runs = runs if runs is not None else [
            # Still going: no finish time at all, which is why the window is on
            # created_at rather than finished_at.
            {"id": 600, "status": 3, "created_at": "2026-09-05T11:55:00.000000Z",
             "finished_at": None},
            {"id": 501, "status": 10, "created_at": "2026-09-05T11:29:00.000000Z",
             "finished_at": "2026-09-05T11:30:00.000000Z"},
            {"id": 502, "status": 20, "created_at": "2026-09-05T09:59:00.000000Z",
             "finished_at": "2026-09-05T10:00:00.000000Z"},
            {"id": 401, "status": 10, "created_at": "2026-09-01T09:59:00.000000Z",
             "finished_at": "2026-09-01T10:00:00.000000Z"},
        ]
        self.missing = set(missing)
        self.status = status
        self.fetched: list[str] = []

    def get(self, url, params=None, **kwargs):
        request = httpx.Request("GET", f"https://cloud.getdbt.com{url}")
        if self.status != 200:
            return httpx.Response(self.status, json={}, request=request)
        if url.endswith("/runs/"):
            return httpx.Response(200, json={"data": self.runs}, request=request)
        name = url.rsplit("/", 1)[-1]
        self.fetched.append(url)
        if name in self.missing:
            return httpx.Response(404, json={}, request=request)
        fixture = {
            "run_results.json": "dbt_run_results_1.12.3.json",
            "manifest.json": "dbt_manifest_1.12.3.json",
        }[name]
        return httpx.Response(
            200, json=json.loads((FIXTURES / fixture).read_text()), request=request
        )


# ------------------------------------------------------------------ the client


def test_only_finished_runs_inside_the_window_are_considered():
    """Ordered newest-first, so the first run older than the window ends the walk
    rather than paging the account's entire history every few minutes."""
    cloud = Cloud()

    runs = dbt_cloud.finished_runs(cloud, since=NOW - timedelta(hours=24))

    assert [r["id"] for r in runs] == [501, 502], "not the old one, not the in-progress one"


def test_runs_still_going_are_listed_separately():
    """They carry no artifacts, so they cannot produce a tree or a test result.
    What they can produce is the knowledge that a job is underway."""
    runs = dbt_cloud.running_runs(Cloud(), since=NOW - timedelta(hours=24))

    assert [r["id"] for r in runs] == [600]


def test_the_walk_pages_past_the_first_batch():
    """One page of 50 would silently lose the *older* runs on a busy account —
    the worst way to lose them, since recent ones keep arriving and nothing
    looks wrong."""
    many = [
        {"id": 1000 + i, "status": 10,
         "created_at": f"2026-09-05T{11 - (i // 30):02d}:00:00.000000Z",
         "finished_at": f"2026-09-05T{11 - (i // 30):02d}:05:00.000000Z"}
        for i in range(120)
    ]

    class Paged(Cloud):
        def get(self, url, params=None, **kwargs):
            if url.endswith("/runs/"):
                off = (params or {}).get("offset", 0)
                lim = (params or {}).get("limit", 50)
                return httpx.Response(200, json={"data": many[off:off + lim]},
                                      request=httpx.Request("GET", url))
            return super().get(url, params=params, **kwargs)

    runs = dbt_cloud.finished_runs(Paged(), since=NOW - timedelta(hours=24), limit=200)

    assert len(runs) == 120, "all of them, not just the first page"


def test_a_bad_token_says_so_plainly():
    with pytest.raises(dbt_cloud.DbtCloudError, match=dbt_cloud.TOKEN_ENV):
        dbt_cloud.finished_runs(Cloud(status=401), since=NOW - timedelta(hours=24))


def test_missing_configuration_is_reported_not_guessed(monkeypatch):
    monkeypatch.delenv(dbt_cloud.TOKEN_ENV, raising=False)
    assert not dbt_cloud.configured()
    with pytest.raises(dbt_cloud.DbtCloudError):
        dbt_cloud.finished_runs(Cloud(), since=NOW)


# ---------------------------------------------------------------- ingestion


def test_a_cloud_run_becomes_an_ordinary_run_tree(conn):
    """The point of synthesising: nothing downstream knows dbt Cloud exists."""
    cloud = Cloud()

    result = dbt_cloud.ingest_run(conn, cloud, 501)

    assert result["events"] == 14
    assert result["checks"] == 4
    root = conn.execute(
        "select state, integration from run_summary where job_name = 'analytics.run'"
    ).fetchone()
    assert root["state"] == "FAILED", "a failing dbt test fails the invocation"
    assert root["integration"] == "DBT"


def test_the_tables_it_built_become_datasets(conn):
    dbt_cloud.ingest_run(conn, Cloud(), 501)

    names = {
        r["name"] for r in conn.execute("select name from datasets").fetchall()
    }
    assert "postgres.analytics_marts.fct_orders" in names


def test_its_tests_land_as_checks(conn):
    from dataspine import dq

    dbt_cloud.ingest_run(conn, Cloud(), 501)

    assert {r["check_name"] for r in dq.failing(conn)} == {
        "not_null_fct_order_items_order_id",
        "unique_fct_order_items_item_id",
    }


def test_ingesting_twice_changes_nothing(conn):
    """The only reason running a webhook *and* a poll is sane."""
    dbt_cloud.ingest_run(conn, Cloud(), 501)
    before = _counts(conn)

    dbt_cloud.ingest_run(conn, Cloud(), 501)

    assert _counts(conn) == before


def _counts(conn) -> tuple[int, int, int]:
    one = conn.execute("select count(*) as n from runs").fetchone()["n"]
    two = conn.execute("select count(*) as n from datasets").fetchone()["n"]
    three = conn.execute("select count(*) as n from external_checks").fetchone()["n"]
    return one, two, three


def test_a_run_with_no_artifacts_is_skipped_not_failed(conn):
    """A run that died during `dbt parse` has no run_results.json. Ordinary."""
    result = dbt_cloud.ingest_run(conn, Cloud(missing={"run_results.json"}), 501)

    assert result["skipped"] == "no run_results.json"
    assert result["events"] == 0


def test_a_missing_manifest_still_records_the_run(conn):
    """Degrades to what run_results alone can say: the tree, minus any test's
    claim about which table it asserted on."""
    result = dbt_cloud.ingest_run(conn, Cloud(missing={"manifest.json"}), 501)

    assert result["events"] > 0
    assert result["checks"] == 0


def test_the_namespace_can_be_pinned(conn, monkeypatch):
    """Worth setting when a Spark job or a sources.yml poller reports the same
    warehouse, so the catalog reads as one system rather than two."""
    monkeypatch.setenv(dbt_cloud.NAMESPACE_ENV, "snowflake://acme")
    dbt_cloud.ingest_run(conn, Cloud(), 501)

    namespaces = {
        r["namespace"] for r in conn.execute("select namespace from datasets").fetchall()
    }
    assert namespaces == {"snowflake://acme"}


def test_one_bad_run_does_not_stop_the_others(conn):
    """Otherwise a single malformed artifact holds up every run behind it, on
    every poll, forever."""

    class Flaky(Cloud):
        def get(self, url, params=None, **kwargs):
            if "/runs/502/" in url:
                raise httpx.ConnectError("boom")
            return super().get(url, params=params, **kwargs)

    results = dbt_cloud.pull(conn, since=NOW - timedelta(hours=24), client=Flaky(), now=NOW)

    by_run = {r["run_id"]: r for r in results}
    assert by_run["501"]["events"] == 14
    assert "error" in by_run["502"]


# ----------------------------------------------------------------- the webhook


def _sign(body: bytes, secret: str = SECRET) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def test_a_valid_signature_is_accepted(monkeypatch):
    monkeypatch.setenv(dbt_cloud.SECRET_ENV, SECRET)
    body = b'{"data":{"runId":501}}'
    assert dbt_cloud.verify(body, _sign(body))


def test_a_tampered_body_is_rejected(monkeypatch):
    monkeypatch.setenv(dbt_cloud.SECRET_ENV, SECRET)
    signature = _sign(b'{"data":{"runId":501}}')
    assert not dbt_cloud.verify(b'{"data":{"runId":999}}', signature)


def test_no_secret_means_refuse_everything(monkeypatch):
    """An endpoint that ingests on demand and checks nothing is one anybody can
    point at anything."""
    monkeypatch.delenv(dbt_cloud.SECRET_ENV, raising=False)
    body = b'{"data":{"runId":501}}'
    assert not dbt_cloud.verify(body, _sign(body))
    assert not dbt_cloud.verify(body, None)


def test_the_run_id_is_the_only_thing_taken_from_the_payload():
    assert dbt_cloud.run_id_from({"data": {"runId": 501}}) == "501"
    assert dbt_cloud.run_id_from({"data": {"run_id": 7}}) == "7"
    assert dbt_cloud.run_id_from({"accountId": 1}) is None


def test_the_endpoint_refuses_an_unsigned_request(api_client):
    response = api_client.post(
        "/api/v1/dbt-cloud/webhook", content=b'{"data":{"runId":501}}'
    )
    assert response.status_code == 401


def test_the_endpoint_refuses_a_wrong_signature(api_client, monkeypatch):
    monkeypatch.setenv(dbt_cloud.SECRET_ENV, SECRET)
    response = api_client.post(
        "/api/v1/dbt-cloud/webhook",
        content=b'{"data":{"runId":501}}',
        headers={"Authorization": _sign(b"something else")},
    )
    assert response.status_code == 401


def test_the_endpoint_ingests_a_signed_run(api_client, monkeypatch):
    monkeypatch.setenv(dbt_cloud.SECRET_ENV, SECRET)
    cloud = Cloud()
    monkeypatch.setattr(dbt_cloud, "_client", lambda client=None: (cloud, False))
    body = b'{"data":{"runId":501}}'

    response = api_client.post(
        "/api/v1/dbt-cloud/webhook", content=body,
        headers={"Authorization": _sign(body)},
    )

    assert response.status_code == 202
    assert response.json()["events"] == 14
    # Nothing was taken from the body but the run id: everything recorded came
    # from artifacts fetched with our own token.
    assert any("run_results.json" in url for url in cloud.fetched)


# ------------------------------------------------- the webhook and bearer auth


def test_the_webhook_works_with_api_tokens_configured(api_client, monkeypatch):
    """The bug a real deployment found and every test missed.

    The `/api/v1` router carries `Depends(require_token)`, and dbt Cloud cannot
    present a bearer token. With auth configured — which is every real
    deployment — the webhook returned 401 from the auth layer before its handler
    ran. Tests passed because the fixture leaves DATASPINE_API_TOKENS unset, so
    the dependency never fired.
    """
    monkeypatch.setenv("DATASPINE_API_TOKENS", "secret-token")
    monkeypatch.setenv(dbt_cloud.SECRET_ENV, SECRET)
    cloud = Cloud()
    monkeypatch.setattr(dbt_cloud, "_client", lambda client=None: (cloud, False))
    body = b'{"data":{"runId":501}}'

    response = api_client.post(
        "/api/v1/dbt-cloud/webhook", content=body,
        headers={"Authorization": _sign(body)},
    )

    assert response.status_code == 202, "no bearer token, and that is the point"
    assert response.json()["events"] == 14


def test_a_bad_signature_is_still_refused_with_auth_on(api_client, monkeypatch):
    """And it must be refused by the *signature* check, not incidentally by the
    bearer check standing in front of it."""
    monkeypatch.setenv("DATASPINE_API_TOKENS", "secret-token")
    monkeypatch.setenv(dbt_cloud.SECRET_ENV, SECRET)

    response = api_client.post(
        "/api/v1/dbt-cloud/webhook", content=b'{"data":{"runId":501}}',
        headers={"Authorization": _sign(b"something else")},
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "invalid signature"


def test_other_api_routes_still_require_a_token(api_client, monkeypatch):
    """Moving one route off the authed router must not open the rest."""
    monkeypatch.setenv("DATASPINE_API_TOKENS", "secret-token")

    assert api_client.get("/api/v1/runs").status_code == 401
    assert api_client.get(
        "/api/v1/runs", headers={"Authorization": "Bearer secret-token"}
    ).status_code == 200


# --------------------------------------------------- a run that is still going


def _running(run_id=600, project="aidanAX", job="Nightly Build"):
    class Live(Cloud):
        def get(self, url, params=None, **kwargs):
            req = httpx.Request("GET", f"https://cloud.getdbt.com{url}")
            if url.endswith(f"/projects/{project_id}/"):
                return httpx.Response(200, json={"data": {"name": project}}, request=req)
            if url.endswith(f"/runs/{run_id}/"):
                return httpx.Response(
                    200, json={"data": {"id": run_id, "job": {"name": job}}}, request=req)
            return super().get(url, params=params, **kwargs)

    project_id = 777
    return Live(), {"id": run_id, "status": 3, "project_id": project_id,
                    "created_at": "2026-09-05T11:55:00.000000Z",
                    "started_at": "2026-09-05T11:55:10.000000Z"}


def test_a_run_in_flight_is_recorded_before_it_finishes(conn):
    """Artifacts only exist once a run ends, so without this a dbt Cloud job can
    never appear as running — the feed would only ever get a fait accompli."""
    cloud, run = _running()

    result = dbt_cloud.ingest_running(conn, cloud, run)

    assert result["state"] == "RUNNING"
    row = conn.execute(
        "select job_name, state from run_summary where depth = 0"
    ).fetchone()
    assert row["job_name"] == "aidanAX.Nightly Build"
    assert row["state"] == "RUNNING"


def test_the_live_feed_can_see_it(conn):
    """The whole point: a row in the feed while the job is still going."""
    from dataspine import notify

    cloud, run = _running()
    dbt_cloud.ingest_running(conn, cloud, run)

    # Clock pinned: the fixture's run started at 11:55 and the feed calls
    # anything silent for 45 minutes "stalled", so real wall time would report
    # this long-finished-in-reality run as a dead cluster.
    feed = notify.pipeline_feed(conn, now=NOW)

    assert [n.status for n in feed] == ["running"]
    assert feed[0].job == "aidanAX.Nightly Build"


def test_finishing_edits_the_same_run_rather_than_making_a_second(conn):
    """The placeholder and the tree must be one run. Keyed on dbt Cloud's run id
    rather than dbt's invocation_id, which does not exist until artifacts do."""
    cloud, run = _running(run_id=501)
    dbt_cloud.ingest_running(conn, cloud, run)
    before = conn.execute(
        "select count(*) as n from runs where parent_run_id is null"
    ).fetchone()["n"]

    dbt_cloud.ingest_run(conn, Cloud(), 501)

    after = conn.execute(
        "select run_id, state from runs where parent_run_id is null"
    ).fetchall()
    assert before == len(after) == 1, "one root throughout, not two"
    assert after[0]["state"] == "FAILED"
    assert str(after[0]["run_id"]) == str(dbt_cloud.cloud_run_id(501))


def test_pull_covers_both_halves(conn):
    """One sweep records what finished and what is still going."""
    class Both(Cloud):
        def get(self, url, params=None, **kwargs):
            req = httpx.Request("GET", f"https://cloud.getdbt.com{url}")
            if "/projects/" in url:
                return httpx.Response(200, json={"data": {"name": "aidanAX"}}, request=req)
            if url.endswith("/runs/600/"):
                return httpx.Response(
                    200, json={"data": {"id": 600, "job": {"name": "Hourly"}}}, request=req)
            return super().get(url, params=params, **kwargs)

    results = dbt_cloud.pull(conn, since=NOW - timedelta(hours=24), client=Both(), now=NOW)

    states = {r.get("state") for r in results}
    assert "RUNNING" in states, "the in-flight one"
    assert any(r.get("events") for r in results), "and the finished ones"


def test_running_ingest_can_be_switched_off(conn):
    results = dbt_cloud.pull(conn, since=NOW - timedelta(hours=24), client=Cloud(),
                             now=NOW, with_running=False)
    assert all(r.get("state") != "RUNNING" for r in results)


# ---------------------------------------------------------------- diagnostics


def _run_check(monkeypatch, responses):
    """Invoke `dataspine dbt-cloud-check` against a stubbed API."""
    import httpx as _httpx
    from typer.testing import CliRunner

    from dataspine.cli import app

    class Client:
        def __init__(self, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, url, **kwargs):
            # Longest pattern first: "/accounts/12345/" is a substring of the
            # jobs and webhooks URLs too, and matching it there served the
            # account body to the jobs call.
            for pattern in sorted(responses, key=len, reverse=True):
                if url.endswith(pattern):
                    code, body = responses[pattern]
                    return _httpx.Response(
                        code, json=body, request=_httpx.Request("GET", url))
            return _httpx.Response(404, json={}, request=_httpx.Request("GET", url))

    monkeypatch.setattr(_httpx, "Client", Client)
    return CliRunner().invoke(app, ["dbt-cloud-check"])


def test_check_reports_a_healthy_setup(monkeypatch):
    result = _run_check(monkeypatch, {
        "/accounts/12345/": (200, {"data": {"name": "Personal"}}),
        "/jobs/": (200, {"data": [
            {"id": 1, "name": "Nightly Build", "execute_steps": ["dbt build"]}]}),
        "/webhooks/subscriptions": (200, {"data": []}),
    })
    assert "Nightly Build" in result.output
    assert "reachable" in result.output


def test_check_blames_the_host_before_the_token(monkeypatch):
    """The failure this command exists for. A 401 cannot tell a wrong host from
    a wrong token, and guessing "bad token" sends someone to re-issue one that
    was fine — which is exactly the afternoon this is meant to save."""
    result = _run_check(monkeypatch, {"/accounts/12345/": (401, {})})

    assert dbt_cloud.HOST_ENV in result.output
    assert "multi-cell" in result.output
    assert result.exit_code == 1


def test_check_says_what_to_do_when_webhooks_are_unavailable(monkeypatch):
    """Not an error — a plan limit with a supported alternative."""
    result = _run_check(monkeypatch, {
        "/accounts/12345/": (200, {"data": {"name": "Personal"}}),
        "/jobs/": (200, {"data": []}),
        "/webhooks/subscriptions": (404, {}),
    })

    assert "webhooks unavailable" in result.output
    assert "pull-dbt-cloud" in result.output


def test_check_refuses_when_nothing_is_configured(monkeypatch):
    monkeypatch.delenv(dbt_cloud.TOKEN_ENV, raising=False)
    from typer.testing import CliRunner

    from dataspine.cli import app

    result = CliRunner().invoke(app, ["dbt-cloud-check"])
    assert result.exit_code == 1
    assert dbt_cloud.TOKEN_ENV in result.output


# ------------------------------------------------------ the link back to dbt

HREF = (
    "https://abc123.us1.dbt.com/deploy/11111111111110"
    "/projects/22222222222220/runs/55555555555551/"
)


def test_the_href_is_kept_verbatim_never_assembled():
    """dbt Cloud is multi-cell: accounts live on hosts like `abc123.us1.dbt.com`
    and a URL built from the default host plus the right ids lands nowhere.

    The run payload states the URL, so keeping it is the only correct move.
    """
    facet = dbt_cloud.run_facet(
        55555555555551,
        {"href": HREF, "job_definition_id": 1, "project_id": 2},
    )["dbt_cloud"]
    assert facet["href"] == HREF
    assert facet["runId"] == "55555555555551"
    assert (facet["jobId"], facet["projectId"]) == ("1", "2")


def test_a_run_dbt_cloud_gave_no_href_for_carries_only_its_id():
    """No URL is a real answer, and inventing one is the failure this avoids."""
    facet = dbt_cloud.run_facet(600, {})["dbt_cloud"]
    assert "href" not in facet
    assert facet["runId"] == "600"


def test_a_finished_run_records_where_it_lives(conn, monkeypatch):
    """So the failure alert can offer `dbt Cloud` beside `open in wish:d`."""
    class WithHref(Cloud):
        def get(self, url, params=None, **kwargs):
            if url.endswith("/runs/502/"):
                return httpx.Response(
                    200,
                    json={"data": {"id": 502, "job": {"name": "Nightly Build"},
                                   "href": HREF, "project_id": 2}},
                    request=httpx.Request("GET", f"https://cloud.getdbt.com{url}"),
                )
            return super().get(url, params=params, **kwargs)

    captured = []

    def capture_send(conn, notes, **kwargs):
        captured.extend(notes)
        return {"sent": [n.dedup_key for n in notes], "skipped": [], "failed": []}

    monkeypatch.setattr(notify, "send", capture_send)
    monkeypatch.setenv(notify.BASE_URL_ENV, "https://dataspine.example.com")
    dbt_cloud.ingest_run(conn, WithHref(), 502)

    assert len(captured) == 1
    note = captured[0]
    root = str(dbt_cloud.cloud_run_id(502))
    assert note.dedup_key == root
    assert note.url == f"https://dataspine.example.com/runs/{root}"
    assert ("dbt Cloud", HREF) in note.links
    assert conn.execute("select 1 from runs where run_id = %s", (root,)).fetchone()

    facets = conn.execute(
        "select facets from runs where depth = 0"
    ).fetchone()["facets"]
    assert facets["dbt_cloud"]["href"] == HREF
    assert links.run_links("DBT", facets)[0] == {
        "label": "dbt Cloud", "url": HREF, "value": None
    }


def test_a_running_run_is_linkable_before_it_finishes(conn):
    """The half of the run someone actually wants to go and watch.

    The listing payload already carries `href`, so the live feed's first message
    can link out rather than only its last.
    """
    cloud, run = _running()
    dbt_cloud.ingest_running(conn, cloud, dict(run, href=HREF))

    note = notify.pipeline_feed(conn)[0]
    assert ("dbt Cloud", HREF) in note.links


def test_the_job_name_lookup_still_works_on_its_own():
    """`job_name` is kept as the narrow question, now answered from the same
    fetch that produces the link."""
    cloud, _ = _running(job="Hourly Run and Test")
    assert dbt_cloud.job_name(cloud, 600) == "Hourly Run and Test"
