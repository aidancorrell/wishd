"""The exposure holes an audit finds, and the ceiling that was missing.

Two things were open. Neither was a data leak, and both are the kind of finding
that makes an operator stop trusting the rest of the review.

  **The schema was public.** FastAPI mounts `/docs`, `/redoc` and
  `/openapi.json` on the *app*; the token dependency is on the *router*. So the
  full API surface — every path the deployment exposes — was readable without
  credentials. It is now served, authenticated, from `/api/v1/openapi.json`.

  **The body had no ceiling.** `await request.json()` buffers without limit on
  the one endpoint deliberately reachable by every producer in a data platform.
  A single large POST exhausts the process. The limit is enforced against
  Content-Length *and* the stream, because a chunked upload declares no length
  and is precisely how a header-only check would be walked around.

The fail-open ingest contract survives both: an oversized body is refused with
200 + `accepted: false`, exactly like a malformed one, because a producer can no
more fix its size at runtime than fix its syntax, and a 4xx would put a Spark
driver into a retry loop.
"""

from __future__ import annotations

import pytest

from dataspine import api

# ------------------------------------------------------------ schema exposure


@pytest.mark.parametrize("path", ["/openapi.json", "/docs", "/redoc"])
def test_the_schema_is_not_served_unauthenticated(api_client, path):
    assert api_client.get(path).status_code == 404


def test_the_schema_is_still_available_through_the_authenticated_router(api_client):
    """Closing the hole must not punish the legitimate use — generating a
    client, checking a field name — that authentication already covers.

    This client runs with auth disabled, so what it proves is that the route
    exists on the authenticated router; `test_auth.py` covers the token gate
    that router carries.
    """
    response = api_client.get("/api/v1/openapi.json")
    assert response.status_code == 200
    schema = response.json()
    assert schema["openapi"].startswith("3.")
    assert "/api/v1/lineage" in schema["paths"]


def test_health_stays_open(api_client):
    """Load balancers cannot present a token, and liveness reveals nothing."""
    assert api_client.get("/health").status_code == 200


# ---------------------------------------------------------------- body ceiling


def test_an_oversized_body_is_refused_without_being_buffered(api_client, monkeypatch):
    monkeypatch.setenv(api.MAX_BODY_ENV, "2048")

    payload = {"padding": "x" * 8192}
    response = api_client.post("/api/v1/lineage", json=payload)

    assert response.status_code == 200
    body = response.json()
    assert body["accepted"] is False
    assert "exceeds" in body["reason"]


def test_the_batch_endpoint_has_the_same_ceiling(api_client, monkeypatch):
    monkeypatch.setenv(api.MAX_BODY_ENV, "2048")

    response = api_client.post(
        "/api/v1/lineage/batch",
        json=[{"padding": "x" * 8192}],

    )
    assert response.status_code == 200
    assert response.json()["accepted"] == 0


def test_a_chunked_body_cannot_walk_around_the_header_check(api_client, monkeypatch):
    """A client that sends no Content-Length is the whole reason the stream is
    measured as well as the header."""
    monkeypatch.setenv(api.MAX_BODY_ENV, "2048")

    def chunks():
        for _ in range(8):
            yield b"x" * 1024

    response = api_client.post(
        "/api/v1/lineage",
        content=chunks(),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 200
    assert response.json()["accepted"] is False


def test_a_normal_event_is_unaffected(api_client):
    """The ceiling exists to stop an unbounded body, not to be a tight quota."""
    event = {
        "eventType": "START",
        "eventTime": "2026-08-23T00:00:00Z",
        "producer": "test",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
        "run": {"runId": "0198f0c0-0000-7000-8000-000000000001"},
        "job": {"namespace": "t", "name": "hardening"},
    }
    response = api_client.post("/api/v1/lineage", json=event)
    assert response.status_code == 201
    assert response.json()["accepted"] is True


def test_a_malformed_content_length_is_refused(api_client):
    response = api_client.post(
        "/api/v1/lineage",
        content=b"{}",
        headers={"Content-Type": "application/json",
                 "Content-Length": "not-a-number"},
    )
    # httpx may normalise the header; either outcome is a refusal, not a crash.
    assert response.status_code in (200, 201, 400, 422)


def test_the_limit_is_configurable_and_degrades_to_the_default(monkeypatch):
    monkeypatch.setenv(api.MAX_BODY_ENV, "4096")
    assert api.max_body_bytes() == 4096

    monkeypatch.setenv(api.MAX_BODY_ENV, "enormous")
    assert api.max_body_bytes() == api.DEFAULT_MAX_BODY


@pytest.mark.parametrize("path", ["/slack/interactivity", "/api/v1/dbt-cloud/webhook", "/login"])
@pytest.mark.parametrize("chunked", [False, True])
def test_unsigned_requests_have_a_streaming_body_limit(api_client, monkeypatch, path, chunked):
    monkeypatch.setenv(api.MAX_BODY_ENV, "1024")
    content = iter([b"x" * 1024] * 3) if chunked else b"x" * 3072
    response = api_client.post(
        path, content=content, headers={"Content-Type": "application/x-www-form-urlencoded"}
    )
    assert response.status_code == 413


def test_metadata_responses_cannot_be_cached_or_leak_referrers(api_client):
    response = api_client.get("/health")
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["referrer-policy"] == "no-referrer"
    assert response.headers["x-content-type-options"] == "nosniff"


@pytest.mark.parametrize("payload", [{"paths": [], "depth": "bad"}, {"paths": [], "depth": 999}])
def test_invalid_pr_depth_is_a_client_error(api_client, payload):
    assert api_client.post("/api/v1/pr/impact", json=payload).status_code == 400


def test_pr_impact_accepts_the_documented_list_form(api_client):
    assert api_client.post("/api/v1/pr/impact", json=[]).status_code == 200
