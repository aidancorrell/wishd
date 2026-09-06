"""Auth tests.

The behaviour worth protecting here is the bind guard: it is the difference
between "we documented that you should set a token" and "you cannot accidentally
run this open on a reachable interface".
"""

from __future__ import annotations

import pytest

from dataspine import auth

TOKEN = "test-token-abc123"


@pytest.fixture()
def with_tokens(monkeypatch):
    monkeypatch.setenv(auth.TOKEN_ENV, f"airflow:{TOKEN},spark:second-token")
    return TOKEN


def test_tokens_parse_with_and_without_labels(monkeypatch):
    monkeypatch.setenv(auth.TOKEN_ENV, "airflow:abc, plain , spark:def")
    assert auth.configured_tokens() == ["abc", "plain", "def"]


def test_auth_disabled_when_unset(monkeypatch):
    monkeypatch.delenv(auth.TOKEN_ENV, raising=False)
    assert auth.auth_enabled() is False


# ------------------------------------------------------------------ bind guard


def test_guard_allows_loopback_without_tokens(monkeypatch):
    monkeypatch.delenv(auth.TOKEN_ENV, raising=False)
    monkeypatch.delenv(auth.INSECURE_ENV, raising=False)
    auth.guard_bind_address("127.0.0.1")  # must not raise
    auth.guard_bind_address("localhost")


def test_guard_refuses_public_bind_without_tokens(monkeypatch):
    """0.0.0.0 is every container's default, which is exactly where 'we'll add
    auth later' turns into never."""
    monkeypatch.delenv(auth.TOKEN_ENV, raising=False)
    monkeypatch.delenv(auth.INSECURE_ENV, raising=False)
    with pytest.raises(SystemExit):
        auth.guard_bind_address("0.0.0.0")


def test_guard_refuses_private_network_bind_too(monkeypatch):
    """A VPC address is reachable by other machines. 'It's internal' is not a
    security control."""
    monkeypatch.delenv(auth.TOKEN_ENV, raising=False)
    monkeypatch.delenv(auth.INSECURE_ENV, raising=False)
    with pytest.raises(SystemExit):
        auth.guard_bind_address("10.0.4.17")


def test_guard_allows_public_bind_with_tokens(with_tokens):
    auth.guard_bind_address("0.0.0.0")  # must not raise


def test_guard_honours_explicit_insecure_optout(monkeypatch):
    monkeypatch.delenv(auth.TOKEN_ENV, raising=False)
    monkeypatch.setenv(auth.INSECURE_ENV, "true")
    auth.guard_bind_address("0.0.0.0")  # must not raise


# ------------------------------------------------------------- request checks


def _client(monkeypatch, token_env: str | None):
    from fastapi.testclient import TestClient

    from dataspine import db
    from dataspine.api import app

    if token_env is None:
        monkeypatch.delenv(auth.TOKEN_ENV, raising=False)
    else:
        monkeypatch.setenv(auth.TOKEN_ENV, token_env)
    db.reset_pool()
    return TestClient(app)


def test_api_rejects_missing_token(database_url, monkeypatch):
    with _client(monkeypatch, TOKEN) as client:
        assert client.get("/api/v1/runs").status_code == 401
        assert client.post("/api/v1/lineage", json={}).status_code == 401
        # Health stays open so probes and operators still work.
        assert client.get("/health").status_code == 200
        assert client.get("/health").json()["auth_enabled"] is True


def test_api_accepts_bearer_and_api_key(database_url, monkeypatch):
    with _client(monkeypatch, TOKEN) as client:
        assert client.get(
            "/api/v1/runs", headers={"Authorization": f"Bearer {TOKEN}"}
        ).status_code == 200
        assert client.get("/api/v1/runs", headers={"X-API-Key": TOKEN}).status_code == 200
        assert client.get(
            "/api/v1/runs", headers={"Authorization": "Bearer wrong"}
        ).status_code == 401


def test_ui_redirects_to_login_rather_than_401(database_url, monkeypatch):
    """A human who bookmarked a run URL should get a form, not a status code."""
    with _client(monkeypatch, TOKEN) as client:
        resp = client.get("/", follow_redirects=False)
        assert resp.status_code == 303
        assert resp.headers["location"].startswith("/login")


def test_login_sets_cookie_and_grants_access(database_url, monkeypatch):
    with _client(monkeypatch, TOKEN) as client:
        bad = client.post("/login", data={"token": "nope", "next": "/"}, follow_redirects=False)
        assert "error=1" in bad.headers["location"]

        ok = client.post("/login", data={"token": TOKEN, "next": "/"}, follow_redirects=False)
        assert ok.status_code == 303
        assert auth.COOKIE_NAME in ok.cookies

        # The cookie now authenticates subsequent requests.
        assert client.get("/", follow_redirects=False).status_code == 200


@pytest.mark.parametrize("target", [
    "https://example.org", "//example.org", "/\\example.org", "/%5cexample.org",
    "/%2fexample.org", "/\n/example.org",
])
def test_login_redirect_cannot_leave_origin(database_url, monkeypatch, target):
    with _client(monkeypatch, TOKEN) as client:
        response = client.post(
            "/login", data={"token": TOKEN, "next": target}, follow_redirects=False
        )
        assert response.headers["location"] == "/"


def test_login_validates_form_token_even_with_authenticated_header(database_url, monkeypatch):
    with _client(monkeypatch, TOKEN) as client:
        response = client.post(
            "/login", data={"token": "wrong"},
            headers={"Authorization": f"Bearer {TOKEN}"}, follow_redirects=False,
        )
        assert "error=1" in response.headers["location"]
        assert auth.COOKIE_NAME not in response.cookies


def test_unicode_token_is_rejected_without_crashing(database_url, monkeypatch):
    with _client(monkeypatch, TOKEN) as client:
        response = client.post("/login", data={"token": "snowman-☃"}, follow_redirects=False)
        assert "error=1" in response.headers["location"]


def test_https_login_cookie_is_secure(database_url, monkeypatch):
    with _client(monkeypatch, TOKEN) as client:
        response = client.post(
            "https://testserver/login", data={"token": TOKEN}, follow_redirects=False
        )
        assert "Secure" in response.headers["set-cookie"]
        assert "HttpOnly" in response.headers["set-cookie"]


def test_cross_origin_login_is_rejected(database_url, monkeypatch):
    with _client(monkeypatch, TOKEN) as client:
        response = client.post(
            "/login", data={"token": TOKEN}, headers={"Origin": "https://example.org"}
        )
        assert response.status_code == 403


def test_empty_labelled_secret_fails_configuration(monkeypatch):
    monkeypatch.setenv(auth.TOKEN_ENV, "airflow:")
    with pytest.raises(ValueError, match="non-empty"):
        auth.guard_bind_address("0.0.0.0")
