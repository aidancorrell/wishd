"""Bearer-token auth.

Threat model, stated plainly so the design is judgeable: this service receives
production pipeline metadata -- SQL text, table names, error stack traces, and
soon cost figures. That is commercially sensitive and it is a map of the data
estate. It is not, however, a system anyone logs into individually; it is
scraped by machine producers and read by a small team.

So: shared bearer tokens, not user accounts. Rotation is redeploy. When we need
per-user identity (Phase 04, when incidents get assignees) this gets replaced by
real sessions rather than extended.

The safety property that matters most is in `guard_bind_address`: an unauthenticated
gateway must not be able to bind to a public interface by accident. Documentation
saying "please set a token" does not survive contact with a hurried deploy.
"""

from __future__ import annotations

import hmac
import ipaddress
import logging

from fastapi import HTTPException, Request

from .config import env

log = logging.getLogger("dataspine.auth")

TOKEN_ENV = "DATASPINE_API_TOKENS"
INSECURE_ENV = "DATASPINE_ALLOW_INSECURE"
COOKIE_NAME = "dataspine_token"


def configured_tokens() -> list[str]:
    """Tokens from `DATASPINE_API_TOKENS`, comma-separated.

    An optional `name:` prefix is allowed so deployments can label which system a
    token belongs to (`airflow:s3cr3t`) without us needing a database for it.
    """
    raw = env.get(TOKEN_ENV, "")
    tokens = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        token = (part.split(":", 1)[1] if ":" in part else part).strip()
        if not token:
            raise ValueError("API token labels must have a non-empty secret")
        tokens.append(token)
    return tokens


def auth_enabled() -> bool:
    return bool(configured_tokens())


def _presented_token(request: Request) -> str | None:
    """Accept the token three ways.

    `Authorization: Bearer` is what OpenLineage's HTTP transport sends, so it is
    the one that matters. `X-API-Key` is for curl and for producers with awkward
    header handling. The cookie exists so the web UI can authenticate without a
    JavaScript layer holding a token.
    """
    header = request.headers.get("authorization", "")
    if header.lower().startswith("bearer "):
        return header[7:].strip()
    api_key = request.headers.get("x-api-key")
    if api_key:
        return api_key.strip()
    return request.cookies.get(COOKIE_NAME)


def check(request: Request) -> bool:
    """True if the request is authorised. Open when no tokens are configured."""
    tokens = configured_tokens()
    if not tokens:
        return True
    presented = _presented_token(request)
    if not presented:
        return False
    return valid_token(presented)


def valid_token(presented: str) -> bool:
    """Compare every configured secret, including when a previous one matched."""
    matched = False
    for token in configured_tokens():
        matched |= hmac.compare_digest(presented.encode("utf-8"), token.encode("utf-8"))
    return matched


def local_redirect(target: str) -> str:
    """A browser redirect must stay on this origin, including after URL decoding."""
    from urllib.parse import unquote

    decoded = unquote(target)
    if (
        not decoded.startswith("/")
        or decoded.startswith("//")
        or "\\" in decoded
        or any(ord(char) < 32 or ord(char) == 127 for char in decoded)
    ):
        return "/"
    return target


async def require_token(request: Request) -> None:
    """FastAPI dependency for API routes."""
    if not check(request):
        raise HTTPException(
            status_code=401,
            detail="missing or invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )


# ----------------------------------------------------------------- bind safety


def _is_public_bind(host: str) -> bool:
    if host in ("localhost", ""):
        return False
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        # A hostname we cannot classify. Treat as public: failing safe here costs
        # one env var, and failing open costs a leaked data map.
        return True
    # Anything reachable from another machine counts, including private VPC
    # addresses -- "it's on the internal network" is precisely the reasoning that
    # leaves gateways unauthenticated for two years.
    return not addr.is_loopback


def guard_bind_address(host: str) -> None:
    """Refuse to serve an unauthenticated gateway on a public interface.

    `0.0.0.0` counts as public: it is the default in every container, which is
    exactly the situation where "we'll add auth later" quietly becomes "we never
    added auth". Escape hatch is explicit and named for what it is.
    """
    if auth_enabled():
        return
    if env.get(INSECURE_ENV, "").lower() in ("1", "true", "yes"):
        log.warning(
            "Running with NO AUTHENTICATION on %s because %s is set. "
            "Anyone who can reach this port can read your pipeline metadata.",
            host,
            INSECURE_ENV,
        )
        return
    if not _is_public_bind(host):
        log.warning(
            "No %s configured -- running open on %s. Fine for local development; "
            "set tokens before exposing this anywhere.",
            TOKEN_ENV,
            host,
        )
        return
    raise SystemExit(
        f"Refusing to bind {host} with no authentication configured.\n"
        f"  Set {TOKEN_ENV}=<token>[,<token>...] to enable auth, or\n"
        f"  set {INSECURE_ENV}=true if this port is genuinely private.\n"
        "This gateway carries SQL text, table names and error traces."
    )
