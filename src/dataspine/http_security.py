"""Enforce request limits before form parsing, including unsigned webhooks."""

from __future__ import annotations

from urllib.parse import urlsplit

from fastapi import HTTPException
from starlette.datastructures import Headers, MutableHeaders
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from . import artifacts, auth


class SecurityMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        from .api import max_body_bytes

        headers = Headers(scope=scope)
        path = scope["path"]
        # Lineage has its own streaming ceiling and the producer fail-open response.
        lineage = path in {"/api/v1/lineage", "/api/v1/lineage/batch"}
        limit = max_body_bytes()
        if path.startswith("/api/v1/runs/") and path.endswith("/artifacts"):
            limit = artifacts.max_bytes() + 1024 * 1024  # multipart envelope

        async def secured_send(message: Message) -> None:
            if message["type"] == "http.response.start":
                outgoing = MutableHeaders(scope=message)
                outgoing["X-Content-Type-Options"] = "nosniff"
                outgoing["X-Frame-Options"] = "DENY"
                outgoing["Referrer-Policy"] = "no-referrer"
                if not path.startswith("/static/"):
                    outgoing["Cache-Control"] = "no-store"
            await send(message)

        async def reject(code: int, detail: str) -> None:
            await JSONResponse({"detail": detail}, status_code=code)(scope, receive, secured_send)

        # SameSite cookies still permit same-site, cross-origin requests. An
        # Origin check covers those without changing machine bearer-token auth.
        origin = headers.get("origin")
        browser_credentials = auth.COOKIE_NAME + "=" in headers.get("cookie", "")
        if scope["method"] not in {"GET", "HEAD", "OPTIONS"} and origin:
            if browser_credentials or path == "/login":
                expected = (scope["scheme"], headers.get("host", ""))
                try:
                    parsed = urlsplit(origin)
                    matches = (parsed.scheme, parsed.netloc) == expected
                except ValueError:
                    matches = False
                if not matches:
                    await reject(403, "cross-origin form submission refused")
                    return

        if not lineage and "content-length" in headers:
            try:
                declared = int(headers["content-length"])
            except ValueError:
                await reject(400, "malformed content-length")
                return
            if declared < 0:
                await reject(400, "malformed content-length")
                return
            if declared > limit:
                await reject(413, "request body too large")
                return

        total = 0

        async def bounded_receive() -> Message:
            nonlocal total
            message = await receive()
            if message["type"] == "http.request" and not lineage:
                total += len(message.get("body", b""))
                if total > limit:
                    raise HTTPException(status_code=413, detail="request body too large")
            return message

        await self.app(scope, bounded_receive, secured_send)
