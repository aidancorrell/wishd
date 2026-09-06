"""Ask a wish:d gateway for the downstream impact of changed files.

Reads changed paths on stdin, writes Markdown on stdout. Deliberately dependency
free -- it runs on a CI runner that has not installed wish:d, so it uses the
standard library and talks to the gateway over HTTP.

Fails open: a CI job must never block a merge because the observability service
was restarting. A missing comment is an inconvenience; a red check nobody can
explain is how the whole integration gets removed.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request


def main() -> int:
    paths = [line.strip() for line in sys.stdin if line.strip()]
    if not paths:
        return 0

    base = os.environ.get("WISHD_URL", os.environ.get("DATASPINE_URL", "")).rstrip("/")
    if not base:
        print("WISHD_URL is not set", file=sys.stderr)
        return 0

    try:
        depth = int(os.environ.get("DEPTH", "5"))
        if not 1 <= depth <= 20:
            raise ValueError("depth out of range")
    except ValueError:
        print("DEPTH must be an integer between 1 and 20", file=sys.stderr)
        return 0
    payload = json.dumps({"paths": paths, "depth": depth}).encode()
    request = urllib.request.Request(
        f"{base}/api/v1/pr/impact",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    token = os.environ.get("WISHD_TOKEN", os.environ.get("DATASPINE_TOKEN"))
    if token:
        request.add_header("Authorization", f"Bearer {token}")

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = json.load(response)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        # Fail open, loudly in the log and silently on the PR.
        print(f"wish:d unreachable ({exc}); skipping impact comment", file=sys.stderr)
        return 0

    sys.stdout.write(body.get("comment") or "")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
