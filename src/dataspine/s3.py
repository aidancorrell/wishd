"""Reading objects from S3, which is where EMR actually writes event logs.

`ingest-eventlog` was documented — in its own docstring, in the README and in the
roadmap's AWS runbook — as taking an S3 prefix, and did not: it called
`Path(...).rglob`, so a real `s3://` argument failed on the first cluster anyone
pointed it at. This module is that gap closed.

Two properties matter more than they look:

**Streaming, not slurping.** A backfill's whole purpose is a year of logs, and a
single uncompressed event log from a long application is routinely gigabytes.
The reader below wraps the response body rather than calling `.read()` on it, so
memory stays bounded by the buffer regardless of object size.

**boto3 stays lazy and optional.** Imported inside the call, exactly as
`artifacts.S3Store` does it, for the reason ADR-001 keeps restating: a local
install should not need an AWS SDK to evaluate a freshness monitor.
"""

from __future__ import annotations

import gzip
import io
import logging
from collections.abc import Iterator
from typing import Any

log = logging.getLogger("dataspine.s3")

SCHEME = "s3://"


def is_s3_uri(value: Any) -> bool:
    return str(value).startswith(SCHEME)


def split_uri(uri: str) -> tuple[str, str]:
    """`s3://bucket/a/b` -> `("bucket", "a/b")`."""
    bucket, _, key = str(uri)[len(SCHEME) :].partition("/")
    return bucket, key


def client(existing: Any = None) -> Any:
    """The caller may inject a client; otherwise build one lazily."""
    if existing is not None:
        return existing
    import boto3  # noqa: PLC0415 - optional dependency, only needed here

    return boto3.client("s3")


class _BodyReader(io.RawIOBase):
    """Adapts a botocore StreamingBody to something `io` can layer on.

    StreamingBody has `.read(n)` but is not an `io.RawIOBase`, so `gzip` and
    `TextIOWrapper` will not accept it directly. Implementing `readinto` is the
    whole adapter.
    """

    def __init__(self, body: Any) -> None:
        self._body = body

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int:
        chunk = self._body.read(len(buffer))
        if not chunk:
            return 0
        buffer[: len(chunk)] = chunk
        return len(chunk)

    def close(self) -> None:
        try:
            self._body.close()
        finally:
            super().close()


def open_text(uri: str, *, s3: Any = None) -> Any:
    """A text handle over an S3 object, transparently gunzipping `.gz`.

    Mirrors `sparklog._open` so the parser cannot tell the difference between a
    local log and one still sitting where EMR left it.
    """
    bucket, key = split_uri(uri)
    body = client(s3).get_object(Bucket=bucket, Key=key)["Body"]
    raw = io.BufferedReader(_BodyReader(body))
    if key.endswith(".gz"):
        return gzip.open(raw, "rt", encoding="utf-8", errors="replace")
    return io.TextIOWrapper(raw, encoding="utf-8", errors="replace")


def is_object(uri: str, *, s3: Any = None) -> bool:
    """True if the URI names one object rather than a prefix.

    S3 has no directories, so `s3://b/logs` is both a plausible object and a
    plausible prefix and only the account can say which. Asked directly rather
    than guessed from a trailing slash, because EMR's log prefixes do not have
    one.
    """
    bucket, key = split_uri(uri)
    if not key:
        return False
    try:
        client(s3).head_object(Bucket=bucket, Key=key)
    except Exception:
        return False
    return True


def list_prefix(uri: str, *, s3: Any = None) -> Iterator[tuple[str, int]]:
    """Yield `(s3://… , size)` for every object under a prefix, paginated.

    Zero-byte keys are skipped: S3 has no directories, but the console and
    `aws s3 sync` both create empty marker objects that would otherwise be
    parsed as empty event logs and counted as skips forever.
    """
    bucket, prefix = split_uri(uri)
    paginator = client(s3).get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents") or []:
            if not item.get("Size"):
                continue
            yield f"{SCHEME}{bucket}/{item['Key']}", int(item["Size"])
