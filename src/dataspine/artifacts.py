"""Durable artifact capture.

Keeps `manifest.json` / `run_results.json` (and anything else worth keeping)
alive past the node that produced them. See migrations/005 for why this is
content-addressed.

Two backends: a local directory, and S3 for anyone who already has a bucket.
S3 support is lazy-imported so boto3 stays an optional dependency -- a local
install should not need an AWS SDK to store two JSON files.
"""

from __future__ import annotations

import hashlib
import logging
import re
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg

from .config import env

log = logging.getLogger("dataspine.artifacts")

STORAGE_DIR_ENV = "DATASPINE_ARTIFACT_DIR"
STORAGE_S3_ENV = "DATASPINE_ARTIFACT_S3_BUCKET"
MAX_BYTES_ENV = "DATASPINE_ARTIFACT_MAX_BYTES"

DEFAULT_MAX_BYTES = 64 * 1024 * 1024  # a dbt manifest is ~MBs; 64 is generous
# Deliberately strict. Content addressing already keeps the name off the
# filesystem, but the name is stored and rendered, so it gets validated here
# rather than relying on a single layer holding.
SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class ArtifactError(Exception):
    """Base for artifact problems."""


class ArtifactTooLarge(ArtifactError):
    pass


class ArtifactNameInvalid(ArtifactError):
    pass


class ArtifactCorrupt(ArtifactError):
    """Stored bytes do not match their digest."""


def max_bytes() -> int:
    value = int(env.get(MAX_BYTES_ENV, DEFAULT_MAX_BYTES))
    if value <= 0:
        raise ValueError("artifact size limit must be positive")
    return value


# ------------------------------------------------------------------- backends


class LocalStore:
    """Blobs on disk, sharded two levels by digest prefix.

    Sharding matters: a single flat directory with a few hundred thousand
    entries is miserable on most filesystems.
    """

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, sha: str) -> Path:
        return self.root / sha[:2] / sha[2:4] / sha

    def put(self, sha: str, content: bytes) -> str:
        path = self._path(sha)
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            # Write-then-rename: a crash mid-write must not leave a truncated
            # blob that looks valid by name.
            tmp = path.with_suffix(".tmp")
            tmp.write_bytes(content)
            tmp.replace(path)
        return f"file://{path}"

    def get(self, sha: str) -> bytes:
        return self._path(sha).read_bytes()

    def corrupt_for_test(self, sha: str, content: bytes) -> None:
        self._path(sha).write_bytes(content)


class S3Store:
    """Blobs in S3. boto3 is imported lazily and only when configured."""

    def __init__(self, bucket: str, prefix: str = "dataspine/artifacts") -> None:
        import boto3  # noqa: PLC0415 - optional dependency, only needed here

        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self._s3 = boto3.client("s3")

    def _key(self, sha: str) -> str:
        return f"{self.prefix}/{sha[:2]}/{sha[2:4]}/{sha}"

    def put(self, sha: str, content: bytes) -> str:
        key = self._key(sha)
        # Content-addressed, so an existing object is byte-identical by
        # definition; skip the upload rather than paying for it again.
        try:
            self._s3.head_object(Bucket=self.bucket, Key=key)
        except Exception:
            self._s3.put_object(Bucket=self.bucket, Key=key, Body=content)
        return f"s3://{self.bucket}/{key}"

    def get(self, sha: str) -> bytes:
        return self._s3.get_object(Bucket=self.bucket, Key=self._key(sha))["Body"].read()

    def corrupt_for_test(self, sha: str, content: bytes) -> None:  # pragma: no cover
        self._s3.put_object(Bucket=self.bucket, Key=self._key(sha), Body=content)


_store: Any = None


def get_store() -> Any:
    global _store
    if _store is None:
        bucket = env.get(STORAGE_S3_ENV)
        if bucket:
            _store = S3Store(bucket)
        else:
            _store = LocalStore(Path(env.get(STORAGE_DIR_ENV, ".dataspine/artifacts")))
    return _store


def reset_store() -> None:
    global _store
    _store = None


# --------------------------------------------------------------------- api


def put_artifact(
    conn: psycopg.Connection,
    store: Any,
    run_id: UUID,
    name: str,
    content: bytes,
    content_type: str = "application/octet-stream",
) -> dict[str, Any]:
    if not SAFE_NAME.match(name or ""):
        raise ArtifactNameInvalid(f"unsafe artifact name: {name!r}")
    if len(content) > max_bytes():
        raise ArtifactTooLarge(f"{len(content)} bytes exceeds the {max_bytes()} limit")

    sha = hashlib.sha256(content).hexdigest()
    uri = store.put(sha, content)

    conn.execute(
        """
        insert into artifact_blobs (sha256, size_bytes, storage_uri)
        values (%s, %s, %s)
        on conflict (sha256) do nothing
        """,
        (sha, len(content), uri),
    )
    conn.execute(
        """
        insert into artifacts (run_id, name, sha256, content_type)
        values (%s, %s, %s, %s)
        on conflict (run_id, name) do update set
            sha256       = excluded.sha256,
            content_type = excluded.content_type,
            updated_at   = now()
        """,
        (run_id, name, sha, content_type),
    )
    return {"name": name, "sha256": sha, "size_bytes": len(content), "storage_uri": uri}


def list_artifacts(conn: psycopg.Connection, run_id: UUID) -> list[dict[str, Any]]:
    return conn.execute(
        """
        select a.name, a.content_type, a.sha256, b.size_bytes, a.created_at, a.updated_at
        from artifacts a
        join artifact_blobs b on b.sha256 = a.sha256
        where a.run_id = %s
        order by a.name
        """,
        (run_id,),
    ).fetchall()


def get_artifact(
    conn: psycopg.Connection, store: Any, run_id: UUID, name: str
) -> tuple[bytes, dict[str, Any]]:
    row = conn.execute(
        """
        select a.name, a.sha256, a.content_type, b.size_bytes
        from artifacts a
        join artifact_blobs b on b.sha256 = a.sha256
        where a.run_id = %s and a.name = %s
        """,
        (run_id, name),
    ).fetchone()
    if row is None:
        raise KeyError(name)

    content = store.get(row["sha256"])
    # The digest is the integrity check, and checking it is the entire benefit of
    # content addressing. Serving bytes that do not match would quietly hand
    # someone the wrong manifest while looking correct.
    if hashlib.sha256(content).hexdigest() != row["sha256"]:
        raise ArtifactCorrupt(
            f"stored bytes for {name} do not match digest {row['sha256'][:12]}"
        )
    return content, row
