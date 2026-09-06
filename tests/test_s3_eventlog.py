"""Reading Spark event logs from S3, which is where EMR actually writes them.

`ingest-eventlog` was documented as taking an S3 prefix — in `backfill.py`'s own
module docstring, and in step 4 of the roadmap's AWS runbook — and did not. It
called `Path(...).rglob`, so that step would have failed on the first real
cluster anyone pointed it at. Found on 2026-08-11 while preparing the AWS
validation session, before a cluster existed to be embarrassed by it.

Two layers of test, deliberately:

**Offline**, against a double that mimics botocore's `StreamingBody` — which is
not a file object, exposing only `read(n)` and `close()`. That distinction is
the entire reason `s3._BodyReader` exists, so the double has to reproduce it or
the test proves nothing. Using `BytesIO` here would pass against an adapter that
does not work, because `gzip` accepts a `BytesIO` directly.

**Against real S3**, skipped unless `DATASPINE_TEST_S3_BUCKET` is set. Same
discipline as the rest of the project: our own double is the control, the real
service is the variable, and every real producer so far has falsified something.
"""

from __future__ import annotations

import gzip
import io
import os
import uuid
from pathlib import Path

import pytest

from dataspine import backfill, resources, s3, sparklog

EVENTLOG = Path(__file__).parent / "fixtures" / "spark_eventlog_3.5.7.jsonl"

# EMR's real layout. The cluster id lives in the key and nowhere else, which is
# what `resources.link_applications` joins on.
EMR_PREFIX = "elasticmapreduce/j-2VALIDATION1/hadoop/spark/logs"


class _FakeBody:
    """Mimics botocore's StreamingBody: `read(n)` and `close()`, nothing else.

    Deliberately *not* an `io` object. If this grew a `readable()` or a
    `readinto()` it would stop testing the adapter it exists to test.
    """

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            chunk = self._data[self._pos :]
            self._pos = len(self._data)
            return chunk
        # Return *less* than asked for, as a real socket-backed body does. A
        # reader that assumes a full buffer per call breaks on exactly this.
        size = min(size, 8192)
        chunk = self._data[self._pos : self._pos + size]
        self._pos += len(chunk)
        return chunk

    def close(self) -> None:
        self.closed = True


class FakeS3:
    """An in-memory S3 with only the four operations this module uses."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects

    def get_object(self, Bucket: str, Key: str) -> dict:  # noqa: N803 - boto3 casing
        if Key not in self.objects:
            raise KeyError(Key)
        return {"Body": _FakeBody(self.objects[Key])}

    def head_object(self, Bucket: str, Key: str) -> dict:  # noqa: N803
        if Key not in self.objects:
            raise KeyError(Key)
        return {"ContentLength": len(self.objects[Key])}

    def get_paginator(self, name: str):
        objects = self.objects

        class _Paginator:
            def paginate(self, Bucket: str, Prefix: str):  # noqa: N803
                items = [
                    {"Key": k, "Size": len(v)}
                    for k, v in sorted(objects.items())
                    if k.startswith(Prefix)
                ]
                # Two pages, so a reader that forgets to paginate loses data.
                yield {"Contents": items[:1]}
                yield {"Contents": items[1:]}

        return _Paginator()


@pytest.fixture()
def eventlog_bytes() -> bytes:
    return EVENTLOG.read_bytes()


# ------------------------------------------------------------------- offline


def test_uri_parsing():
    assert s3.is_s3_uri("s3://bucket/key")
    assert not s3.is_s3_uri("/local/path")
    assert not s3.is_s3_uri(Path("/local/path"))
    assert s3.split_uri("s3://bucket/a/b/c") == ("bucket", "a/b/c")
    assert s3.split_uri("s3://bucket") == ("bucket", "")


def test_parse_event_log_from_s3_matches_the_local_parse(eventlog_bytes):
    """The parser must not be able to tell where the log came from."""
    local = sparklog.parse_event_log(EVENTLOG)
    fake = FakeS3({f"{EMR_PREFIX}/application_1": eventlog_bytes})

    remote = sparklog.parse_event_log(
        f"s3://logs/{EMR_PREFIX}/application_1", s3_client=fake
    )

    assert remote.app_id == local.app_id
    assert remote.task_count == local.task_count
    assert len(remote.stages) == len(local.stages)
    assert remote.truncated == local.truncated
    assert remote.skipped_lines == local.skipped_lines


def test_gzipped_logs_are_read_transparently(eventlog_bytes):
    """EMR gzips rolled logs. This is the path most likely to break, because it
    layers gzip on top of the StreamingBody adapter."""
    local = sparklog.parse_event_log(EVENTLOG)
    fake = FakeS3({f"{EMR_PREFIX}/application_1.gz": gzip.compress(eventlog_bytes)})

    remote = sparklog.parse_event_log(
        f"s3://logs/{EMR_PREFIX}/application_1.gz", s3_client=fake
    )

    assert remote.app_id == local.app_id
    assert remote.task_count == local.task_count


def test_the_body_reader_streams_rather_than_slurping(eventlog_bytes):
    """A year of logs is the use case; a multi-gigabyte log is ordinary. The
    reader must not depend on holding the object in memory."""
    body = _FakeBody(eventlog_bytes)
    reader = io.BufferedReader(s3._BodyReader(body))

    first = reader.readline()
    assert first.startswith(b"{")
    # The body is only partly consumed — proof this is a stream, not a slurp.
    assert body._pos < len(eventlog_bytes)


def test_listing_skips_zero_byte_directory_markers(eventlog_bytes):
    """The console and `aws s3 sync` both create empty marker objects. Parsed as
    event logs they are skips forever, and they are not logs."""
    fake = FakeS3(
        {
            f"{EMR_PREFIX}/": b"",
            f"{EMR_PREFIX}/application_1": eventlog_bytes,
        }
    )
    found = list(s3.list_prefix(f"s3://logs/{EMR_PREFIX}", s3=fake))
    assert [uri for uri, _ in found] == [f"s3://logs/{EMR_PREFIX}/application_1"]


def test_is_object_distinguishes_an_object_from_a_prefix(eventlog_bytes):
    fake = FakeS3({f"{EMR_PREFIX}/application_1": eventlog_bytes})
    assert s3.is_object(f"s3://logs/{EMR_PREFIX}/application_1", s3=fake)
    assert not s3.is_object(f"s3://logs/{EMR_PREFIX}", s3=fake)
    assert not s3.is_object("s3://logs", s3=fake)


def test_backfill_walks_an_s3_prefix(conn, eventlog_bytes):
    """The runbook's step 4, which previously could not work at all."""
    app_id = sparklog.parse_event_log(EVENTLOG).app_id
    text = eventlog_bytes.decode()
    fake = FakeS3(
        {
            f"{EMR_PREFIX}/application_0": text.replace(app_id, "application_0").encode(),
            f"{EMR_PREFIX}/application_1.gz": gzip.compress(
                text.replace(app_id, "application_1").encode()
            ),
            f"{EMR_PREFIX}/README.txt": b"not a log",
            f"{EMR_PREFIX}/": b"",
        }
    )

    result = backfill.backfill_directory(
        conn, f"s3://logs/{EMR_PREFIX}", s3_client=fake
    )

    assert result["ingested"] == 2
    assert result["failed"] == 0
    stored = conn.execute("select count(*) c from spark_apps").fetchone()["c"]
    assert stored == 2


def test_source_uri_keeps_the_s3_key_so_the_cluster_id_survives(conn, eventlog_bytes):
    """The join that makes cost attribution possible.

    On EMR the cluster id exists *only* in the S3 key. Storing a rewritten or
    local-looking path would leave `link_applications` with nothing to match on,
    and the failure would be silent: applications simply never acquire a cluster,
    so they are never priced.
    """
    fake = FakeS3({f"{EMR_PREFIX}/application_1": eventlog_bytes})

    backfill.backfill_directory(conn, f"s3://logs/{EMR_PREFIX}", s3_client=fake)

    uri = conn.execute("select source_uri from spark_apps").fetchone()["source_uri"]
    assert uri.startswith("s3://")
    assert resources.CLUSTER_ID_PATTERN.search(uri).group(1) == "j-2VALIDATION1"


# ----------------------------------------------------------------- real AWS
#
# Run with:
#   DATASPINE_TEST_S3_BUCKET=<bucket> pytest tests/test_s3_eventlog.py -m aws
#
# Skipped by default so the suite needs no cloud account, matching how the
# thrift and Airflow validations are structured.

BUCKET = os.environ.get("DATASPINE_TEST_S3_BUCKET")

pytestmark_aws = pytest.mark.skipif(
    not BUCKET, reason="set DATASPINE_TEST_S3_BUCKET to run against real S3"
)


@pytest.fixture()
def real_prefix():
    """A unique prefix in the configured bucket, deleted afterwards."""
    boto3 = pytest.importorskip("boto3")
    client = boto3.client("s3")
    prefix = f"dataspine-validation/{uuid.uuid4().hex[:8]}"
    yield client, prefix
    listed = client.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents") or []
    for item in listed:
        client.delete_object(Bucket=BUCKET, Key=item["Key"])


@pytest.mark.aws
@pytestmark_aws
def test_real_s3_round_trip(real_prefix, eventlog_bytes):
    """The real thing: a real event log, in real S3, through the real reader.

    Both encodings, because gzip over a real socket-backed StreamingBody is the
    combination our double can only approximate.
    """
    client, prefix = real_prefix
    key = f"{prefix}/{EMR_PREFIX}/application_real"
    client.put_object(Bucket=BUCKET, Key=key, Body=eventlog_bytes)
    client.put_object(
        Bucket=BUCKET, Key=f"{key}.gz", Body=gzip.compress(eventlog_bytes)
    )
    client.put_object(Bucket=BUCKET, Key=f"{prefix}/{EMR_PREFIX}/", Body=b"")

    local = sparklog.parse_event_log(EVENTLOG)

    plain = sparklog.parse_event_log(f"s3://{BUCKET}/{key}")
    assert plain.app_id == local.app_id
    assert plain.task_count == local.task_count
    assert len(plain.stages) == len(local.stages)

    zipped = sparklog.parse_event_log(f"s3://{BUCKET}/{key}.gz")
    assert zipped.app_id == local.app_id
    assert zipped.task_count == local.task_count

    found = [uri for uri, _ in s3.list_prefix(f"s3://{BUCKET}/{prefix}")]
    assert sorted(found) == sorted(
        [f"s3://{BUCKET}/{key}", f"s3://{BUCKET}/{key}.gz"]
    )
    assert s3.is_object(f"s3://{BUCKET}/{key}")
    assert not s3.is_object(f"s3://{BUCKET}/{prefix}")


@pytest.mark.aws
@pytestmark_aws
def test_real_s3_artifact_store_round_trip():
    """`S3Store` has shipped since Phase 01 with no coverage at all."""
    import hashlib

    from dataspine import artifacts

    content = b'{"manifest": "real s3 round trip"}'
    sha = hashlib.sha256(content).hexdigest()
    store = artifacts.S3Store(BUCKET, prefix="dataspine-validation/artifacts")
    try:
        uri = store.put(sha, content)
        assert uri.startswith(f"s3://{BUCKET}/")
        assert store.get(sha) == content
        # Content-addressed: a second put of identical bytes is a no-op, not a
        # re-upload. Asserted because the skip is a `head_object` in a `try`.
        assert store.put(sha, content) == uri
    finally:
        import boto3

        boto3.client("s3").delete_object(
            Bucket=BUCKET, Key=f"dataspine-validation/artifacts/{sha[:2]}/{sha[2:4]}/{sha}"
        )
