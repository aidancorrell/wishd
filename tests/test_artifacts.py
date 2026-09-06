"""Durable artifact capture.

The problem this solves is specific and is the whole reason it is in Phase 01:
dbt writes `manifest.json` and `run_results.json` into `target/` **at the end of
a run**, on a machine that may not exist a minute later. On an ephemeral EMR
node, a run that dies halfway leaves nothing behind at all -- and those are
exactly the runs you need to inspect.

So artifacts are pushed to storage we control, keyed to the run.

Storage is content-addressed by SHA-256. `manifest.json` is large (350KB in our
own fixture) and near-identical between consecutive runs of the same project;
storing one blob per upload would multiply that by the number of runs for no
benefit. Deduplication is the difference between this being cheap to keep
forever and being the first thing someone turns off.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from dataspine import artifacts
from dataspine.events import RunEvent
from dataspine.ingest import ingest_run_event
from dataspine.simulate import build_pipeline

MANIFEST = json.dumps({"nodes": {"model.analytics.fct_orders": {"x": 1}}}).encode()
RESULTS = json.dumps({"results": [{"status": "success"}]}).encode()


@pytest.fixture()
def store(tmp_path, monkeypatch):
    # reset_store() on both sides: the store is a process singleton, so without
    # this a test silently reuses the previous test's directory -- which is how
    # the corruption test first appeared to break the dedupe test.
    artifacts.reset_store()
    monkeypatch.setenv(artifacts.STORAGE_DIR_ENV, str(tmp_path / "artifacts"))
    monkeypatch.delenv(artifacts.STORAGE_S3_ENV, raising=False)
    yield artifacts.get_store()
    artifacts.reset_store()


@pytest.fixture()
def a_run(conn):
    events = build_pipeline(fail_model=None, start=datetime(2026, 8, 1, tzinfo=UTC))
    for payload in events:
        ingest_run_event(conn, RunEvent.model_validate(payload))
    return conn.execute(
        "select run_id from runs where parent_run_id is null limit 1"
    ).fetchone()["run_id"]


# --------------------------------------------------------------- round trip


def test_artifact_round_trips(conn, store, a_run):
    artifacts.put_artifact(conn, store, a_run, "manifest.json", MANIFEST, "application/json")

    listed = artifacts.list_artifacts(conn, a_run)
    assert [a["name"] for a in listed] == ["manifest.json"]
    assert listed[0]["size_bytes"] == len(MANIFEST)

    content, meta = artifacts.get_artifact(conn, store, a_run, "manifest.json")
    assert content == MANIFEST
    assert meta["content_type"] == "application/json"


def test_content_is_verified_on_read(conn, store, a_run):
    """The stored digest is the integrity check. If storage hands back something
    else, that must surface rather than being silently served."""
    artifacts.put_artifact(conn, store, a_run, "manifest.json", MANIFEST, "application/json")
    sha = hashlib.sha256(MANIFEST).hexdigest()
    store.corrupt_for_test(sha, b"tampered")

    with pytest.raises(artifacts.ArtifactCorrupt):
        artifacts.get_artifact(conn, store, a_run, "manifest.json")


# ------------------------------------------------------------ deduplication


def test_identical_content_is_stored_once(conn, store, a_run):
    """Two runs, same manifest -- one blob.

    dbt manifests barely change between runs. Without dedupe, keeping a year of
    them costs a multiple of the warehouse metadata itself.
    """
    other_run = uuid4()
    artifacts.put_artifact(conn, store, a_run, "manifest.json", MANIFEST, "application/json")
    artifacts.put_artifact(conn, store, other_run, "manifest.json", MANIFEST, "application/json")

    blobs = conn.execute("select count(*) c from artifact_blobs").fetchone()["c"]
    rows = conn.execute("select count(*) c from artifacts").fetchone()["c"]
    assert blobs == 1, "identical content was stored twice"
    assert rows == 2, "both runs should still reference it"

    # And both still read back correctly.
    for run in (a_run, other_run):
        content, _ = artifacts.get_artifact(conn, store, run, "manifest.json")
        assert content == MANIFEST


def test_reupload_of_the_same_name_replaces_metadata(conn, store, a_run):
    artifacts.put_artifact(conn, store, a_run, "run_results.json", RESULTS, "application/json")
    updated = RESULTS + b" "
    artifacts.put_artifact(conn, store, a_run, "run_results.json", updated, "application/json")

    listed = artifacts.list_artifacts(conn, a_run)
    assert len(listed) == 1
    content, _ = artifacts.get_artifact(conn, store, a_run, "run_results.json")
    assert content == updated


# ------------------------------------------------------------- out of order


def test_artifacts_can_arrive_before_the_run_does(conn, store):
    """Same reasoning as the parent chain: arrival order is not ours to control.

    dbt may finish and upload before the Airflow COMPLETE event lands, so an
    artifact must not require its run row to exist yet.
    """
    unknown = uuid4()
    artifacts.put_artifact(conn, store, unknown, "manifest.json", MANIFEST, "application/json")
    assert [a["name"] for a in artifacts.list_artifacts(conn, unknown)] == ["manifest.json"]


# ------------------------------------------------------------------ limits


def test_oversized_artifacts_are_rejected(conn, store, a_run):
    """A bound is required: this endpoint is reachable by anything holding a
    token, and an unbounded upload is a way to fill the disk."""
    too_big = b"x" * (artifacts.max_bytes() + 1)
    with pytest.raises(artifacts.ArtifactTooLarge):
        artifacts.put_artifact(conn, store, a_run, "huge.json", too_big, "application/json")
    assert artifacts.list_artifacts(conn, a_run) == []


@pytest.mark.parametrize("name", ["../escape.json", "/etc/passwd", "a/../../b", ""])
def test_path_traversal_in_names_is_rejected(conn, store, a_run, name):
    """Names come from a client. Content addressing means the name never reaches
    the filesystem, but it is still stored and rendered, so validate it rather
    than relying on that one layer."""
    with pytest.raises(artifacts.ArtifactNameInvalid):
        artifacts.put_artifact(conn, store, a_run, name, MANIFEST, "application/json")


# ---------------------------------------------------------------- HTTP API


def test_upload_and_download_over_http(api_client, tmp_path, monkeypatch):
    monkeypatch.setenv(artifacts.STORAGE_DIR_ENV, str(tmp_path / "http-artifacts"))
    artifacts.reset_store()

    events = build_pipeline(fail_model=None, start=datetime(2026, 8, 1, tzinfo=UTC))
    api_client.post("/api/v1/lineage/batch", json=events)
    run_id = api_client.get("/api/v1/runs", params={"roots_only": True}).json()["runs"][0]["run_id"]

    resp = api_client.post(
        f"/api/v1/runs/{run_id}/artifacts",
        files={"file": ("manifest.json", MANIFEST, "application/json")},
    )
    assert resp.status_code == 201, resp.text

    listing = api_client.get(f"/api/v1/runs/{run_id}/artifacts").json()
    assert [a["name"] for a in listing["artifacts"]] == ["manifest.json"]

    download = api_client.get(f"/api/v1/runs/{run_id}/artifacts/manifest.json")
    assert download.status_code == 200
    assert download.content == MANIFEST
    artifacts.reset_store()


def test_missing_artifact_404s(api_client, tmp_path, monkeypatch):
    monkeypatch.setenv(artifacts.STORAGE_DIR_ENV, str(tmp_path / "empty"))
    artifacts.reset_store()
    run_id = uuid4()
    assert api_client.get(f"/api/v1/runs/{run_id}/artifacts/nope.json").status_code == 404
    artifacts.reset_store()


def test_artifacts_appear_on_the_run_page(api_client, tmp_path, monkeypatch):
    monkeypatch.setenv(artifacts.STORAGE_DIR_ENV, str(tmp_path / "ui-artifacts"))
    artifacts.reset_store()

    events = build_pipeline(fail_model=None, start=datetime(2026, 8, 1, tzinfo=UTC))
    api_client.post("/api/v1/lineage/batch", json=events)
    run_id = api_client.get("/api/v1/runs", params={"roots_only": True}).json()["runs"][0]["run_id"]
    api_client.post(
        f"/api/v1/runs/{run_id}/artifacts",
        files={"file": ("run_results.json", RESULTS, "application/json")},
    )

    body = api_client.get(f"/runs/{run_id}").text
    assert "run_results.json" in body
    assert "Artifacts" in body
    artifacts.reset_store()


# ------------------------------------------------------------------- CLI push


def test_cli_pushes_a_dbt_target_directory(api_client, tmp_path, monkeypatch):
    """The realistic invocation: an Airflow task runs dbt, then pushes
    `target/` before the node goes away.

    Only the artifacts worth keeping are uploaded — `target/` also contains
    compiled SQL for every model and a partial-parse blob, none of which is
    worth storing per run.
    """
    from dataspine.cli import push_artifacts

    monkeypatch.setenv(artifacts.STORAGE_DIR_ENV, str(tmp_path / "cli-artifacts"))
    artifacts.reset_store()

    events = build_pipeline(fail_model=None, start=datetime(2026, 8, 1, tzinfo=UTC))
    api_client.post("/api/v1/lineage/batch", json=events)
    run_id = api_client.get("/api/v1/runs", params={"roots_only": True}).json()["runs"][0]["run_id"]

    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_bytes(MANIFEST)
    (target / "run_results.json").write_bytes(RESULTS)
    (target / "partial_parse.msgpack").write_bytes(b"noise")
    (target / "compiled").mkdir()
    (target / "compiled" / "model.sql").write_text("select 1")

    pushed = push_artifacts(target, run_id, client=api_client)

    assert sorted(pushed) == ["manifest.json", "run_results.json"]
    listing = api_client.get(f"/api/v1/runs/{run_id}/artifacts").json()["artifacts"]
    assert sorted(a["name"] for a in listing) == ["manifest.json", "run_results.json"]
    artifacts.reset_store()


def test_cli_push_is_quiet_when_there_is_nothing_to_push(api_client, tmp_path, monkeypatch):
    """A dbt run that died before writing artifacts must not fail the task that
    is trying to clean up after it."""
    from dataspine.cli import push_artifacts

    monkeypatch.setenv(artifacts.STORAGE_DIR_ENV, str(tmp_path / "none"))
    artifacts.reset_store()
    empty = tmp_path / "empty-target"
    empty.mkdir()
    assert push_artifacts(empty, str(uuid4()), client=api_client) == []
    artifacts.reset_store()
