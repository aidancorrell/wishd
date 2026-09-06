"""Reading dbt's own artifacts — the file both dbt-core and dbt Cloud produce.

Every test here runs against `tests/fixtures/dbt_*_1.12.3.json`, which are the
real files from a real `dbt build` on dbt 1.12.3: two models, two passing tests,
one genuine failure and one genuine warning. Nothing about the shape is
constructed, which matters because the three things that bite are all things a
handwritten fixture would have got wrong.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dataspine import dbt_artifacts as dbt

FIXTURES = Path(__file__).parent / "fixtures"


def _before(invocation):
    """A window that actually contains the invocation's measurements.

    `generated_at` is when dbt wrote run_results.json, which is after the last
    node finished — so a window starting there contains none of them.
    """
    from datetime import timedelta

    return invocation.started_at - timedelta(minutes=1)


@pytest.fixture(scope="module")
def run_results() -> dict:
    return json.loads((FIXTURES / "dbt_run_results_1.12.3.json").read_text())


@pytest.fixture(scope="module")
def manifest() -> dict:
    return json.loads((FIXTURES / "dbt_manifest_1.12.3.json").read_text())


@pytest.fixture()
def invocation(run_results, manifest):
    return dbt.parse(run_results, manifest)


# ------------------------------------------------------------------- parsing


def test_the_invocation_describes_itself(invocation):
    assert invocation.project == "analytics"
    assert invocation.adapter == "postgres"
    assert invocation.dbt_version == "1.12.3"
    assert invocation.invocation_id
    assert invocation.started_at <= invocation.generated_at


def test_models_and_tests_are_told_apart(invocation):
    assert len(invocation.models) == 2
    assert len(invocation.tests) == 4
    assert {n.name for n in invocation.models} == {"fct_orders", "fct_order_items"}


def test_a_relation_name_is_unquoted(invocation):
    """dbt hands back `"postgres"."analytics_marts"."fct_orders"`, exactly as the
    adapter renders it. Every table name here is matched on its final segment,
    and the quoted form reduces to `fct_orders"` — quote attached — which would
    never match the table any other producer reports."""
    relations = {n.relation for n in invocation.models}
    assert relations == {
        "postgres.analytics_marts.fct_orders",
        "postgres.analytics_marts.fct_order_items",
    }
    assert not any('"' in (r or "") for r in relations)


def test_severity_is_normalised(invocation):
    """dbt writes the default as `ERROR` and a user-set one as they typed it, so
    the real fixture contains both `"ERROR"` and `"warn"`."""
    severities = {n.severity for n in invocation.tests}
    assert severities == {"error", "warn"}


def test_a_warning_is_not_something_to_page_about(invocation):
    """Its author said in writing that they did not want waking."""
    warned = [t for t in invocation.tests if t.warn_only]
    assert [t.name for t in warned] == ["unique_fct_order_items_item_id"]
    assert warned[0].status == "warn"


def test_a_test_names_no_table_of_its_own(invocation):
    """`relation_name` is null on every test node, which is why the manifest is
    needed: the link lives only in `attached_node`."""
    assert all(t.relation is None for t in invocation.tests)
    failing = next(t for t in invocation.tests if t.status == "fail")
    assert dbt.tested_relation(invocation, failing) == (
        "postgres.analytics_marts.fct_order_items"
    )


def test_node_state_maps_onto_run_state(invocation):
    by_name = {n.name: n for n in invocation.nodes}
    assert by_name["fct_orders"].state == "COMPLETED"
    assert by_name["not_null_fct_order_items_order_id"].state == "FAILED"
    # A warn ran perfectly well; what warned is the assertion it made.
    assert by_name["unique_fct_order_items_item_id"].state == "COMPLETED"


def test_a_missing_manifest_still_parses(run_results):
    """Someone uploading only run_results.json gets models and statuses; what
    they lose is the ability to say which table a test asserted on."""
    invocation = dbt.parse(run_results)

    assert invocation.project == "analytics"
    assert len(invocation.tests) == 4
    assert dbt.test_results(invocation) == [], "no table to attribute them to"
    assert {n.relation for n in invocation.models} == {
        "postgres.analytics_marts.fct_orders",
        "postgres.analytics_marts.fct_order_items",
    }


def test_rubbish_is_refused(invocation):
    with pytest.raises(dbt.DbtArtifactError):
        dbt.parse({"nonsense": True})


# -------------------------------------------------------------- test results


def test_test_results_carry_the_failing_row_count(invocation):
    rows = {r["check"]: r for r in dbt.test_results(invocation)}

    failing = rows["not_null_fct_order_items_order_id"]
    assert failing["status"] == "fail"
    assert failing["value"] == 1
    assert failing["table"] == "postgres.analytics_marts.fct_order_items"
    assert failing["details"]["column"] == "order_id"
    assert failing["details"]["test_type"] == "not_null"
    assert failing["details"]["severity"] == "error"


def test_a_warning_is_recorded_as_a_failure_with_its_severity(invocation):
    """It did fail the assertion. Whether that pages anyone is a later decision,
    and one the ledger should not pre-empt by losing the row."""
    row = next(
        r for r in dbt.test_results(invocation)
        if r["check"] == "unique_fct_order_items_item_id"
    )
    assert row["status"] == "fail"
    assert row["details"]["severity"] == "warn"


def test_passing_tests_are_recorded_too(invocation):
    passing = [r for r in dbt.test_results(invocation) if r["status"] == "pass"]
    assert len(passing) == 2
    assert all(r["value"] == 0 for r in passing)


def test_a_skipped_test_is_not_evidence(run_results, manifest):
    """Recording it as a pass would let an upstream failure read as a clean bill
    of health."""
    skipped = json.loads(json.dumps(run_results))
    for result in skipped["results"]:
        if result["unique_id"].startswith("test."):
            result["status"] = "skipped"

    assert dbt.test_results(dbt.parse(skipped, manifest)) == []


def test_results_land_in_external_checks(conn, invocation):
    from dataspine import dq

    written = dq.import_results(conn, source="dbt", rows=dbt.test_results(invocation))

    assert written == 4
    failing = dq.failing(conn)
    assert {r["check_name"] for r in failing} == {
        "not_null_fct_order_items_order_id",
        "unique_fct_order_items_item_id",
    }


def test_reimporting_the_same_invocation_does_not_multiply_rows(conn, invocation):
    from dataspine import dq

    rows = dbt.test_results(invocation)
    dq.import_results(conn, source="dbt", rows=rows)
    dq.import_results(conn, source="dbt", rows=rows)

    assert conn.execute(
        "select count(*) as n from external_checks"
    ).fetchone()["n"] == 4


def test_a_test_attaches_to_the_table_whatever_case_it_arrives_in(conn, invocation):
    """Snowflake reports FCT_ORDERS and everything else reports fct_orders; an
    unquoted SQL identifier means the same table either way."""
    from dataspine import dq

    dataset = conn.execute(
        "insert into datasets (namespace, name) values ('snowflake://acme', %s) returning id",
        ("ANALYTICS.MARTS.FCT_ORDER_ITEMS",),
    ).fetchone()["id"]
    dq.import_results(conn, source="dbt", rows=dbt.test_results(invocation))

    linked = conn.execute(
        "select dataset_id from external_checks where check_name = %s",
        ("not_null_fct_order_items_order_id",),
    ).fetchone()
    assert linked["dataset_id"] == dataset


# ------------------------------------------------------- synthesised events


def test_events_name_things_the_way_dbt_ol_does(invocation):
    """A shop running dbt-core *and* dbt Cloud should see one vocabulary."""
    events = dbt.events(invocation)
    names = {e["job"]["name"] for e in events}

    assert "analytics.run" in names
    assert "analytics.model.analytics.fct_orders" in names


def test_every_run_gets_a_start_and_a_terminal_event(invocation):
    events = dbt.events(invocation)
    starts = [e for e in events if e["eventType"] == "START"]
    terminal = [e for e in events if e["eventType"] in ("COMPLETE", "FAIL", "ABORT")]

    assert len(starts) == len(terminal) == len(invocation.nodes) + 1, "nodes plus the root"


def test_a_failed_test_fails_the_invocation(invocation):
    """dbt itself exits non-zero, and the run tree should say the same."""
    events = dbt.events(invocation)
    root = [e for e in events if e["job"]["name"] == "analytics.run"]

    assert [e["eventType"] for e in root] == ["START", "FAIL"]


def test_a_warning_does_not_fail_its_own_run(invocation):
    events = dbt.events(invocation)
    warned = [
        e for e in events
        if "unique_fct_order_items_item_id" in e["job"]["name"]
    ]
    assert {e["eventType"] for e in warned} == {"START", "COMPLETE"}


def test_models_write_datasets_and_tests_read_them(invocation):
    events = dbt.events(invocation)
    outputs = {o["name"] for e in events for o in e["outputs"]}
    inputs = {i["name"] for e in events for i in e["inputs"]}

    assert outputs == {
        "postgres.analytics_marts.fct_orders",
        "postgres.analytics_marts.fct_order_items",
    }
    assert inputs == outputs, "every test asserts on a table this run built"


def test_every_node_hangs_off_the_one_root(invocation):
    events = dbt.events(invocation)
    root_id = next(
        e["run"]["runId"] for e in events if e["job"]["name"] == "analytics.run"
    )
    children = [e for e in events if e["job"]["name"] != "analytics.run"]

    assert children
    assert all(
        e["run"]["facets"]["parent"]["run"]["runId"] == root_id for e in children
    )


def test_run_ids_are_deterministic(invocation):
    """Re-ingesting the same artifacts must change nothing, not duplicate an
    entire invocation."""
    first = [e["run"]["runId"] for e in dbt.events(invocation)]
    second = [e["run"]["runId"] for e in dbt.events(invocation)]
    assert first == second


def test_a_skipped_node_produces_no_run(run_results, manifest):
    """It did not run. A run in the tree that never existed would make every
    duration over that tree measure dbt's scheduling rather than any work."""
    skipped = json.loads(json.dumps(run_results))
    skipped["results"][0]["status"] = "skipped"
    invocation = dbt.parse(skipped, manifest)
    target = invocation.nodes[0].unique_id

    events = dbt.events(invocation)

    assert not any(target in e["job"]["name"] for e in events)


def test_the_dataset_namespace_can_be_stated(invocation):
    events = dbt.events(invocation, namespace="snowflake://acme")
    assert {o["namespace"] for e in events for o in e["outputs"]} == {"snowflake://acme"}


def test_synthesised_events_are_ingestible(conn, invocation):
    """The point of synthesising: dbt Cloud becomes an ordinary producer, and
    nothing downstream needs a special case."""
    from dataspine.events import RunEvent
    from dataspine.ingest import ingest_run_event

    for event in dbt.events(invocation):
        ingest_run_event(conn, RunEvent.model_validate(event))

    tree = conn.execute(
        "select job_name, state, integration from run_summary where job_name = 'analytics.run'"
    ).fetchone()
    assert tree["state"] == "FAILED"
    assert tree["integration"] == "DBT"

    children = conn.execute(
        "select count(*) as n from runs where parent_run_id is not null"
    ).fetchone()["n"]
    assert children == len(invocation.nodes)


# ------------------------------------------------------------ the dbt-core path


def test_uploading_artifacts_records_the_tests(api_client, conn):
    """dbt-core needs no new integration: `push-artifacts` has been uploading
    these two files since Phase 02, and they sat in the store unread."""
    from uuid import uuid4

    run_id = uuid4()
    for name in ("manifest.json", "run_results.json"):
        response = api_client.post(
            f"/api/v1/runs/{run_id}/artifacts",
            files={"file": (name, (FIXTURES / f"dbt_{name.replace('.json','')}"
                                   "_1.12.3.json").read_bytes(), "application/json")},
        )
        assert response.status_code == 201

    assert response.json()["checks_imported"] == 4, "two models, four tests"


def test_a_corrupt_artifact_does_not_fail_the_upload(api_client):
    """A rejected upload loses the artifact entirely, and the parse can always
    be retried."""
    from uuid import uuid4

    response = api_client.post(
        f"/api/v1/runs/{uuid4()}/artifacts",
        files={"file": ("run_results.json", b"{not json", "application/json")},
    )

    assert response.status_code == 201
    assert response.json()["checks_imported"] == 0


def _elsewhere(rows, source="snowflake"):
    """The same checks, attributed to a source with no dbt invocation around
    them — a Snowflake DMF or a Databricks rule. Those still take the flat path,
    because there is no job to hang a thread off."""
    return [dict(r, check=f"{source}_{r['check']}") for r in rows]


def test_dbt_checks_do_not_also_arrive_flat(conn, invocation):
    """They come with an invocation around them, and that invocation gets one
    message with every failure in its thread. Repeating them here would say the
    same thing twice, in the shape the threading exists to avoid."""
    from dataspine import dq, notify

    dq.import_results(conn, source="dbt", rows=dbt.test_results(invocation))

    assert notify.data_test_notifications(conn, since=_before(invocation)) == []


def test_a_source_with_no_job_still_alerts_flat(conn, invocation):
    """Snowflake DMFs and Databricks rules have no invocation to thread under."""
    from dataspine import dq, notify

    dq.import_results(
        conn, source="snowflake", rows=_elsewhere(dbt.test_results(invocation))
    )

    notes = notify.data_test_notifications(conn, since=_before(invocation))

    assert [n.event for n in notes] == ["data_test"]
    assert notes[0].status == "fail"
    assert notes[0].dataset == "postgres.analytics_marts.fct_order_items"


def test_a_flat_check_failing_every_hour_is_one_notification(conn, invocation):
    """The rule monitor alerting already follows. 72 messages about one problem
    is how the channel gets muted, taking every future alert with it."""
    from datetime import timedelta

    from dataspine import dq, notify

    rows = _elsewhere(dbt.test_results(invocation))
    since = invocation.generated_at - timedelta(days=1)
    for hour in range(6):
        dq.import_results(
            conn, source="snowflake",
            rows=[dict(r, measured_at=r["measured_at"] + timedelta(hours=hour)) for r in rows],
        )

    notes = notify.data_test_notifications(conn, since=since)

    assert len([n for n in notes if n.status == "fail"]) == 1


def test_a_flat_recovery_is_delivered(conn, invocation):
    """A check that only ever speaks when it breaks leaves an unresolved failure
    indistinguishable from an ongoing one."""
    from datetime import timedelta

    from dataspine import dq, notify

    rows = _elsewhere(dbt.test_results(invocation))
    since = invocation.generated_at - timedelta(days=1)
    dq.import_results(conn, source="snowflake", rows=rows)
    dq.import_results(
        conn, source="snowflake",
        rows=[
            dict(r, status="pass", value=0, measured_at=r["measured_at"] + timedelta(hours=1))
            for r in rows
        ],
    )

    recovered = [
        n for n in notify.data_test_notifications(conn, since=since) if n.status == "ok"
    ]
    assert len(recovered) == 1


def test_flat_checks_route_to_the_data_channel(conn, invocation, monkeypatch, tmp_path):
    from dataspine import dq, notify, slack

    path = tmp_path / "slack.yml"
    path.write_text(
        "routes:\n"
        "  - match: {event: [monitor, incident, data_test, dbt_job]}\n"
        "    channel: '#data-alerts'\n"
        "  - match: {event: [pipeline, digest]}\n    channel: '#data-pipelines'\n"
    )
    monkeypatch.setenv(slack.ROUTES_ENV, str(path))
    dq.import_results(
        conn, source="snowflake", rows=_elsewhere(dbt.test_results(invocation))
    )

    note = notify.data_test_notifications(conn, since=_before(invocation))[0]

    assert slack.destinations(note, slack.load_routes()) == ("#data-alerts",)


def test_a_team_can_route_its_own_tables(conn, invocation, tmp_path):
    from dataspine import slack

    path = tmp_path / "slack.yml"
    path.write_text(
        "routes:\n"
        "  - match: {dataset: '*.fct_order_items'}\n    channel: '#finance-data'\n"
        "  - channel: '#data-alerts'\n"
    )
    routes = slack.load_routes(path)
    note = notify_note(invocation)

    assert slack.destinations(note, routes) == ("#finance-data",)


def notify_note(invocation):
    from dataspine import notify

    return notify.Notification(
        event="data_test", status="fail", title="t", summary="s", dedup_key="k",
        dataset="postgres.analytics_marts.fct_order_items",
    )


# --------------------------------------- what a flat check hands the reader


@pytest.fixture()
def snowflake_invocation():
    """A real dbt Cloud run against Snowflake, where `adapter_response` carries
    the warehouse's own query id."""
    return dbt.parse(
        json.loads((FIXTURES / "dbt_cloud_snowflake_run_results.json").read_text()),
        json.loads((FIXTURES / "dbt_cloud_snowflake_manifest.json").read_text()),
    )


def test_a_stored_check_keeps_the_query_that_found_the_rows(
    conn, snowflake_invocation
):
    """The compiled SQL and the warehouse statement id both survive into
    `external_checks.details`, which is what the alert reads."""
    from dataspine import dq

    rows = _elsewhere(dbt.test_results(snowflake_invocation))
    dq.import_results(conn, source="snowflake", rows=rows)

    failing = next(r for r in rows if r["status"] == "fail")
    assert failing["details"]["query_id"]
    # The compiled assertion, fully qualified against the warehouse.
    assert "ANALYTICS_DB.PUBLIC" in failing["details"]["compiled_sql"]


def test_the_flat_alert_offers_the_query_and_the_table(
    conn, snowflake_invocation, monkeypatch
):
    from dataspine import dq, links, notify

    monkeypatch.setenv(
        links.SNOWFLAKE_ACCOUNT_ENV, "https://app.snowflake.com/myorg/ab12345"
    )
    dq.import_results(
        conn, source="snowflake",
        rows=_elsewhere(dbt.test_results(snowflake_invocation)),
    )

    note = next(
        n
        for n in notify.data_test_notifications(
            conn, since=_before(snowflake_invocation)
        )
        if n.status == "fail"
    )
    labels = [label for label, _ in note.links]
    assert "Snowflake query" in labels
    assert labels == ["Snowflake query"]
    assert "select" not in note.summary.lower()


def test_a_recovery_does_not_link_the_query_that_passed(
    conn, snowflake_invocation, monkeypatch
):
    """Sending someone to look at a clean result is a wasted click."""
    from datetime import timedelta

    from dataspine import dq, links, notify

    monkeypatch.setenv(
        links.SNOWFLAKE_ACCOUNT_ENV, "https://app.snowflake.com/myorg/ab12345"
    )
    rows = _elsewhere(dbt.test_results(snowflake_invocation))
    # Severity matters: a `warn` test never notifies in either direction, so a
    # recovery of one would be a message about an event nobody was told about.
    failing = next(
        r for r in rows
        if r["status"] == "fail" and r["details"]["severity"] == "error"
    )
    dq.import_results(conn, source="snowflake", rows=[failing])
    dq.import_results(
        conn,
        source="snowflake",
        rows=[dict(failing, status="pass", value=0,
                   measured_at=failing["measured_at"] + timedelta(minutes=5))],
    )

    recovered = next(
        n
        for n in notify.data_test_notifications(
            conn, since=_before(snowflake_invocation)
        )
        if n.status == "ok"
    )
    assert all(label != "Snowflake query" for label, _ in recovered.links)
