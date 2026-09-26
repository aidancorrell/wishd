"""Judgments, and the four ways they decline.

**Nothing here reaches the network.** Every test either runs with no key
configured — which is the shipped default and must keep the old behaviour exactly
— or substitutes the one transport function, so the question we would send and
the way we read the answer are both under test without a live service deciding
whether CI passes.

The measurements quoted in `typesafe.py` came from running the real API against
`tests/fixtures/`. They are recorded there as evidence for the design; what is
asserted here is the part that must hold on every machine: that a missing key, a
failed request, an explicit "none of these" and a low-confidence answer all land
on the caller's fallback, and that a confident answer is actually used.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from dataspine import checks, cost, typesafe

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture()
def keyed(monkeypatch):
    monkeypatch.setenv(typesafe.API_KEY_ENV, "test-key")
    return monkeypatch


def _answers(monkeypatch, answers, *, record=None):
    """Replace the transport, optionally capturing what would have been sent."""
    def fake_ask(state, questions):
        if record is not None:
            record.append({"state": state, "questions": questions})
        return answers
    monkeypatch.setattr(typesafe, "ask", fake_ask)


# ------------------------------------------------------------ off by default


def test_without_a_key_nothing_is_asked(monkeypatch):
    """The shipped default. Every caller keeps the behaviour it had before, and
    there is one fallback path rather than two."""
    monkeypatch.delenv(typesafe.API_KEY_ENV, raising=False)
    monkeypatch.delenv("WISHD_TYPESAFE_API_KEY", raising=False)

    assert typesafe.configured() is False
    assert typesafe.ask({"a": 1}, {"q": {"type": "noul", "instructions": "?"}}) == {}
    assert typesafe.classify_failure("permission denied for table raw_orders") == {}
    assert typesafe.rename_of("a", {"b": "int"}, table="t", columns={}) is None


def test_a_failed_request_is_a_missing_judgment_not_an_exception(keyed, monkeypatch):
    """Same rule the alert paths follow: a 500 from an external service is a lost
    enrichment. Letting it raise would turn that into a lost monitoring sweep."""
    class Boom:
        @staticmethod
        def post(*a, **k):
            raise OSError("connection reset")

    monkeypatch.setitem(__import__("sys").modules, "httpx", Boom)
    assert typesafe.ask({"a": 1}, {"q": {"type": "noul", "instructions": "?"}}) == {}


# ---------------------------------------------------------- reading an answer


def test_every_choice_carries_a_way_out():
    """Asked for something the options do not contain, the model must be able to
    say so. Without this it picks the least-bad wrong answer."""
    question = typesafe._choice("which?", {"a": "first", "b": "second"})
    assert typesafe.NONE_OPTION in question["criteria"]


def test_too_many_options_are_truncated_rather_than_refused():
    """A caller past the API's ceiling has a narrowing problem, but failing the
    whole judgment loses the answer for the candidates that did fit."""
    question = typesafe._choice("which?", {f"c{i}": str(i) for i in range(400)})
    assert len(question["criteria"]) == typesafe.MAX_OPTIONS
    assert typesafe.NONE_OPTION in question["criteria"]


@pytest.mark.parametrize(
    "answer,expected",
    [
        ({"choice": "a", "confidence": 0.95}, "a"),
        ({"choice": "a", "confidence": 0.5}, None),          # below the floor
        ({"choice": typesafe.NONE_OPTION, "confidence": 1.0}, None),  # said so
        ({}, None),
        (None, None),
    ],
)
def test_a_choice_is_used_only_when_it_is_confident_and_committal(answer, expected):
    got, _ = typesafe._read_choice(answer, min_confidence=0.8)
    assert got == expected


def test_a_confident_no_is_not_the_same_as_no_answer():
    """A Noul of 0.0 is the model saying "certainly not". A caller that cannot
    tell that from "nobody asked" reads a service outage as a confident no."""
    assert typesafe._read_noul({"noul": 0.0}) == 0.0
    assert typesafe._read_noul({}) is None


# ------------------------------------------------------------ failure causes


def test_a_confident_cause_is_reported_with_its_retryability(keyed, monkeypatch):
    _answers(monkeypatch, {
        "cause": {"choice": "resource_exhaustion", "confidence": 0.97},
        "retryable": {"noul": 0.2},
    })
    judged = typesafe.classify_failure("Container killed by YARN", engine="spark")

    assert judged["cause"] == "resource_exhaustion"
    assert judged["retryable"] == 0.2
    assert judged["data_problem"] is False


def test_an_unconfident_cause_is_dropped_rather_than_shown(keyed, monkeypatch):
    """The one misclassification measured came back at 0.61 while every correct
    answer sat at 0.86 or above. A wrong label sends the reader to the wrong first
    guess, and they trust the next one less."""
    _answers(monkeypatch, {
        "cause": {"choice": "transient_infra", "confidence": 0.61},
        "retryable": {"noul": 0.79},
    })
    assert typesafe.classify_failure("could not serialize access", engine="postgres") == {}


def test_whether_it_is_a_data_problem_is_derived_not_asked(keyed, monkeypatch):
    """Keep the judgment raw and the policy in code: `data_problem` is a mapping
    over the cause, so it can be argued with in a diff."""
    _answers(monkeypatch, {"cause": {"choice": "assertion", "confidence": 1.0}})
    assert typesafe.classify_failure("Got 1 result", engine="dbt")["data_problem"] is True


def test_empty_error_text_is_not_worth_a_request(keyed, monkeypatch):
    called = []
    _answers(monkeypatch, {}, record=called)
    assert typesafe.classify_failure("   ") == {}
    assert called == []


# ------------------------------------------------------------------ renames


def test_a_rename_is_offered_only_from_the_columns_actually_added(keyed, monkeypatch):
    """The options are the added columns, so this cannot invent a name."""
    sent: list = []
    _answers(monkeypatch, {"choice": {"choice": "cust_id", "confidence": 0.98}}, record=sent)

    got = typesafe.rename_of(
        "customer_id", {"cust_id": "integer", "is_gift": "boolean"},
        table="fct_orders", columns={"order_id": "integer", "cust_id": "integer"},
    )
    assert got == "cust_id"
    criteria = sent[0]["questions"]["choice"]["criteria"]
    assert set(criteria) == {"cust_id", "is_gift", typesafe.NONE_OPTION}


def test_no_rename_when_the_model_declines(keyed, monkeypatch):
    """`customer_id` removed and `customer_segment` added is not a rename, and is
    closer by every string metric than the rename that is."""
    _answers(monkeypatch, {"choice": {"choice": typesafe.NONE_OPTION, "confidence": 0.73}})
    assert typesafe.rename_of(
        "customer_id", {"customer_segment": "string"}, table="t", columns={},
    ) is None


def test_nothing_added_means_nothing_to_ask(keyed, monkeypatch):
    called = []
    _answers(monkeypatch, {}, record=called)
    assert typesafe.rename_of("customer_id", {}, table="t", columns={}) is None
    assert called == []


# ------------------------------------------- renames, through the monitor path


def _breach(**context):
    return checks.Result(status="breach", message="schema changed: removed customer_id",
                         subject="fct_orders", context=context)


def test_a_rename_is_added_to_the_message_and_changes_no_verdict(keyed, monkeypatch):
    """Only the sentence changes. A removal still breaches whether or not we can
    say where the column went."""
    monkeypatch.setattr(typesafe, "rename_of", lambda *a, **k: "cust_id")
    result = checks._annotate_rename(_breach(
        removed=["customer_id"], added=["cust_id"],
        columns={"order_id": "integer", "cust_id": "integer"},
    ))

    assert result.status == "breach"
    assert "customer_id appears to be renamed to cust_id" in result.message
    assert result.context["renamed"] == {"customer_id": "cust_id"}


def test_an_ok_result_is_never_annotated(keyed, monkeypatch):
    called = []
    monkeypatch.setattr(typesafe, "rename_of", lambda *a, **k: called.append(1) or "x")
    ok = checks.Result(status="ok", message="3 columns, unchanged",
                       context={"removed": [], "added": ["segment"]})
    assert checks._annotate_rename(ok).message == "3 columns, unchanged"
    assert called == []


def test_two_removals_cannot_both_claim_one_added_column(keyed, monkeypatch):
    """Two columns dropped in one write are two different columns. Letting both
    land on the same new name would describe a table that cannot exist."""
    monkeypatch.setattr(typesafe, "rename_of", lambda removed, added, **k: next(iter(added), None))
    result = checks._annotate_rename(_breach(
        removed=["customer_id", "order_total"], added=["cust_id"],
        columns={"cust_id": "integer"},
    ))
    assert list(result.context["renamed"].values()) == ["cust_id"]


# ------------------------------------------------------- CUR column discovery


def _cur_rows(name):
    with open(FIXTURES / name) as fh:
        return list(csv.DictReader(fh))


def test_a_recognised_export_asks_nothing(keyed, monkeypatch):
    """Both fixtures are spelled the way `COLUMNS` already lists. Paying for a
    judgment to re-derive a lookup that worked is the one thing this must not do."""
    called = []
    _answers(monkeypatch, {}, record=called)
    for name in ("aws_cur_legacy_2026-08.csv", "aws_cur_2.0_2026-08.csv"):
        assert cost.discover_columns(_cur_rows(name)[0]) == {}
    assert called == []


def test_an_unrecognised_spelling_is_resolved_from_the_headers(keyed, monkeypatch):
    """A CUR 2.0 export only emits flat columns if its own SQL aliases them, so a
    spelling we do not list is ordinary rather than exotic. See D12."""
    row = {
        "row_id": "abc123", "svc": "ElasticMapReduce", "arn": "j-2ABC",
        "usage_from": "2026-08-11T02:00:00Z", "usage_to": "2026-08-11T03:00:00Z",
        "charged_usd": "1.2400000",
    }
    _answers(monkeypatch, {
        key: {"choice": header, "confidence": 0.97}
        for key, header in [
            ("line_item_id", "row_id"), ("service", "svc"), ("resource", "arn"),
            ("start", "usage_from"), ("end", "usage_to"), ("cost", "charged_usd"),
        ]
    })
    found = cost.discover_columns(row)

    assert found["cost"] == "charged_usd"
    assert found["start"] == "usage_from"
    assert cost._pick(row, "cost", found) == "1.2400000"


def test_a_column_the_export_does_not_have_stays_missing(keyed, monkeypatch):
    """`INCLUDE_RESOURCES=TRUE` is not the default, so an export with no resource
    id is the common case. Reaching for a plausible substitute would attribute
    spend to the wrong thing while the totals still reconciled."""
    row = {"row_id": "abc123", "charged_usd": "1.24"}
    _answers(monkeypatch, {
        "resource": {"choice": typesafe.NONE_OPTION, "confidence": 0.88},
        "cost": {"choice": "charged_usd", "confidence": 0.99},
    })
    found = cost.discover_columns(row)

    assert "resource" not in found
    assert cost._pick(row, "resource", found) is None


def test_a_low_confidence_column_is_not_used(keyed, monkeypatch):
    """A misread bill is invisible: every row still imports and the totals still
    reconcile against the AWS console. Only the attribution goes wrong."""
    row = {"row_id": "abc", "blended": "1.0", "unblended": "1.2"}
    _answers(monkeypatch, {"cost": {"choice": "blended", "confidence": 0.7}})
    assert "cost" not in cost.discover_columns(row)


def test_discovered_columns_are_read_for_every_row_of_the_import(conn, keyed, monkeypatch):
    """Resolved once from the first row, then used for the whole file."""
    rows = [
        {"row_id": f"item-{i}", "charged_usd": "1.25",
         "usage_from": f"2026-08-11T0{i}:00:00Z", "svc": "ElasticMapReduce"}
        for i in range(3)
    ]
    _answers(monkeypatch, {
        "line_item_id": {"choice": "row_id", "confidence": 0.99},
        "cost": {"choice": "charged_usd", "confidence": 0.99},
        "start": {"choice": "usage_from", "confidence": 0.99},
        "service": {"choice": "svc", "confidence": 0.99},
        "end": {"choice": typesafe.NONE_OPTION, "confidence": 0.9},
        "resource": {"choice": typesafe.NONE_OPTION, "confidence": 0.9},
    })
    assert cost.import_cur(conn, rows) == 3

    stored = conn.execute(
        "select line_item_id, cost_usd from cost_line_items order by line_item_id"
    ).fetchall()
    assert [r["line_item_id"] for r in stored] == ["item-0", "item-1", "item-2"]
    assert all(float(r["cost_usd"]) == 1.25 for r in stored)


def test_an_empty_import_asks_nothing(conn, keyed, monkeypatch):
    called = []
    _answers(monkeypatch, {}, record=called)
    assert cost.import_cur(conn, []) == 0
    assert called == []


# ------------------------------------------------------------ routing on cause


def test_a_route_can_match_on_what_actually_broke():
    """`integration` was carrying this and could not bear it: a dbt model failing
    on a missing grant and the same model failing its own test are both `DBT`."""
    from dataspine import slack
    from dataspine.notify import Notification

    routes = slack.parse_routes(
        {"routes": [
            {"match": {"cause": "permission"}, "channels": ["#platform"]},
            {"match": {"cause": "assertion"}, "channels": ["#analytics"]},
            {"channels": ["#data"]},
        ]},
        source="test.yml",
    )

    def note(cause):
        return Notification(event="run_failure", status="failed", title="t",
                            summary="s", dedup_key="k", cause=cause)

    assert [r for r in routes if r.matches(note("permission"))][0].channels == ("#platform",)
    assert [r for r in routes if r.matches(note("assertion"))][0].channels == ("#analytics",)
    # Nothing judged it -- an unstated value must not match a route that asks for one.
    assert [r for r in routes if r.matches(note(None))][0].channels == ("#data",)


def test_a_judged_cause_reaches_the_notification(conn, keyed, monkeypatch):
    """End to end through the query that builds a run-failure alert."""
    from dataspine import notify

    monkeypatch.setattr(
        typesafe, "classify_failure",
        lambda *a, **k: {"cause": "permission", "confidence": 0.99,
                         "retryable": 0.1, "data_problem": False},
    )
    row = {
        "root_id": "11111111-1111-1111-1111-111111111111",
        "root_job": "analytics_daily", "leaf_job": "fct_orders",
        "failed_runs": 1, "failed_at": None,
        "error_message": "permission denied for table raw_orders",
        "leaf_integration": "DBT", "integrations": ["DBT"],
        "root_integration": "AIRFLOW", "root_facets": json.dumps({}),
        "leaf_facets": json.dumps({}),
    }
    note = notify._run_failure_notification(row)

    assert note.cause == "permission"
    assert ("looks like", "permission") in note.fields


def test_a_failing_annotation_never_downgrades_the_breach(keyed, monkeypatch):
    """`_annotate_rename` runs inside `evaluate`'s try block, where an exception
    becomes `status="error"`. An enrichment that raised would replace a correct
    breach with a failure to evaluate -- detection lost to decoration."""
    def boom(*a, **k):
        raise RuntimeError("nope")

    monkeypatch.setattr(typesafe, "rename_of", boom)
    result = checks._annotate_rename(_breach(
        removed=["customer_id"], added=["cust_id"], columns={"cust_id": "integer"},
    ))

    assert result.status == "breach"
    assert result.message == "schema changed: removed customer_id"
