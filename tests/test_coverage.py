"""Measuring how much column lineage we actually have.

Open question 3. ADR-007 settled *which* engine parses SQL; what was never
measured is how often it declines. Every decline is a missing edge, and the
failure mode that worries me is not a wrong graph — it is a graph quietly
missing a third of its edges while looking complete. An impact analysis that
silently under-reports is worse than one that says "I don't know".

So the product has to be able to state its own coverage, per source:

    facet     the producer told us. Authoritative.
    sql       SQLGlot resolved it from text.
    declined  SQLGlot refused, with a reason.

The reasons matter more than the number. "40% declined because they were all
`select *`" is a schema-registry problem; "40% declined as unparseable" is a
dialect problem; and they have completely different fixes.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dataspine import lineage

CAPTURE = Path(__file__).parent / "fixtures" / "dbt_spark_thrift_1.52.0.json"


# ------------------------------------------------------------- classification


def test_a_resolvable_statement_is_counted_as_covered():
    report = lineage.coverage_of(
        "create table fct as select o.id, c.segment from orders o "
        "join customers c on c.id = o.customer_id"
    )
    assert report["outputs"] == 2
    assert report["resolved"] == 2
    assert report["declined"] == 0


def test_a_star_select_is_declined_with_its_reason():
    """Not a parser failure — a missing schema. The fix is a catalog, not a
    better parser, and the report has to say which."""
    report = lineage.coverage_of("create table copy as select * from orders")
    assert report["declined_reason"] == "star"
    assert report["resolved"] == 0


def test_an_ambiguous_column_is_declined_as_ambiguous():
    report = lineage.coverage_of(
        "create table j as select id from a join b on a.k = b.k"
    )
    assert report["declined"] >= 1
    assert report["declined_reason"] == "ambiguous"


def test_unparseable_sql_is_declined_as_unparseable():
    report = lineage.coverage_of("ALTER TABLE x SET TBLPROPERTIES (( NOT SQL")
    assert report["declined_reason"] == "unparseable"


def test_ddl_with_no_select_is_not_counted_against_coverage():
    """dbt emits `drop table`, `alter table`, `create schema` among its
    per-statement SQL. Counting those as failures would report a coverage
    catastrophe caused entirely by statements that never had columns."""
    report = lineage.coverage_of("drop table if exists analytics.fct_orders")
    assert report["declined_reason"] == "no_projection"
    assert report["outputs"] == 0


def test_an_empty_statement_is_ignored():
    assert lineage.coverage_of("")["outputs"] == 0
    assert lineage.coverage_of(None)["outputs"] == 0


# -------------------------------------------------------- corpus aggregation


def test_a_corpus_report_aggregates_and_rates():
    statements = [
        "create table a as select x.id from src x",          # resolved
        "create table b as select * from src",               # star
        "drop table c",                                      # no projection
        "NOT SQL ((",                                        # unparseable
    ]
    report = lineage.coverage(statements)

    assert report["statements"] == 4
    # Only statements that actually project columns count toward the rate;
    # otherwise DDL drags the denominator around for no reason.
    assert report["with_projection"] == 2
    assert report["resolved_columns"] == 1
    assert report["coverage"] == pytest.approx(0.5)
    assert report["reasons"]["star"] == 1
    assert report["reasons"]["unparseable"] == 1
    assert report["reasons"]["no_projection"] == 1


def test_a_corpus_of_only_ddl_reports_no_coverage_rather_than_zero_percent():
    """Nought out of nought is not nought per cent. Reporting 0% here would send
    someone hunting a parser bug that does not exist."""
    report = lineage.coverage(["drop table a", "create schema b"])
    assert report["coverage"] is None


def test_the_report_is_ordered_worst_first():
    report = lineage.coverage(
        ["create table a as select * from s"] * 3
        + ["create table b as select s.id from s"]
    )
    assert list(report["reasons"])[0] == "star"


# ------------------------------------------------------- against real capture


@pytest.fixture(scope="module")
def real_sql() -> list[str]:
    """Every SQL statement dbt-spark actually sent through the Thrift Server."""
    events = json.loads(CAPTURE.read_text())
    statements = []
    for event in events:
        query = ((event.get("job") or {}).get("facets") or {}).get("sql") or {}
        text = query.get("query")
        if text:
            statements.append(text)
    return statements


def test_the_real_capture_contains_sql_to_measure(real_sql):
    assert real_sql, "the thrift capture should carry SQL job facets"


def test_coverage_on_the_real_corpus_is_reported_not_assumed(real_sql):
    """The measurement open question 3 asked for, on SparkSQL a real dbt-spark
    run actually emitted.

    Deliberately asserts the report is *well formed* rather than pinning a
    number: the corpus is small and the point is that coverage becomes a stated
    figure instead of an assumption. The figure itself is recorded in the
    roadmap.
    """
    report = lineage.coverage(real_sql)

    assert report["statements"] == len(real_sql)
    assert set(report["reasons"]) <= {
        "star", "ambiguous", "unparseable", "no_projection", "no_source",
        "no_upstream",
    }
    if report["coverage"] is not None:
        assert 0.0 <= report["coverage"] <= 1.0


# ------------------------------------ correct silence vs. actual missing edges


def test_a_literal_only_select_has_no_upstream_rather_than_a_missing_one():
    """`select 1 as id, 'x' as name` genuinely has no upstream table.

    Counting that as a decline understates coverage and sends someone chasing a
    parser bug that does not exist — the same mistake as reporting 0% for a
    corpus of pure DDL. Real dbt projects are full of these: every seed-style
    staging model is a literal select.
    """
    report = lineage.coverage_of("create table stg as select 1 as id, 'a' as segment")
    assert report["declined_reason"] == "no_upstream"
    assert report["outputs"] == 0, "no upstream to resolve means nothing to score"


def test_no_upstream_is_excluded_from_the_rate():
    report = lineage.coverage(
        [
            "create table a as select s.id from src s",       # 1 resolved
            "create table b as select 1 as id",               # no upstream
        ]
    )
    assert report["coverage"] == pytest.approx(1.0)
    assert report["reasons"]["no_upstream"] == 1


def test_a_column_we_genuinely_failed_on_still_counts_against_us():
    """The distinction has to cut both ways, or it becomes a way of hiding
    misses. A select over a real table whose column we cannot trace is a gap."""
    report = lineage.coverage_of(
        "create table j as select id from a join b on a.k = b.k"
    )
    assert report["declined_reason"] == "ambiguous"
    assert report["outputs"] > 0
