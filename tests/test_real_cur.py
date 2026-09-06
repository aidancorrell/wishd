"""The cost model, against a real AWS bill.

D12 said the CUR column mapping was "built from the vendors' documented schemas
and never run against a real account", and predicted the column names were the
thing a real report would falsify. Two real reports arrived on 2026-08-16 — a
CUR 2.0 export and a legacy CUR, from a live account — and the column names were
*fine*. What they falsified was the primary key.

**`identity/LineItemId` does not identify a line item.** It is stable for a
resource + usage type and repeats once per hour: 139 rows here carry 27 distinct
ids, one appearing 32 times, differing only in the time interval. `import_cur`
upserted them all onto the same key, kept whichever hour landed last, reported
139 written and stored 27 — four fifths of the bill gone, with a success
message. See migration 019.

That is the failure mode this project keeps saying it fears, arriving in the one
feature whose credibility depends on reconciling with the AWS console. It was
invisible to the existing tests because a synthetic bill is generated with
unique ids; only a real one repeats them.

**The fixtures are verbatim but redacted** — account id and bucket name are
substituted, nothing else. Costs are near zero (a free account), so the row
counts are the primary assertions here. The totals check turns out to catch the
bug too, which was worth learning by running it rather than assuming: dropping
four fifths of the rows drops usage and credits in different proportions, so the
surviving total is wrong rather than merely small. On a bill of ~$0 that is a
1.6e-5 discrepancy — real, detectable, and utterly unremarkable to look at,
which is how it would have survived review on a real bill too.
"""

from __future__ import annotations

import csv
import io
from pathlib import Path

import pytest

from dataspine import cost

FIXTURES = Path(__file__).parent / "fixtures"
LEGACY = FIXTURES / "aws_cur_legacy_2026-08.csv"
CUR2 = FIXTURES / "aws_cur_2.0_2026-08.csv"


def _rows(path: Path) -> list[dict]:
    return list(csv.DictReader(io.StringIO(path.read_text())))


@pytest.fixture(params=[LEGACY, CUR2], ids=["legacy", "cur2"])
def report(request) -> list[dict]:
    """Both column families, because `cost.py` claims to read either."""
    return _rows(request.param)


# --------------------------------------------------- what the real bill is like


def test_line_item_ids_repeat_across_hours():
    """The fact the whole fix turns on, asserted so it cannot silently change.

    If a future report stops repeating ids this test fails loudly, and the
    composite key becomes merely unnecessary rather than wrong.
    """
    rows = _rows(LEGACY)
    ids = [r["identity/LineItemId"] for r in rows]
    assert len(rows) == 139
    assert len(set(ids)) == 27

    worst = max(set(ids), key=ids.count)
    group = [r for r in rows if r["identity/LineItemId"] == worst]
    assert len(group) == 32

    # Same id, same resource, same rate — different hour. The hour is the only
    # thing that tells them apart, which is why it belongs in the key.
    assert len({r["lineItem/ResourceId"] for r in group}) == 1
    assert len({r["lineItem/UsageStartDate"] for r in group}) == 32


def test_both_column_families_are_readable(report):
    """The mapping D12 predicted would be wrong, and was not."""
    for row in report:
        assert cost._pick(row, "line_item_id")
        assert cost._pick(row, "start")
        assert cost._pick(row, "cost") is not None


# ------------------------------------------------------------- the regression


def test_a_real_bill_is_stored_row_for_row(conn, report):
    """The bug: 139 in, 27 stored, "139 written" reported.

    The row count is the assertion that names the fault directly. `written` is
    checked alongside it because reporting 139 while storing 27 is the half that
    made this invisible: the CLI printed a number that was true about rows read
    and false about rows kept.
    """
    written = cost.import_cur(conn, report)
    stored = conn.execute("select count(*) c from cost_line_items").fetchone()["c"]

    assert written == len(report)
    assert stored == len(report)


def test_reimporting_the_same_report_is_idempotent(conn, report):
    """What the original key was *trying* to buy, and which still holds.

    CUR is restated through the month, so the same hour genuinely does arrive
    repeatedly. A restatement is the same (id, hour) and must update; a
    different hour of the same id must not.
    """
    cost.import_cur(conn, report)
    first = conn.execute("select count(*) c from cost_line_items").fetchone()["c"]

    cost.import_cur(conn, report)
    second = conn.execute("select count(*) c from cost_line_items").fetchone()["c"]

    assert first == second == len(report)


def test_a_restated_hour_updates_rather_than_duplicating(conn):
    """A restatement is the same id *and* hour with a new amount."""
    rows = _rows(LEGACY)
    cost.import_cur(conn, rows)

    restated = [dict(r) for r in rows]
    for row in restated:
        row["lineItem/UnblendedCost"] = "1.25"

    cost.import_cur(conn, restated)

    stored = conn.execute("select count(*) c from cost_line_items").fetchone()["c"]
    total = conn.execute("select sum(cost_usd) s from cost_line_items").fetchone()["s"]
    assert stored == len(rows)
    assert float(total) == pytest.approx(1.25 * len(rows))


def test_the_whole_bill_is_conserved(conn, report):
    """Totals must reconcile with the report, unattributable rows included.

    Expected to be weak on this fixture — the amounts are ~$0 — and it is not:
    it fails against the old key. Dropping rows drops usage and credit lines in
    different proportions, so the surviving total is wrong rather than small.
    A totals check is a real guard even on a near-empty bill.
    """
    key = "cost"
    expected = sum(float(cost._pick(r, key) or 0) for r in report)

    cost.import_cur(conn, report)
    stored = conn.execute(
        "select coalesce(sum(cost_usd), 0) s from cost_line_items"
    ).fetchone()["s"]

    assert float(stored) == pytest.approx(expected, abs=1e-6)


# ------------------------------------------------------------------ the tags


def test_unactivated_tags_arrive_as_nothing(conn):
    """Why a default export attributes nothing, in two different shapes.

    Neither report carries a usable tag, because the tag keys were not activated
    as cost allocation tags when the usage was recorded. The two versions fail
    differently, and both silently:

      * CUR 2.0 (aliased) emits `resource_tags_user_cluster` as an empty string
      * legacy omits every `resourceTags/` column entirely — 87 columns, none a tag

    Either way `_tags()` returns nothing, every row imports, every total
    reconciles, and attribution is empty. Documented as a test because it is
    indistinguishable from "the cluster was never tagged" at the point where
    someone debugs it.
    """
    legacy = _rows(LEGACY)
    assert not [c for c in legacy[0] if c.startswith("resourceTags/")]
    assert all(cost._tags(row) == {} for row in legacy)

    cur2 = _rows(CUR2)
    assert "resource_tags_user_cluster" in cur2[0]
    assert all(cost._tags(row) == {} for row in cur2)


def test_an_activated_tag_would_attribute(conn):
    """The same rows, with the tag AWS would have supplied had it been activated.

    Proves the failure above is the tag's absence and not our reader: nothing
    changes but the tag column, and attribution starts working.
    """
    conn.execute(
        "insert into clusters (cluster_id, platform, tags) "
        "values ('j-REALBILL', 'emr', '{\"cluster\": \"j-REALBILL\"}'::jsonb)"
    )
    rows = [dict(r) for r in _rows(LEGACY)]
    for row in rows:
        row["resourceTags/user:cluster"] = "j-REALBILL"

    assert cost._tags(rows[0]) == {"cluster": "j-REALBILL"}

    cost.import_cur(conn, rows, tag_key="cluster")
    attributed = conn.execute(
        "select count(*) c from cost_line_items where cluster_id = 'j-REALBILL'"
    ).fetchone()["c"]
    assert attributed == len(rows)
