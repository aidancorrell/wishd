"""dbt-spark over a real Thrift Server. D6, settled — and it found worse.

D6 had been open since Phase 00 with a specific claim: job naming and the parent
handoff are adapter-independent and carry over from dbt-postgres, but **dataset
namespaces will not**, and Phase 04's lineage joins on exactly those.

The capture in `dbt_spark_thrift_1.52.0.json` is a real run: dbt-core 1.12 +
dbt-spark 1.11 + openlineage-dbt 1.52.0 against a Spark 3.5.7 Thrift Server
carrying openlineage-spark 1.52.0, both pointed at the gateway. 76 events, none
of which we wrote.

**D6's prediction was right, and it was the smaller half of the problem.**
Namespaces do differ, as predicted, and the correlator handles that. What nobody
predicted is that **the two producers' run trees never join at all.** dbt emits a
tree rooted at its own invocation; the Thrift Server's Spark listener emits a
separate tree rooted at the long-lived server application. They share no run id,
so:

  * `unstitched_runs` stays at zero — both trees are internally consistent, so the
    Phase 00 alarm cannot see this. It is a *silent* split.
  * Phase 04 identity resolution cannot merge the two views of a table, because
    co-write evidence requires a shared run tree. Every table becomes two nodes.
  * Phase 05 query attribution cannot reach the dbt models, because the Spark
    application is not in their tree.

This is D3 coming due exactly as written: "Reopen if ... the thrift server starts
serving several dbt invocations concurrently — which, given ADR-003, is likely
the first thing real EMR will show us." A shared Thrift Server cannot carry a
static `spark.openlineage.parentRunId`, so the parent has to travel per query.

These tests pin the observed reality so that a fix can be proven against it, and
so nobody re-derives the assumption from the dbt-postgres capture next to it.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

CAPTURE = Path(__file__).parent / "fixtures" / "dbt_spark_thrift_1.52.0.json"


@pytest.fixture(scope="module")
def events() -> list[dict]:
    return json.loads(CAPTURE.read_text())


def _datasets(events: list[dict]) -> set[tuple[str, str]]:
    found = set()
    for event in events:
        for side in ("inputs", "outputs"):
            for dataset in event.get(side) or []:
                found.add((dataset.get("namespace"), dataset.get("name")))
    return found


def _by_producer(events: list[dict], marker: str) -> list[dict]:
    return [e for e in events if marker in (e.get("producer") or "")]


# ------------------------------------------------------------------ naming


def test_dbt_job_naming_is_adapter_independent(events):
    """The half of D6 that carried over, confirmed rather than assumed.

    Identical to the dbt-postgres capture: `dbt-run-<project>`,
    `model.<project>.<model>`, and per-statement `.sql.N` from structured logs.
    """
    names = {e["job"]["name"] for e in events if e.get("job")}

    assert "dbt-run-analytics" in names
    assert "model.analytics.fct_orders" in names
    assert any(n.startswith("model.analytics.fct_orders.sql.") for n in names)


def test_dataset_namespaces_are_not_postgres_shaped(events):
    """D6's actual prediction, now evidence.

    dbt-spark reports the warehouse it is connected to, so the namespace is the
    Thrift Server's address and the name is `schema.table` — nothing like
    dbt-postgres's `database.schema.table` under a `postgres://` namespace.
    """
    dbt_datasets = _datasets(_by_producer(events, "dbt"))
    namespaces = {namespace for namespace, _ in dbt_datasets}

    assert namespaces, "the capture should contain dbt datasets"
    assert not any(n.startswith("postgres") for n in namespaces)
    assert any(n.startswith("spark://") for n in namespaces)


def test_the_two_producers_name_the_same_table_differently(events):
    """dbt says `analytics_marts_marts.fct_orders` under `spark://thrift:10000`;
    the Spark listener says `/warehouse/…/fct_orders` under `file`. One table."""
    dbt_names = {name for _, name in _datasets(_by_producer(events, "dbt"))}
    spark_names = {name for _, name in _datasets(_by_producer(events, "spark"))}

    assert any(n.endswith("analytics_marts_marts.fct_orders") for n in dbt_names)
    assert any(n.endswith("/fct_orders") for n in spark_names)
    assert dbt_names.isdisjoint(spark_names)


def test_the_leaf_name_still_matches_across_producers(events):
    """The one thing that does survive, and what identity resolution needs.

    Both sides end in `fct_orders`, so leaf matching still finds the pair. What
    is missing is the *evidence* to act on it — see the run-tree test below.
    """
    def leaf(name: str) -> str:
        return name.rsplit("/", 1)[-1].rsplit(".", 1)[-1]

    dbt_leaves = {leaf(n) for _, n in _datasets(_by_producer(events, "dbt"))}
    spark_leaves = {leaf(n) for _, n in _datasets(_by_producer(events, "spark"))}

    assert "fct_orders" in dbt_leaves & spark_leaves


# ------------------------------------------------------------- the surprise


def test_the_producers_run_trees_do_not_join(events):
    """The finding that matters, and the reason this capture exists.

    Phase 00 proved three producers assemble into one tree — but that run had a
    `spark-submit` whose parent run id the DAG set explicitly. A **shared Thrift
    Server cannot carry a static parent**: it is one long-lived application
    serving every dbt invocation that connects to it.

    So the trees are disjoint, and nothing in the system currently notices.
    """
    def roots(marker: str) -> set[str]:
        found = set()
        for event in _by_producer(events, marker):
            facets = (event.get("run") or {}).get("facets") or {}
            parent = facets.get("parent") or {}
            root = ((parent.get("root") or {}).get("run") or {}).get("runId")
            found.add(root or (parent.get("run") or {}).get("runId")
                      or event["run"]["runId"])
        return found

    assert roots("dbt").isdisjoint(roots("spark")), (
        "if these ever overlap, the parent is being propagated and this "
        "capture is stale"
    )


def test_no_spark_event_claims_a_dbt_parent(events):
    """The mechanism behind the split, asserted directly.

    The Thrift Server's events carry no `parent` facet pointing at dbt, because
    nothing told it which query belonged to which invocation.
    """
    for event in _by_producer(events, "spark"):
        parent = ((event.get("run") or {}).get("facets") or {}).get("parent") or {}
        job = (parent.get("job") or {})
        assert "dbt" not in str(job.get("namespace", "")), (
            "a Spark event claiming a dbt parent means propagation now works"
        )


def test_the_split_is_silent_rather_than_alarming(events):
    """Why this went unnoticed until someone looked.

    `unstitched_runs` — the Phase 00 correlation alarm — fires when a run names a
    parent nobody sent. Here both trees are internally consistent and neither
    names a missing parent, so the alarm stays at zero while the graph is wrong.
    That is a gap in the alarm, not just in the propagation.
    """
    dangling = []
    known = {e["run"]["runId"] for e in events}
    for event in events:
        parent = ((event.get("run") or {}).get("facets") or {}).get("parent") or {}
        parent_id = (parent.get("run") or {}).get("runId")
        if parent_id and parent_id not in known:
            dangling.append(parent_id)

    assert not dangling, (
        "no dangling parents: the split produces two valid trees, which is "
        "exactly why unstitched_runs cannot detect it"
    )


# ------------------------------------------------------------- spark quirk


def test_spark_reports_a_doubled_warehouse_path(events):
    """A real openlineage-spark quirk, pinned so a future version change is visible.

    The same table is reported under both `/warehouse/<db>.db/<table>` and
    `/warehouse/<db>.db/<db>.db/<table>`. Identity resolution merges these two
    (they are co-written in the Thrift Server's own tree), so it is currently
    harmless — but it is the kind of thing that silently doubles a node count if
    the merge ever stops working.
    """
    spark_names = {name for _, name in _datasets(_by_producer(events, "spark"))}
    doubled = [n for n in spark_names if n.count("analytics_marts_marts.db") > 1]

    assert doubled, "expected the doubled-path form in this capture"
