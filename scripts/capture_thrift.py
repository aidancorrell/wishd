"""Capture what dbt-spark-over-thrift actually emits, and diff it against what
we assumed.

D6 has been open since Phase 00 with a specific, testable claim: job naming and
the parent handoff are adapter-independent and carry over from dbt-postgres, but
**dataset namespaces do not** -- and Phase 04's lineage graph joins on exactly
those.

This reads the event archive after a real dbt-spark run and prints the answer,
rather than leaving it to be inferred from a UI. Run after `make validate-thrift`.
"""

from __future__ import annotations

import json
import os
import sys
from collections import defaultdict

import psycopg
from psycopg.rows import dict_row


def main() -> int:
    url = os.environ.get("DATASPINE_DATABASE_URL")
    if not url:
        print("DATASPINE_DATABASE_URL is not set", file=sys.stderr)
        return 1

    conn = psycopg.connect(url, row_factory=dict_row)

    events = conn.execute(
        """
        select payload from events
        where payload -> 'job' ->> 'namespace' like '%%spark%%'
           or payload -> 'job' ->> 'namespace' like '%%dbt%%'
        order by received_at
        """
    ).fetchall()
    if not events:
        print("no dbt/spark events in the archive — did the run reach the gateway?")
        return 1

    namespaces: dict[str, set[str]] = defaultdict(set)
    job_names: dict[str, set[str]] = defaultdict(set)
    for row in events:
        payload = row["payload"]
        job = payload.get("job") or {}
        producer = (payload.get("producer") or "").rsplit("/", 1)[-1]
        integration = "dbt" if "dbt" in (payload.get("producer") or "") else "spark"
        job_names[integration].add(f"{job.get('namespace')} / {job.get('name')}")
        for side in ("inputs", "outputs"):
            for dataset in payload.get(side) or []:
                namespaces[integration].add(
                    f"{dataset.get('namespace')} / {dataset.get('name')}"
                )
        del producer

    print("\n=== D6: dataset namespaces, as actually emitted ===\n")
    for integration in sorted(namespaces):
        print(f"{integration}:")
        for entry in sorted(namespaces[integration]):
            print(f"    {entry}")
        print()

    print("=== job naming ===\n")
    for integration in sorted(job_names):
        for entry in sorted(job_names[integration])[:12]:
            print(f"    {entry}")
        print()

    # The specific assumption under test, stated as a pass/fail rather than left
    # for a human to squint at.
    dbt_namespaces = {n.split(" / ")[0] for n in namespaces.get("dbt", set())}
    postgres_shaped = any(n.startswith("postgres") for n in dbt_namespaces)
    print("=== verdict ===")
    print(f"  dbt dataset namespaces observed: {sorted(dbt_namespaces) or '(none)'}")
    if postgres_shaped:
        print("  UNEXPECTED: postgres-shaped namespaces from a spark target")
    else:
        print("  As predicted by D6: namespaces are NOT postgres-shaped.")
    print(
        "\n  Phase 04 note: identity resolution merges on co-write evidence, so\n"
        "  differing namespaces are handled — but `dataspine monitors --resolve`\n"
        "  and the catalog's 'same name, not merged' list are where a mismatch\n"
        "  will show up first."
    )

    out = os.environ.get("DATASPINE_CAPTURE_OUT")
    if out:
        with open(out, "w") as handle:
            json.dump([row["payload"] for row in events], handle, indent=1)
        print(f"\n  wrote {len(events)} events to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
