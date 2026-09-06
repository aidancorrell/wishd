"""Print what dbt + Iceberg actually emitted, and what the catalog holds.

Run after `make validate-iceberg`.

The question this answers is narrow and was worth asking on its own: D6 settled
what dbt-spark-over-thrift reports for *Hive* tables. Change one variable -- the
file format -- and see what moves. The answer turned out to be the dataset
identity itself, plus four facets nothing else in this project had seen.

Companion to capture_thrift.py, deliberately the same shape.
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
    rows = conn.execute(
        """
        select d ->> 'namespace' as namespace,
               d ->> 'name'      as name,
               d -> 'facets'     as facets
        from events e,
             lateral jsonb_array_elements(coalesce(e.payload -> 'outputs', '[]'::jsonb)) d
        where d -> 'facets' ? 'symlinks'
           or d -> 'facets' ? 'catalog'
           or d ->> 'name' like '%%/iceberg/%%'
        """
    ).fetchall()

    if not rows:
        print("No Iceberg datasets in the archive. Run `make validate-iceberg` first.")
        return 1

    print("=" * 78)
    print("ICEBERG DATASET IDENTITY")
    print("=" * 78)
    print("\nHow Spark names an Iceberg table (compare with the Hive rows in")
    print("capture_thrift.py, which are `spark://thrift:10000` + `schema.table`):\n")
    for namespace, name in sorted({(r["namespace"], r["name"]) for r in rows}):
        print(f"  {namespace:12} {name}")

    print("\n" + "=" * 78)
    print("DECLARED IDENTITIES (symlinks)")
    print("=" * 78)
    print("\nThe producer stating the catalog identity of the path it wrote. This is")
    print("what Phase 04 previously had to infer from co-writing:\n")
    seen = set()
    for row in rows:
        facet = (row["facets"] or {}).get("symlinks") or {}
        for item in facet.get("identifiers") or []:
            pair = (row["name"], item.get("namespace"), item.get("name"))
            if pair in seen:
                continue
            seen.add(pair)
            print(f"  {row['name']}")
            print(f"    -> {item.get('name')}  @ {item.get('namespace')}")

    print("\n" + "=" * 78)
    print("CATALOG AND STORAGE FACETS")
    print("=" * 78)
    print("\nEnough to *discover* the catalog rather than be configured with it:\n")
    kinds: dict[str, set] = defaultdict(set)
    for row in rows:
        facets = row["facets"] or {}
        catalog = facets.get("catalog") or {}
        if catalog:
            kinds["catalog"].add(
                json.dumps(
                    {
                        k: catalog.get(k)
                        for k in ("name", "type", "framework", "metadataUri", "warehouseUri")
                    },
                    sort_keys=True,
                )
            )
        storage = facets.get("storage") or {}
        if storage:
            kinds["storage"].add(
                json.dumps(
                    {k: storage.get(k) for k in ("storageLayer", "fileFormat")},
                    sort_keys=True,
                )
            )

    for kind, values in kinds.items():
        print(f"  {kind}:")
        for value in sorted(values):
            print(f"    {value}")

    print("\n" + "=" * 78)
    print("WHAT THIS MEANS")
    print("=" * 78)
    print(
        """
  * Iceberg tables are identified by PATH, not by catalog name. The namespace
    conclusion in D6 is format-dependent, not adapter-dependent as recorded.
  * `symlinks` supplies the catalog identity, so the two views of one table
    merge on a DECLARED fact rather than the co-write heuristic. Declared beats
    inferred -- the rule D3 settled for parent propagation.
  * `catalog.metadataUri` means a stack already sending lineage has told us
    where its catalog lives. See catalogs.catalog_from_facet.
"""
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
