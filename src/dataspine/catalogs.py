"""Table catalogs: the thing that knows where a table's metadata *currently* is.

Without a catalog, the Iceberg reader in `sources.py` is barely usable on a live
table. It takes a path to a specific `NNNNN-<uuid>.metadata.json`, and Iceberg
writes a new one **on every commit** -- so a configured path is stale by the next
write, and silently: the reader keeps returning the snapshot it was pointed at,
which looks exactly like a table that stopped updating. That is the failure mode
this project keeps warning about, aimed at freshness monitoring.

A catalog resolves `analytics_marts.stg_customers` -> the current metadata
location. That is the whole job here.

**Two implementations, honestly labelled.**

  `RestCatalog` speaks the Iceberg REST spec and is **validated against a real
  catalog** -- apache/iceberg-rest-fixture 1.9.1, serving tables written by a
  real Spark 3.5.7 through dbt-spark. See tests/test_iceberg.py.

  `GlueCatalog` is built from Glue's documented `GetTable` response and has
  **never been run against an account**, exactly like the Snowflake and
  Databricks pollers (D9). Glue stores the same pointer in a table parameter
  called `metadata_location`, which is the Iceberg convention rather than a Glue
  feature, so the shape is well specified -- but it is documentation until an
  account says otherwise.

**Discovery, and why it matters.** Real captures showed openlineage-spark emits
a `catalog` dataset facet carrying `type`, `framework`, `metadataUri` and
`warehouseUri`. So on a stack that is already sending lineage, dataspine can
learn its catalogs from the events rather than being configured with them --
`catalog_from_facet` is that path. Configuration remains available for tables
nobody has written since we were installed.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger("dataspine.catalogs")

# Iceberg's own convention, and the one Glue, Hive and the REST spec all follow:
# the pointer to the current metadata file lives under this key.
METADATA_LOCATION = "metadata_location"

DEFAULT_TIMEOUT = 10


class CatalogError(Exception):
    """A catalog that could not answer. Never raised into a poll sweep."""


@dataclass
class TableRef:
    """A table as a catalog names it, plus where its metadata currently lives."""

    namespace: str  # `analytics_marts`
    name: str  # `stg_customers`
    metadata_location: str | None = None
    properties: dict[str, Any] = field(default_factory=dict)

    @property
    def identifier(self) -> str:
        return f"{self.namespace}.{self.name}" if self.namespace else self.name


# --------------------------------------------------------------- REST catalog


class RestCatalog:
    """An Iceberg REST catalog.

    Deliberately urllib rather than a client library. `pyiceberg` would pull
    pyarrow and a large dependency tree to issue two GETs against a documented
    JSON API -- the install-story argument ADR-001 keeps making, and the same
    reasoning that kept boto3 an optional extra.
    """

    kind = "rest"

    def __init__(
        self,
        uri: str,
        *,
        warehouse: str | None = None,
        token: str | None = None,
        timeout: int = DEFAULT_TIMEOUT,
        opener: Any = None,
    ) -> None:
        self.uri = uri.rstrip("/")
        self.warehouse = warehouse
        self.token = token
        self.timeout = timeout
        # Injected in tests so the HTTP layer is exercised without a live server;
        # the real one is used in the validation run.
        self._opener = opener or urllib.request.urlopen

    def _get(self, path: str) -> dict[str, Any]:
        url = f"{self.uri}/v1/{path.lstrip('/')}"
        request = urllib.request.Request(url, headers={"Accept": "application/json"})
        if self.token:
            request.add_header("Authorization", f"Bearer {self.token}")
        try:
            with self._opener(request, timeout=self.timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raise CatalogError(f"{url} returned {exc.code}") from exc
        except Exception as exc:  # noqa: BLE001 - urllib raises a wide family
            raise CatalogError(f"{url}: {exc}") from exc

    def namespaces(self) -> list[str]:
        payload = self._get("namespaces")
        return [".".join(n) for n in payload.get("namespaces") or [] if n]

    def tables(self, namespace: str) -> list[TableRef]:
        encoded = urllib.parse.quote(namespace, safe="")
        payload = self._get(f"namespaces/{encoded}/tables")
        return [
            TableRef(namespace=".".join(i.get("namespace") or []), name=i.get("name") or "")
            for i in payload.get("identifiers") or []
            if i.get("name")
        ]

    def load_table(self, namespace: str, name: str) -> TableRef:
        """Resolve a table to its *current* metadata location.

        The REST spec returns the parsed metadata inline as well, but the
        location is what we want: `sources.read_iceberg_metadata` already knows
        how to read that file, and reusing it keeps one parser rather than two
        that can disagree about which snapshot is current.
        """
        encoded = urllib.parse.quote(namespace, safe="")
        payload = self._get(f"namespaces/{encoded}/tables/{urllib.parse.quote(name, safe='')}")
        return TableRef(
            namespace=namespace,
            name=name,
            metadata_location=payload.get("metadata-location"),
            properties=payload.get("metadata", {}).get("properties") or {},
        )


# --------------------------------------------------------------- Glue catalog


class GlueCatalog:
    """AWS Glue Data Catalog, as an Iceberg catalog.

    **Unvalidated.** Written from Glue's documented `GetTable` response, never
    run against an account -- the same category as the warehouse pollers (D9)
    and for the same reason. The most likely thing to be wrong is the parameter
    casing: Iceberg writes `metadata_location`, and some tooling has been seen
    to write `metadata-location`, so both are read.

    boto3 is injected or imported lazily, so Glue support costs a local install
    nothing (ADR-001).
    """

    kind = "glue"

    def __init__(self, *, database: str | None = None, region: str | None = None,
                 client: Any = None) -> None:
        self.database = database
        self.region = region
        self._client = client

    @property
    def client(self) -> Any:
        if self._client is None:
            import boto3  # noqa: PLC0415 - optional dependency

            self._client = boto3.client("glue", region_name=self.region)
        return self._client

    def tables(self, namespace: str) -> list[TableRef]:
        paginator = self.client.get_paginator("get_tables")
        found = []
        for page in paginator.paginate(DatabaseName=namespace):
            for table in page.get("TableList") or []:
                found.append(self._to_ref(namespace, table))
        return found

    def load_table(self, namespace: str, name: str) -> TableRef:
        response = self.client.get_table(DatabaseName=namespace, Name=name)
        return self._to_ref(namespace, response.get("Table") or {})

    @staticmethod
    def _to_ref(namespace: str, table: dict[str, Any]) -> TableRef:
        params = table.get("Parameters") or {}
        location = params.get(METADATA_LOCATION) or params.get("metadata-location")
        return TableRef(
            namespace=namespace,
            name=table.get("Name") or "",
            metadata_location=location,
            properties=params,
        )


# ------------------------------------------------------------------ discovery


def catalog_from_facet(facet: Any) -> dict[str, Any] | None:
    """Read openlineage-spark's `catalog` dataset facet.

    Verified against a real capture (openlineage-spark 1.52.0 writing Iceberg
    through a REST catalog):

        {"name": "iceberg", "type": "rest", "framework": "iceberg",
         "metadataUri": "http://iceberg-rest:8181",
         "warehouseUri": "file:///warehouse/iceberg", "source": "spark"}

    This is how a catalog is *discovered* rather than configured: a stack that
    already sends lineage has told us where its catalog lives and what protocol
    it speaks. Returns None for anything that is not a usable catalog reference,
    because a half-populated facet must not produce a client pointed at nothing.
    """
    if not isinstance(facet, dict):
        return None
    metadata_uri = facet.get("metadataUri")
    kind = (facet.get("type") or "").lower()
    if not metadata_uri or not kind:
        return None
    return {
        "name": facet.get("name"),
        "type": kind,
        "framework": (facet.get("framework") or "").lower() or None,
        "metadata_uri": metadata_uri,
        "warehouse_uri": facet.get("warehouseUri"),
    }


def build(spec: dict[str, Any], *, client: Any = None) -> Any:
    """Construct a catalog client from a declared or discovered catalog."""
    kind = (spec.get("type") or "").lower()
    if kind == "rest":
        return RestCatalog(
            spec["metadata_uri"],
            warehouse=spec.get("warehouse_uri"),
            token=spec.get("token"),
        )
    if kind == "glue":
        return GlueCatalog(
            database=spec.get("database"), region=spec.get("region"), client=client
        )
    raise CatalogError(f"unsupported catalog type {kind!r}; expected 'rest' or 'glue'")


# ------------------------------------------------------------------- symlinks


def symlink_identities(facets: Any) -> list[dict[str, str]]:
    """Alternate identities a producer states for one dataset.

    This is the find that matters for Phase 04, and it is older than it looks.
    Iceberg tables arrive from Spark named by their *physical path* (`file` /
    `/warehouse/iceberg/db/table`), while dbt and every human call them
    `db.table`. Phase 04 merges identities on co-write evidence precisely because
    no single producer could tell us they were the same table.

    **That premise was wrong, and the evidence was already in the repo.** This
    facet appears 48 times in the D6 thrift fixture and 20 times in the Phase 00
    Spark fixture -- both committed before Phase 04 was designed. It is not an
    Iceberg feature; Iceberg merely made ignoring it expensive. The capture says:

        {"identifiers": [{"namespace": "http://iceberg-rest:8181",
                          "name": "analytics_marts_marts.stg_customers",
                          "type": "TABLE"}]}

    That is the producer stating the catalog identity of the path it just wrote
    -- authoritative, not inferred. It should therefore beat the co-write
    heuristic rather than merely add to it, the same way real parent propagation
    beats the query-comment inference in D3.
    """
    if not isinstance(facets, dict):
        return []
    facet = facets.get("symlinks")
    if not isinstance(facet, dict):
        return []
    out = []
    for item in facet.get("identifiers") or []:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        if not name:
            continue
        out.append(
            {
                "namespace": item.get("namespace") or "",
                "name": name,
                "type": item.get("type") or "TABLE",
            }
        )
    return out


def storage_layer(facets: Any) -> str | None:
    """The table format, from the `storage` facet: `iceberg`, `delta`, ...

    Verified: {"storageLayer": "iceberg", "fileFormat": "parquet"}. This is what
    lets the UI say a table is Iceberg without anyone declaring it.
    """
    if not isinstance(facets, dict):
        return None
    facet = facets.get("storage")
    if not isinstance(facet, dict):
        return None
    layer = facet.get("storageLayer")
    return str(layer).lower() if layer else None
