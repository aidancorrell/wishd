"""Iceberg: what it actually emits, and the catalog that makes it pollable.

Captured 2026-08-17 from a real run — dbt-core 1.12 + dbt-spark 1.11 against a
Spark 3.5.7 Thrift Server carrying openlineage-spark 1.52.0, materialising
`file_format: iceberg` through an Apache Iceberg REST catalog 1.9.1. One
variable changed from the D6 thrift validation: the file format. So anything
different here is attributable to Iceberg and nothing else.

**Iceberg changes the dataset identity, which D6 did not predict.** A Hive table
over thrift arrives as `spark://thrift:10000` / `analytics_marts_marts.stg_customers`.
The same model as Iceberg arrives as `file` /
`/warehouse/iceberg/analytics_marts_marts/stg_customers` — the physical path, not
the catalog name. On its own that splits every table in two on the reference
deployment, exactly as the disjoint trees did in D6.

**And the fix was already in the events, which is the more uncomfortable
finding.** Spark attaches a `symlinks` facet naming the catalog identity of the
path it wrote. Phase 04 built co-write inference *because* no single producer
could tell us two names were one table — and that premise was wrong: this facet
appears **48 times in the D6 thrift fixture and 20 times in the Phase 00 Spark
fixture**, both committed to this repo since before Phase 04 was designed. It is
not an Iceberg feature and never was; Iceberg only made ignoring it expensive
enough to notice, by naming tables in a way that splits them without it.

So the declared identity now takes precedence over the inference (the same rule
D3 settled for parent propagation), and it improves the Hive captures
retroactively, not just Iceberg.

`catalog` (type, framework, metadataUri — enough to *discover* the catalog from
lineage rather than configure it) has the same history. `storage`
(storageLayer: iceberg) is what tells the UI a table's format without anyone
declaring it.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dataspine import catalogs

FIXTURE = Path(__file__).parent / "fixtures" / "dbt_iceberg_rest_1.52.0.json"


@pytest.fixture(scope="module")
def events() -> list[dict]:
    return json.loads(FIXTURE.read_text())


def _datasets(events: list[dict]) -> list[dict]:
    return [d for e in events for k in ("inputs", "outputs") for d in (e.get(k) or [])]


def _iceberg_outputs(events: list[dict]) -> list[dict]:
    return [
        d
        for e in events
        for d in (e.get("outputs") or [])
        if "/iceberg/" in (d.get("name") or "")
    ]


# ------------------------------------------------- what the real capture says


def test_iceberg_tables_are_named_by_path_not_by_catalog(events):
    """The finding that matters, and the one D6's conclusion did not cover.

    A Hive table over the same thrift connection is `spark://thrift:10000` with
    a `schema.table` name. Iceberg is `file` with a warehouse path — so the
    namespace conclusion recorded in D6 is format-dependent, not adapter-
    dependent as it was written.
    """
    outputs = _iceberg_outputs(events)
    assert outputs, "fixture carries no iceberg datasets"

    for dataset in outputs:
        assert dataset["namespace"] == "file"
        assert dataset["name"].startswith("/warehouse/iceberg/")

    names = {d["name"] for d in outputs}
    assert "/warehouse/iceberg/analytics_marts_marts/stg_customers" in names


def test_spark_declares_the_catalog_identity_in_a_symlink(events):
    """The producer stating what Phase 04 previously had to infer."""
    linked = [
        d for d in _iceberg_outputs(events) if "symlinks" in (d.get("facets") or {})
    ]
    assert linked, "no symlinks facet in the capture"

    identities = catalogs.symlink_identities(linked[0]["facets"])
    assert identities
    first = identities[0]
    # Fully qualified `schema.table`, which is exactly what dbt reports for the
    # same table over thrift -- so the two rows join on a producer's assertion
    # rather than on a leaf-name guess.
    assert "." in first["name"]
    assert first["type"] == "TABLE"
    assert first["namespace"].startswith("http")


def test_the_catalog_facet_makes_the_catalog_discoverable(events):
    """A stack already sending lineage has told us where its catalog is.

    That is the difference between "configure a catalog endpoint" and "we
    already know", and it is why catalogs.build accepts a discovered spec.
    """
    withcat = [
        d for d in _iceberg_outputs(events) if "catalog" in (d.get("facets") or {})
    ]
    assert withcat, "no catalog facet in the capture"

    spec = catalogs.catalog_from_facet(withcat[0]["facets"]["catalog"])
    assert spec is not None
    assert spec["type"] == "rest"
    assert spec["framework"] == "iceberg"
    assert spec["metadata_uri"].startswith("http")

    built = catalogs.build(spec)
    assert isinstance(built, catalogs.RestCatalog)


def test_the_storage_facet_names_the_table_format(events):
    withstorage = [
        d for d in _iceberg_outputs(events) if "storage" in (d.get("facets") or {})
    ]
    assert withstorage
    assert catalogs.storage_layer(withstorage[0]["facets"]) == "iceberg"


def test_hive_and_iceberg_both_appear_for_the_same_model(events):
    """Why the symlink is load-bearing rather than a nicety.

    The capture contains the same three models under two identities. Without the
    declared link they are six entities and the lineage graph is cut in half --
    the D6 failure again, arriving through a different door.
    """
    names = {d["namespace"] for d in _datasets(events)}
    assert "file" in names
    assert "spark://thrift:10000" in names


# ------------------------------------------------------------- catalog clients


class FakeResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = json.dumps(payload).encode()

    def read(self) -> bytes:
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        return None


def test_rest_catalog_resolves_the_current_metadata_location():
    """The whole reason a catalog exists here.

    Iceberg writes a new `NNNNN-<uuid>.metadata.json` on every commit, so a
    configured path goes stale on the next write -- and stale *silently*, which
    on a freshness monitor is indistinguishable from a table that stopped
    updating.
    """
    seen = {}

    def opener(request, timeout=None):
        seen["url"] = request.full_url
        return FakeResponse(
            {
                "metadata-location": "file:///warehouse/iceberg/db/t/metadata/00007-abc.metadata.json",
                "metadata": {"properties": {"format-version": "2"}},
            }
        )

    catalog = catalogs.RestCatalog("http://iceberg-rest:8181", opener=opener)
    ref = catalog.load_table("analytics_marts", "stg_customers")

    assert ref.metadata_location.endswith("00007-abc.metadata.json")
    assert ref.identifier == "analytics_marts.stg_customers"
    assert "/v1/namespaces/analytics_marts/tables/stg_customers" in seen["url"]


def test_rest_catalog_namespaces_are_flattened():
    catalog = catalogs.RestCatalog(
        "http://c:8181",
        opener=lambda r, timeout=None: FakeResponse(
            {"namespaces": [["analytics_marts"], ["raw", "landing"]]}
        ),
    )
    assert catalog.namespaces() == ["analytics_marts", "raw.landing"]


def test_a_catalog_that_cannot_answer_raises_rather_than_returning_nothing():
    """A poll sweep must be able to tell "no such table" from "catalog down".
    Returning None for both would record a missing table as a healthy absence.
    """

    def broken(request, timeout=None):
        raise OSError("connection refused")

    catalog = catalogs.RestCatalog("http://down:8181", opener=broken)
    with pytest.raises(catalogs.CatalogError):
        catalog.namespaces()


class FakeGlue:
    """Shapes from Glue's documented GetTable response."""

    def __init__(self, parameters: dict) -> None:
        self.parameters = parameters

    def get_table(self, DatabaseName: str, Name: str) -> dict:  # noqa: N803
        return {"Table": {"Name": Name, "Parameters": self.parameters}}


def test_glue_reads_the_iceberg_metadata_pointer():
    """Unvalidated against a real account (D9), so the shape is pinned here.

    `metadata_location` is Iceberg's convention rather than a Glue feature,
    which is why it is the same key the REST spec returns.
    """
    glue = catalogs.GlueCatalog(
        client=FakeGlue({"metadata_location": "s3://lake/db/t/metadata/00003-x.metadata.json",
                         "table_type": "ICEBERG"})
    )
    ref = glue.load_table("analytics_marts", "stg_customers")
    assert ref.metadata_location.startswith("s3://")
    assert ref.properties["table_type"] == "ICEBERG"


def test_glue_tolerates_the_hyphenated_spelling():
    """Some tooling writes `metadata-location`. Reading only one spelling would
    return a table with no pointer, which reads as "not an Iceberg table"."""
    glue = catalogs.GlueCatalog(
        client=FakeGlue({"metadata-location": "s3://lake/db/t/metadata/1.metadata.json"})
    )
    assert glue.load_table("db", "t").metadata_location.startswith("s3://")


# ----------------------------------------------------------------- degradation


@pytest.mark.parametrize(
    "facet",
    [None, {}, {"name": "iceberg"}, {"metadataUri": "http://x"}, "not-a-dict"],
)
def test_a_half_populated_catalog_facet_is_declined(facet):
    """A client pointed at nothing is worse than no client: it fails per-table
    during a sweep instead of once, at configuration time."""
    assert catalogs.catalog_from_facet(facet) is None


@pytest.mark.parametrize("facets", [None, {}, {"symlinks": {}}, {"symlinks": "x"}])
def test_missing_symlinks_yield_no_identities(facets):
    assert catalogs.symlink_identities(facets) == []


def test_build_refuses_an_unsupported_catalog_type():
    with pytest.raises(catalogs.CatalogError):
        catalogs.build({"type": "hive", "metadata_uri": "thrift://hms:9083"})


# ------------------------------------------------- catalog-resolved locations


@pytest.mark.parametrize(
    ("given", "expected"),
    [
        ("file:/warehouse/db/t/metadata/0.json", "/warehouse/db/t/metadata/0.json"),
        ("file:///warehouse/db/t/metadata/0.json", "/warehouse/db/t/metadata/0.json"),
        ("/warehouse/db/t/metadata/0.json", "/warehouse/db/t/metadata/0.json"),
    ],
)
def test_a_catalog_returns_uris_not_paths(given, expected):
    """Found by pointing the reader at a real REST catalog.

    It hands back `file:/warehouse/...`, and `Path()` reads that as a *relative*
    filename, so every catalog-resolved read failed with "No such file". A
    configured `path:` is a bare path and a catalog-resolved one never is, so
    both have to work. One slash or three: Iceberg writes `file:/` while Spark's
    own configs use `file:///`.
    """
    from dataspine.sources import _local_path

    assert _local_path(given) == expected


def test_iceberg_metadata_reads_through_the_catalog_uri(tmp_path):
    """The full chain, minus the network: catalog location -> snapshot."""
    import json as _json

    from dataspine import sources

    metadata = tmp_path / "00007-abc.metadata.json"
    metadata.write_text(
        _json.dumps(
            {
                "current-snapshot-id": 42,
                "current-schema-id": 0,
                "schemas": [{"schema-id": 0, "fields": [
                    {"name": "customer_id", "type": "int"},
                    {"name": "segment", "type": "string"},
                ]}],
                "snapshots": [
                    {"snapshot-id": 42, "timestamp-ms": 1755400000000,
                     "summary": {"total-records": "2", "total-files-size": "1422"}}
                ],
            }
        )
    )

    snapshot = sources.read_iceberg_metadata(
        f"file:{metadata}", namespace="iceberg://rest", name="stg_customers"
    )
    assert snapshot is not None
    assert snapshot.row_count == 2
    assert snapshot.columns == {"customer_id": "int", "segment": "string"}


def test_a_catalog_backed_source_needs_a_qualified_dataset():
    """`dataset:` becomes the catalog identifier, so it must name a namespace.
    Guessing a default namespace would poll the wrong table silently."""
    from dataspine import sources

    spec = sources.SourceSpec(
        name="marts", type="iceberg", namespace="iceberg://rest",
        dataset="stg_customers", catalog={"type": "rest", "metadata_uri": "http://c:8181"},
    )
    with pytest.raises(sources.SourceError):
        sources._resolve_via_catalog(spec, catalog=object())


def test_an_iceberg_source_accepts_a_catalog_instead_of_a_path():
    from dataspine import sources

    specs = sources.parse_sources(
        {
            "sources": [
                {
                    "name": "marts",
                    "type": "iceberg",
                    "namespace": "iceberg://rest",
                    "dataset": "analytics_marts.stg_customers",
                    "catalog": {"type": "rest", "metadata_uri": "http://rest:8181"},
                }
            ]
        }
    )
    assert specs[0].catalog["type"] == "rest"
    assert specs[0].path is None


def test_an_iceberg_source_with_neither_path_nor_catalog_is_refused():
    from dataspine import sources

    with pytest.raises(sources.SourceError):
        sources.parse_sources(
            {"sources": [{"name": "m", "type": "iceberg",
                          "namespace": "ns", "dataset": "db.t"}]}
        )
