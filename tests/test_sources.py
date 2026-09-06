"""Metadata collection from systems that do not emit OpenLineage.

Everything shipped so far reads the run archive, which only covers tables *our*
pipelines write. The tables upstream of them — loaded by Fivetran, by a vendor
drop, by a team that has never heard of us — are invisible, and those are exactly
the ones whose silent staleness breaks a pipeline at 02:00.

Two rules shape this module:

  **Metadata, never a scan, by default.** Every poller reads catalog metadata:
  `INFORMATION_SCHEMA`, system tables, Iceberg snapshot summaries, the Delta
  transaction log. A monitoring tool that runs `count(*)` over a customer's
  warehouse on a schedule is a monitoring tool with a line item on their bill,
  and it gets removed. Profiling exists, is opt-in, and has an explicit budget.

  **One storage shape.** A poller produces the same observation a run event does,
  so freshness, volume and schema-drift monitors work on a Fivetran-loaded source
  table without knowing anything changed.

**Validation status.** The Postgres poller and both table-format readers are
tested against the real thing here. The Snowflake, Databricks, BigQuery and
Redshift pollers are tested at the query-construction and row-mapping level
against their documented catalog shapes — **none has been run against a real
warehouse**, which is the same honest category as the untested EMR bootstrap.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from dataspine import checks, monitors, sources

NOW = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)


# ------------------------------------------------------------------- snapshots


def test_a_snapshot_becomes_an_observation_a_monitor_can_read(conn):
    """The integration that makes the whole collection layer worth having.

    Nothing here involves a run, a job or an OpenLineage event — and the
    freshness monitor neither knows nor cares.
    """
    sources.store_snapshots(
        conn,
        "postgres",
        [
            sources.TableSnapshot(
                namespace="postgres://warehouse",
                name="raw.stripe_charges",
                last_modified=NOW - timedelta(hours=8),
                row_count=41_000,
            )
        ],
    )

    monitors.apply_specs(
        conn,
        [monitors.MonitorSpec("src_fresh", "freshness", "dataset", "stripe_charges",
                              {"max_age_minutes": 120}, source="t.yml")],
        sources=["t.yml"],
    )
    monitor = monitors.get_monitor(conn, "src_fresh")
    result = checks.evaluate(conn, monitor, now=NOW)

    assert result["status"] == "breach"
    assert "8h00m ago" in result["message"]


def test_snapshots_and_run_writes_coexist_for_one_table(conn):
    """A table can be both written by our pipeline and polled from the warehouse.

    Taking the newest of the two is right: a poller running hourly must not make
    a table look stale between polls when a run wrote it five minutes ago.
    """
    job = conn.execute(
        "insert into jobs (namespace, name, integration) values ('t','j','DBT') returning id"
    ).fetchone()["id"]
    dataset = conn.execute(
        "insert into datasets (namespace, name) values ('warehouse','marts.fct_orders') "
        "returning id"
    ).fetchone()["id"]
    run = conn.execute(
        "insert into runs (run_id, job_id, root_run_id, state, started_at, ended_at) "
        "values (gen_random_uuid(), %s, gen_random_uuid(), 'COMPLETED', %s, %s) returning run_id",
        (job, NOW - timedelta(minutes=5), NOW - timedelta(minutes=5)),
    ).fetchone()["run_id"]
    conn.execute(
        "insert into run_datasets (run_id, dataset_id, direction, row_count) "
        "values (%s, %s, 'OUTPUT', 500)",
        (run, dataset),
    )
    sources.store_snapshots(
        conn, "postgres",
        [sources.TableSnapshot("warehouse", "marts.fct_orders",
                               last_modified=NOW - timedelta(hours=9), row_count=500)],
    )

    monitors.apply_specs(
        conn,
        [monitors.MonitorSpec("f", "freshness", "dataset", "fct_orders",
                              {"max_age_minutes": 60}, source="t.yml")],
        sources=["t.yml"],
    )
    result = checks.evaluate(conn, monitors.get_monitor(conn, "f"), now=NOW)
    assert result["status"] == "ok", "the recent run write should win over the stale poll"


def test_repeated_polling_of_an_unchanged_table_is_one_observation(conn):
    """A table polled hourly but written nightly must not produce 24 points.

    Same rule as the run-derived path: `observed_at` is when the data changed,
    not when we looked. Keying on `last_modified` makes it idempotent for free.
    """
    snapshot = sources.TableSnapshot(
        "warehouse", "raw.events", last_modified=NOW - timedelta(hours=3), row_count=10
    )
    for _ in range(24):
        sources.store_snapshots(conn, "postgres", [snapshot])

    count = conn.execute("select count(*) as n from dataset_snapshots").fetchone()["n"]
    assert count == 1


def test_a_snapshot_without_a_modification_time_is_skipped(conn):
    """Some catalogs report no timestamp for a table. Inventing `now()` would
    make every such table look permanently fresh, which is the dangerous
    direction to be wrong in."""
    sources.store_snapshots(
        conn, "postgres",
        [sources.TableSnapshot("w", "t", last_modified=None, row_count=5)],
    )
    assert conn.execute("select count(*) as n from dataset_snapshots").fetchone()["n"] == 0


def test_polling_registers_the_dataset_so_it_appears_in_the_catalog(conn):
    sources.store_snapshots(
        conn, "postgres",
        [sources.TableSnapshot("warehouse", "raw.new_table", last_modified=NOW, row_count=1)],
    )
    row = conn.execute(
        "select * from datasets where name = 'raw.new_table'"
    ).fetchone()
    assert row is not None
    assert row["namespace"] == "warehouse"


def test_snapshot_schema_feeds_drift_detection(conn):
    """Schema drift on a source table nobody instruments."""
    for columns, at in (
        ({"id": "integer", "email": "text"}, NOW - timedelta(hours=2)),
        ({"id": "integer"}, NOW - timedelta(hours=1)),
    ):
        sources.store_snapshots(
            conn, "postgres",
            [sources.TableSnapshot("w", "raw.users", last_modified=at, columns=columns)],
        )

    monitors.apply_specs(
        conn,
        [monitors.MonitorSpec("drift", "schema_drift", "dataset", "users", {}, source="t.yml")],
        sources=["t.yml"],
    )
    result = checks.evaluate(conn, monitors.get_monitor(conn, "drift"), now=NOW)
    assert result["status"] == "breach"
    assert "removed email" in result["message"]


# -------------------------------------------------------------- SQL pollers


def test_postgres_poller_reads_real_catalog_metadata(conn, database_url):
    """The one SQL poller tested against a live server rather than a fake."""
    conn.execute("create table if not exists poller_demo (id int, label text)")
    conn.execute("insert into poller_demo values (1,'a'),(2,'b')")
    conn.execute("analyze poller_demo")

    source = sources.SqlSource("postgres", dialect="postgres", namespace="pg://test")
    snapshots = source.tables(conn)
    found = {s.name: s for s in snapshots}

    assert any(name.endswith("poller_demo") for name in found)
    demo = next(s for n, s in found.items() if n.endswith("poller_demo"))
    assert demo.columns == {"id": "integer", "label": "text"}


@pytest.mark.parametrize(
    "dialect", ["snowflake", "databricks", "bigquery", "redshift", "postgres"]
)
def test_every_dialect_has_a_metadata_query_that_selects_no_user_data(dialect):
    """The scan-never rule, enforced structurally.

    A poller that reads catalog metadata cannot accidentally become expensive.
    These queries are asserted to touch only catalog objects — if someone adds a
    `select count(*) from {table}` to make row counts exact, this fails.
    """
    query = sources.METADATA_QUERIES[dialect]
    lowered = query.lower()
    assert any(
        catalog in lowered
        for catalog in ("information_schema", "system.", "svv_", "pg_catalog", "pg_class")
    )
    assert "count(*)" not in lowered


def test_dialect_rows_map_to_snapshots():
    """Row mapping for the warehouses we cannot reach from a test.

    Shapes are taken from each vendor's documented catalog columns. This is
    honest about what it proves: the mapping is right *if* the documented shape
    is right. It is not evidence that anyone has run this against Snowflake.
    """
    rows = [
        {
            "table_namespace": "ANALYTICS.PUBLIC",
            "table_name": "FCT_ORDERS",
            "last_modified": NOW,
            "row_count": 1000,
            "size_bytes": 2048,
        }
    ]
    snapshots = sources.rows_to_snapshots(rows, namespace_prefix="snowflake://acct")
    assert snapshots[0].namespace == "snowflake://acct"
    assert snapshots[0].name == "ANALYTICS.PUBLIC.FCT_ORDERS"
    assert snapshots[0].row_count == 1000


# ------------------------------------------------------------- table formats


def test_iceberg_metadata_gives_freshness_and_row_count_without_a_scan(tmp_path):
    """Iceberg writes a snapshot summary on every commit. It already contains the
    row count, so asking the storage layer for one would be paying twice."""
    metadata = {
        "format-version": 2,
        "current-snapshot-id": 3055729675574597004,
        "snapshots": [
            {
                "snapshot-id": 3055729675574597004,
                "timestamp-ms": int(
                    (NOW - timedelta(hours=2)).timestamp() * 1000
                ),
                "summary": {
                    "operation": "append",
                    "total-records": "1189034",
                    "total-files-size": "48210944",
                },
            }
        ],
        "schemas": [
            {
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "order_id", "required": True, "type": "long"},
                    {"id": 2, "name": "total", "required": False, "type": "double"},
                ],
            }
        ],
        "current-schema-id": 0,
    }
    path = tmp_path / "v3.metadata.json"
    path.write_text(json.dumps(metadata))

    snapshot = sources.read_iceberg_metadata(path, namespace="glue://analytics", name="fct_orders")
    assert snapshot.row_count == 1_189_034
    assert snapshot.size_bytes == 48_210_944
    assert snapshot.last_modified == NOW - timedelta(hours=2)
    assert snapshot.columns == {"order_id": "long", "total": "double"}


def test_iceberg_reader_picks_the_current_snapshot_not_the_newest_listed(tmp_path):
    """A rolled-back table lists snapshots newer than the current one. Reading the
    newest would report data that is no longer in the table."""
    current = {
        "snapshot-id": 1,
        "timestamp-ms": int((NOW - timedelta(hours=5)).timestamp() * 1000),
        "summary": {"total-records": "100"},
    }
    orphan = {
        "snapshot-id": 2,
        "timestamp-ms": int(NOW.timestamp() * 1000),
        "summary": {"total-records": "999999"},
    }
    path = tmp_path / "v9.metadata.json"
    path.write_text(json.dumps({"current-snapshot-id": 1, "snapshots": [current, orphan]}))

    snapshot = sources.read_iceberg_metadata(path, namespace="glue://a", name="t")
    assert snapshot.row_count == 100


def test_delta_transaction_log_gives_freshness_and_row_count(tmp_path):
    """Delta's log is one JSON object per line, per commit."""
    log = tmp_path / "_delta_log"
    log.mkdir()
    (log / "00000000000000000000.json").write_text(
        "\n".join(
            [
                json.dumps({"protocol": {"minReaderVersion": 1}}),
                json.dumps({
                    "metaData": {
                        "id": "abc",
                        "schemaString": json.dumps({
                            "type": "struct",
                            "fields": [
                                {"name": "id", "type": "long", "nullable": False},
                                {"name": "name", "type": "string", "nullable": True},
                            ],
                        }),
                    }
                }),
                json.dumps({
                    "add": {
                        "path": "part-0.parquet",
                        "size": 1024,
                        "modificationTime": int((NOW - timedelta(hours=1)).timestamp() * 1000),
                        "stats": json.dumps({"numRecords": 500}),
                    }
                }),
            ]
        )
    )
    (log / "00000000000000000001.json").write_text(
        json.dumps({
            "add": {
                "path": "part-1.parquet",
                "size": 2048,
                "modificationTime": int(NOW.timestamp() * 1000),
                "stats": json.dumps({"numRecords": 300}),
            }
        })
    )

    snapshot = sources.read_delta_log(tmp_path, namespace="s3://lake", name="events")
    assert snapshot.row_count == 800, "records accumulate across commits"
    assert snapshot.size_bytes == 3072
    assert snapshot.last_modified == NOW
    assert snapshot.columns == {"id": "long", "name": "string"}


def test_delta_reader_honours_removals(tmp_path):
    """A compaction rewrites files: the removed ones must stop counting, or the
    row count doubles every time someone runs OPTIMIZE."""
    log = tmp_path / "_delta_log"
    log.mkdir()
    (log / "00000000000000000000.json").write_text(
        json.dumps({
            "add": {"path": "a.parquet", "size": 100, "modificationTime": 1,
                    "stats": json.dumps({"numRecords": 500})}
        })
    )
    (log / "00000000000000000001.json").write_text(
        "\n".join([
            json.dumps({"remove": {"path": "a.parquet"}}),
            json.dumps({
                "add": {"path": "b.parquet", "size": 90, "modificationTime": 2,
                        "stats": json.dumps({"numRecords": 500})}
            }),
        ])
    )

    snapshot = sources.read_delta_log(tmp_path, namespace="s3://lake", name="events")
    assert snapshot.row_count == 500


def test_a_truncated_table_format_file_does_not_abort_the_poll(tmp_path):
    """One corrupt table must not stop a sweep across a thousand of them — the
    same rule the Spark event-log backfill already follows."""
    log = tmp_path / "_delta_log"
    log.mkdir()
    (log / "00000000000000000000.json").write_text('{"add": {"path": "a", "siz')

    snapshot = sources.read_delta_log(tmp_path, namespace="s3://lake", name="events")
    assert snapshot is None or snapshot.row_count in (None, 0)


# ------------------------------------------------------------ source config


def test_sources_are_declared_in_yaml_with_credentials_from_the_environment(monkeypatch):
    """Connection strings are credentials; monitor files are committed.

    The same split as alerting: what to watch goes in the repo, what it takes to
    reach it comes from the environment.
    """
    monkeypatch.setenv("WAREHOUSE_DSN", "postgresql://user:pw@host/db")
    declared = sources.parse_sources(
        {
            "sources": [
                {"name": "warehouse", "type": "postgres",
                 "dsn": "${WAREHOUSE_DSN}", "namespace": "pg://warehouse"},
                {"name": "lake", "type": "delta", "path": "/mnt/lake/events",
                 "namespace": "s3://lake", "dataset": "events"},
            ]
        }
    )
    assert declared[0].dsn == "postgresql://user:pw@host/db"
    assert declared[1].type == "delta"


def test_an_unset_credential_is_refused_rather_than_polled_as_empty(monkeypatch):
    """An empty DSN would connect to something unintended, or fail obscurely
    halfway through a sweep. Better to refuse the file."""
    monkeypatch.delenv("MISSING_DSN", raising=False)
    with pytest.raises(sources.SourceError, match="MISSING_DSN"):
        sources.parse_sources(
            {"sources": [{"name": "w", "type": "postgres", "dsn": "${MISSING_DSN}",
                          "namespace": "pg://w"}]}
        )


def test_an_unknown_source_type_is_refused():
    with pytest.raises(sources.SourceError, match="type"):
        sources.parse_sources(
            {"sources": [{"name": "w", "type": "teradata", "namespace": "x"}]}
        )


def test_polling_a_delta_source_stores_a_snapshot(conn, tmp_path):
    log = tmp_path / "_delta_log"
    log.mkdir()
    (log / "00000000000000000000.json").write_text(
        json.dumps({
            "add": {"path": "a.parquet", "size": 10,
                    "modificationTime": int(NOW.timestamp() * 1000),
                    "stats": json.dumps({"numRecords": 7})}
        })
    )
    source = sources.SourceSpec(
        name="lake", type="delta", namespace="s3://lake",
        path=str(tmp_path), dataset="events",
    )
    written = sources.poll(conn, source)
    assert written == 1

    row = conn.execute(
        "select s.row_count from dataset_snapshots s "
        "join datasets d on d.id = s.dataset_id where d.name = 'events'"
    ).fetchone()
    assert row["row_count"] == 7
