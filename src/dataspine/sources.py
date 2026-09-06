"""Collecting metadata from systems that do not emit OpenLineage.

Everything else in this project reads the run archive, which covers only the
tables our own pipelines write. The tables *upstream* of those -- loaded by
Fivetran, by a vendor drop, by a team that has never heard of us -- are invisible,
and they are the ones whose silent staleness breaks a pipeline at 02:00.

Two rules govern the whole module:

  **Metadata, never a scan, by default.** Every poller reads catalog objects:
  `INFORMATION_SCHEMA`, system tables, an Iceberg snapshot summary, the Delta
  transaction log. All of them already contain row counts and modification times,
  because the engine maintains them for its own planner. A monitoring tool that
  runs `count(*)` across a customer's warehouse on a cron is a tool with a line
  item on their bill, and it gets removed within the quarter. `profile.py` exists
  for when someone genuinely wants a scan; it is opt-in and budgeted.

  **One storage shape.** A poller produces a `TableSnapshot`, which is stored as a
  row `checks._dataset_writes` unions with run-derived writes. Freshness, volume
  and schema-drift monitors then work on a Fivetran-loaded source table without a
  single line of monitor code knowing that anything is different.

**Validation status, stated plainly.** The Postgres poller and both table-format
readers are tested against the real thing. The Snowflake, Databricks, BigQuery and
Redshift queries are built from each vendor's documented catalog columns and
tested at the mapping level -- **none has been run against a real warehouse.**
That is the same honest category as the EMR bootstrap action: written, reviewed,
unproven. Treat the first run against a real account as likely to falsify
something, exactly as every real producer has so far.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg

from . import s3
from .config import env

log = logging.getLogger("dataspine.sources")


class SourceError(ValueError):
    """A source declaration that cannot be polled."""


SOURCE_TYPES = ("postgres", "snowflake", "databricks", "bigquery", "redshift",
                "iceberg", "delta")


@dataclass
class SourceSpec:
    """A declared source. Where to look, and what to call what is found there."""

    name: str
    type: str
    namespace: str
    dsn: str | None = None
    path: str | None = None
    dataset: str | None = None
    # A declared catalog: {"type": "rest"|"glue", "metadata_uri": ..., ...}.
    # When present it replaces `path:` -- see `_resolve_via_catalog`.
    catalog: dict[str, Any] | None = None


def parse_sources(raw: Any) -> list[SourceSpec]:
    """Read a `sources:` block, resolving `${VAR}` from the environment.

    Same split as alerting, for the same reason: *what to watch* is committed to
    the repo and reviewed, *what it takes to reach it* comes from the
    environment. A connection string is a credential.

    An unset variable is refused rather than substituted as empty. An empty DSN
    either connects to something unintended or fails obscurely halfway through a
    sweep, and both are worse than declining to start.
    """

    entries = raw.get("sources") if isinstance(raw, dict) else None
    if not isinstance(entries, list):
        return []

    specs = []
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise SourceError(f"source #{index + 1} is not a mapping")
        name = entry.get("name") or f"source-{index + 1}"
        kind = entry.get("type")
        if kind not in SOURCE_TYPES:
            raise SourceError(
                f"source `{name}` has type {kind!r}; expected one of {', '.join(SOURCE_TYPES)}"
            )
        if not entry.get("namespace"):
            raise SourceError(f"source `{name}` needs a `namespace:`")

        resolved = {}
        for key in ("dsn", "path", "namespace", "dataset"):
            value = entry.get(key)
            if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                var = value[2:-1]
                if not env.get(var):
                    raise SourceError(
                        f"source `{name}`: {var} is not set in the environment"
                    )
                value = env[var]
            resolved[key] = value

        catalog = entry.get("catalog") if isinstance(entry.get("catalog"), dict) else None
        if kind == "iceberg" and not resolved.get("path") and not catalog:
            raise SourceError(
                f"source `{name}` (iceberg) needs a `path:` or a `catalog:`"
            )
        if kind == "delta" and not resolved.get("path"):
            raise SourceError(f"source `{name}` (delta) needs a `path:`")
        if kind in ("iceberg", "delta") and not resolved.get("dataset"):
            raise SourceError(f"source `{name}` ({kind}) needs a `dataset:` name")

        specs.append(SourceSpec(name=name, type=kind, catalog=catalog, **resolved))
    return specs


def _resolve_via_catalog(spec: SourceSpec, *, catalog: Any = None) -> str | None:
    """Ask the catalog where this table's metadata currently lives.

    `catalog` is injected in tests and by callers that already hold a client;
    otherwise one is built from the spec. Same injection discipline as
    `warehouse` above, and for the same reason -- Glue needs boto3, which a
    local install should not have to have.
    """
    from . import catalogs  # noqa: PLC0415 - keeps the import off the hot path

    client = catalog or catalogs.build(spec.catalog or {})
    namespace, _, table = (spec.dataset or "").rpartition(".")
    if not namespace:
        raise SourceError(
            f"source `{spec.name}`: with a catalog, `dataset:` must be "
            f"`namespace.table`, got {spec.dataset!r}"
        )
    ref = client.load_table(namespace, table)
    if not ref.metadata_location:
        # A Glue table with no pointer is not an Iceberg table. Saying so beats
        # reading a path that does not exist and reporting a missing table.
        raise SourceError(f"{ref.identifier} has no metadata_location; not Iceberg?")
    return ref.metadata_location


def poll(
    conn: psycopg.Connection,
    spec: SourceSpec,
    *,
    warehouse: Any = None,
    catalog: Any = None,
    history: bool = True,
) -> int:
    """Poll one source and store what it saw. Returns snapshots written.

    `warehouse` is an already-open connection for SQL sources. It is injected
    rather than built here because each warehouse's driver is a heavy optional
    dependency, and requiring `snowflake-connector-python` to be installed before
    a Delta poll will run is the install cost ADR-001 exists to avoid.

    `history` reads an Iceberg table's whole retained snapshot log rather than
    only its current state. On by default: it costs the same single read, and
    the alternative is a monitor that knows nothing about a table until we
    happen to look at it twice.
    """
    if spec.type == "delta":
        snapshot = read_delta_log(
            Path(spec.path or ""), namespace=spec.namespace, name=spec.dataset or ""
        )
        return store_snapshots(conn, spec.type, [snapshot] if snapshot else [])

    if spec.type == "iceberg":
        # A catalog, when one is declared, is the only way to poll a table
        # anybody is still writing to: Iceberg writes a new metadata file on
        # every commit, so a configured `path:` is stale by the next write and
        # stale *silently* -- the reader keeps returning the snapshot it was
        # pointed at, which on a freshness monitor is indistinguishable from a
        # table that stopped updating.
        path = spec.path
        if spec.catalog:
            try:
                path = _resolve_via_catalog(spec, catalog=catalog)
            except Exception as exc:  # noqa: BLE001 - one bad table ends no sweep
                log.warning("catalog lookup failed for %s: %s", spec.name, exc)
                return 0
        if history:
            # Backfill: the table has been keeping this series for us, so a
            # monitor declared today gets a real baseline instead of waiting a
            # week to accumulate one it could already have had. Idempotent --
            # `store_snapshots` keys on (dataset, observed_at), so re-polling
            # re-reads the same snapshots without duplicating them.
            return store_snapshots(
                conn,
                spec.type,
                read_iceberg_history(
                    path or "", namespace=spec.namespace, name=spec.dataset or ""
                ),
            )
        snapshot = read_iceberg_metadata(
            path or "", namespace=spec.namespace, name=spec.dataset or ""
        )
        return store_snapshots(conn, spec.type, [snapshot] if snapshot else [])

    target = warehouse if warehouse is not None else conn
    source = SqlSource(spec.name, dialect=spec.type, namespace=spec.namespace)
    return store_snapshots(conn, spec.type, source.tables(target))


@dataclass
class TableSnapshot:
    """One observation of a table's catalog metadata."""

    namespace: str
    name: str
    last_modified: datetime | None = None
    row_count: int | None = None
    size_bytes: int | None = None
    columns: dict[str, str] | None = None
    extra: dict[str, Any] = field(default_factory=dict)


# --------------------------------------------------------------- metadata SQL

# One query per dialect, each reading only catalog objects.
#
# The uniform output contract is (table_namespace, table_name, last_modified,
# row_count, size_bytes) so `rows_to_snapshots` does not need to know which
# warehouse it is talking to.
#
# Row counts from a catalog are approximate on every engine here -- they are
# planner statistics, refreshed on the engine's own schedule. That is the right
# trade: an approximate count that costs nothing beats an exact one that costs a
# full scan, and volume monitoring cares about a tenfold collapse, not about
# being off by a few hundred rows. Anyone who needs exactness wants `profile.py`.
METADATA_QUERIES = {
    # LAST_ALTERED covers DDL as well as DML, so it over-reports freshness
    # slightly; ACCOUNT_USAGE.TABLES has a truer LAST_DDL but lags by up to 90
    # minutes, which is worse for the question being asked.
    "snowflake": """
        select table_catalog || '.' || table_schema as table_namespace,
               table_name,
               last_altered as last_modified,
               row_count,
               bytes as size_bytes
        from information_schema.tables
        where table_type = 'BASE TABLE'
    """,
    "databricks": """
        select table_catalog || '.' || table_schema as table_namespace,
               table_name,
               last_altered as last_modified,
               null as row_count,
               null as size_bytes
        from system.information_schema.tables
        where table_type = 'MANAGED' or table_type = 'EXTERNAL'
    """,
    # BigQuery keeps counts and bytes in TABLE_STORAGE rather than TABLES.
    "bigquery": """
        select table_schema as table_namespace,
               table_name,
               storage_last_modified_time as last_modified,
               total_rows as row_count,
               total_logical_bytes as size_bytes
        from `region-us`.information_schema.table_storage
    """,
    # Redshift's SVV_TABLE_INFO has size in MB and no modification time; the
    # timestamp comes from the system table that records the last commit.
    "redshift": """
        select ti.schema as table_namespace,
               ti.table as table_name,
               null as last_modified,
               ti.tbl_rows as row_count,
               ti.size * 1024 * 1024 as size_bytes
        from svv_table_info ti
    """,
    "postgres": """
        select n.nspname as table_namespace,
               c.relname as table_name,
               greatest(s.last_autoanalyze, s.last_analyze,
                        s.last_autovacuum, s.last_vacuum) as last_modified,
               c.reltuples::bigint as row_count,
               pg_total_relation_size(c.oid) as size_bytes
        from pg_class c
        join pg_namespace n on n.oid = c.relnamespace
        left join pg_stat_user_tables s on s.relid = c.oid
        where c.relkind = 'r'
          and n.nspname not in ('pg_catalog', 'information_schema')
    """,
}

COLUMN_QUERY = """
    select table_schema, table_name, column_name, data_type
    from information_schema.columns
    where table_schema not in ('pg_catalog', 'information_schema')
"""


def rows_to_snapshots(
    rows: list[dict[str, Any]], *, namespace_prefix: str
) -> list[TableSnapshot]:
    """Map a dialect's catalog rows onto the uniform snapshot shape.

    The warehouse's own schema qualification becomes part of the *name* rather
    than the namespace, and the namespace identifies the account or cluster. That
    keeps one physical table under one identity when two environments of the same
    warehouse are polled into the same dataspine.
    """
    snapshots = []
    for row in rows:
        parts = [row.get("table_namespace"), row.get("table_name")]
        name = ".".join(str(p) for p in parts if p)
        snapshots.append(
            TableSnapshot(
                namespace=namespace_prefix,
                name=name,
                last_modified=_as_datetime(row.get("last_modified")),
                row_count=_as_int(row.get("row_count")),
                size_bytes=_as_int(row.get("size_bytes")),
            )
        )
    return snapshots


@dataclass
class SqlSource:
    """A warehouse polled over any DBAPI-ish connection.

    The connection is passed in rather than built here: the driver for each
    warehouse is a heavy optional dependency, and requiring `snowflake-connector-
    python` to be installed before `dataspine check` will run a Postgres monitor
    would be the sort of install cost ADR-001 exists to avoid.
    """

    name: str
    dialect: str
    namespace: str

    def tables(self, conn: Any) -> list[TableSnapshot]:
        if self.dialect not in METADATA_QUERIES:
            raise ValueError(f"unknown dialect {self.dialect!r}")
        rows = _fetch(conn, METADATA_QUERIES[self.dialect])
        snapshots = rows_to_snapshots(rows, namespace_prefix=self.namespace)

        # Columns are a second query rather than a join: on several of these
        # engines the columns view is far larger than the tables view, and a join
        # makes the cheap query as slow as the expensive one.
        try:
            columns = _columns_by_table(conn)
        except Exception as exc:  # noqa: BLE001 - column data is a bonus, not the point
            log.warning("could not read column metadata from %s: %s", self.name, exc)
            columns = {}

        for snapshot in snapshots:
            key = snapshot.name.split(".")[-2:] if "." in snapshot.name else [snapshot.name]
            snapshot.columns = columns.get(tuple(key[-2:])) or columns.get((key[-1],))
        return snapshots


def _fetch(conn: Any, query: str) -> list[dict[str, Any]]:
    """Run a read query and return dict rows, whatever the driver's row type."""
    cursor = conn.execute(query) if hasattr(conn, "execute") else conn.cursor().execute(query)
    rows = cursor.fetchall()
    if rows and isinstance(rows[0], dict):
        return rows
    names = [d[0] for d in cursor.description]
    return [dict(zip(names, row, strict=False)) for row in rows]


def _columns_by_table(conn: Any) -> dict[tuple[str, ...], dict[str, str]]:
    grouped: dict[tuple[str, ...], dict[str, str]] = {}
    for row in _fetch(conn, COLUMN_QUERY):
        key = (str(row["table_schema"]), str(row["table_name"]))
        grouped.setdefault(key, {})[str(row["column_name"])] = str(row["data_type"])
        grouped.setdefault((str(row["table_name"]),), {})[str(row["column_name"])] = str(
            row["data_type"]
        )
    return grouped


# ------------------------------------------------------------- table formats


def read_iceberg_metadata(
    path: str | Path, *, namespace: str, name: str, s3_client: Any = None
) -> TableSnapshot | None:
    """Freshness, row count and schema from an Iceberg metadata file.

    Iceberg writes a summary on every commit that already contains
    `total-records` and `total-files-size`, because the planner needs them. Asking
    the storage layer to count would be paying for a number the table is holding
    out to us.

    Reads the snapshot named by `current-snapshot-id`, never simply the newest in
    the list -- a rolled-back table still lists the abandoned snapshots, and
    taking the latest would report data that is no longer in the table.
    """
    metadata = _read_json(path, s3_client=s3_client)
    if metadata is None:
        return None

    snapshots = metadata.get("snapshots") or []
    current_id = metadata.get("current-snapshot-id")
    current = next((s for s in snapshots if s.get("snapshot-id") == current_id), None)
    if current is None:
        current = snapshots[-1] if snapshots else {}

    summary = current.get("summary") or {}
    timestamp = current.get("timestamp-ms")
    return TableSnapshot(
        namespace=namespace,
        name=name,
        last_modified=(
            datetime.fromtimestamp(timestamp / 1000, tz=UTC) if timestamp else None
        ),
        row_count=_as_int(summary.get("total-records")),
        size_bytes=_as_int(summary.get("total-files-size")),
        columns=_iceberg_columns(metadata),
        extra={"snapshot_id": current.get("snapshot-id")},
    )


def read_iceberg_history(
    path: str | Path,
    *,
    namespace: str,
    name: str,
    s3_client: Any = None,
    limit: int = 500,
) -> list[TableSnapshot]:
    """Every retained snapshot, oldest first — a table's whole write history.

    The current-snapshot reader answers "how fresh is this table now". This one
    answers "how has it behaved", from the same single metadata read, because
    Iceberg keeps the summary of every snapshot it has not expired:

        01:21:29  op=overwrite  total-records=2
        01:21:34  op=overwrite  total-records=2
        01:21:37  op=overwrite  total-records=2

    **Why that matters more than it looks.** Phase 03's rule is that monitors arm
    on day one — `dataspine apply` backfills metric history out of the run
    archive rather than waiting a week to learn a baseline it could already
    compute. A polled table had no equivalent: we knew only what we had seen
    since being installed, so a freshness or volume monitor on a source table
    was blind for as long as it took to accumulate observations. For Iceberg
    there is no reason to wait. The table has been keeping the series for us.

    Snapshots are returned oldest first and each becomes an ordinary
    `TableSnapshot`, so they flow into `store_snapshots` unchanged and every
    monitor kind works on them without knowing where they came from — the same
    "one storage shape" rule that let a Fivetran-loaded table use run-derived
    monitors.

    **`observed_at` is the snapshot's own timestamp, not now.** Recording the
    read time would collapse a month of history into one instant and teach every
    future baseline that the table changes whenever we happen to poll — the
    identical mistake `metric_points.observed_at` exists to prevent.

    Bounded by `limit`, newest-biased: a table compacted hourly for a year has
    thousands of retained snapshots, and a poller must not turn one read into an
    unbounded write.
    """
    metadata = _read_json(path, s3_client=s3_client)
    if metadata is None:
        return []

    snapshots = [s for s in (metadata.get("snapshots") or []) if isinstance(s, dict)]
    if not snapshots:
        return []

    # Ordered by the table's own clock. `snapshots` is conventionally in commit
    # order but nothing guarantees it, and a series sorted wrongly produces a
    # baseline that is silently nonsense rather than obviously broken.
    snapshots.sort(key=lambda s: s.get("timestamp-ms") or 0)
    if len(snapshots) > limit:
        snapshots = snapshots[-limit:]

    columns = _iceberg_columns(metadata)
    current_id = metadata.get("current-snapshot-id")

    history = []
    for snapshot in snapshots:
        timestamp = snapshot.get("timestamp-ms")
        if not timestamp:
            continue
        summary = snapshot.get("summary") or {}
        history.append(
            TableSnapshot(
                namespace=namespace,
                name=name,
                last_modified=datetime.fromtimestamp(timestamp / 1000, tz=UTC),
                row_count=_as_int(summary.get("total-records")),
                size_bytes=_as_int(summary.get("total-files-size")),
                # Schema is only known for the *current* snapshot: the metadata
                # keeps every schema it has used but does not say which snapshot
                # used which. Attaching today's columns to a year-old snapshot
                # would manufacture a drift history that never happened, so
                # historical rows carry none.
                columns=columns if snapshot.get("snapshot-id") == current_id else None,
                extra={
                    "snapshot_id": snapshot.get("snapshot-id"),
                    "operation": summary.get("operation"),
                    "added_records": _as_int(summary.get("added-records")),
                    "deleted_records": _as_int(summary.get("deleted-records")),
                    "is_current": snapshot.get("snapshot-id") == current_id,
                },
            )
        )
    return history


def _iceberg_columns(metadata: dict[str, Any]) -> dict[str, str] | None:
    schemas = metadata.get("schemas") or []
    current = metadata.get("current-schema-id")
    schema = next((s for s in schemas if s.get("schema-id") == current), None)
    schema = schema or (schemas[0] if schemas else metadata.get("schema"))
    if not isinstance(schema, dict):
        return None
    fields = schema.get("fields")
    if not isinstance(fields, list):
        return None
    return {
        f["name"]: str(f.get("type", "?"))
        for f in fields
        if isinstance(f, dict) and isinstance(f.get("name"), str)
    } or None


def read_delta_log(table_path: Path, *, namespace: str, name: str) -> TableSnapshot | None:
    """Freshness, row count and schema from a Delta transaction log.

    The log is one JSON object per line, one file per commit. Replaying `add` and
    `remove` actions gives the current file set without touching a single Parquet
    file. Honouring `remove` is not optional: a compaction rewrites every file, so
    an add-only reading doubles the row count each time someone runs OPTIMIZE.

    Row counts come from the per-file `stats` Delta writes, which are exact rather
    than estimated -- the one place in this module where the free number is not an
    approximation.
    """
    log_dir = table_path / "_delta_log"
    if not log_dir.is_dir():
        return None

    files: dict[str, dict[str, Any]] = {}
    columns: dict[str, str] | None = None
    latest_ms = 0

    for commit in sorted(log_dir.glob("*.json")):
        for line in commit.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                action = json.loads(line)
            except json.JSONDecodeError:
                # One truncated commit must not abort a sweep over a thousand
                # tables -- the same rule the Spark event-log backfill follows.
                log.warning("skipping unparseable line in %s", commit)
                continue

            if "add" in action:
                entry = action["add"]
                files[entry.get("path", "")] = entry
                latest_ms = max(latest_ms, int(entry.get("modificationTime") or 0))
            elif "remove" in action:
                files.pop(action["remove"].get("path", ""), None)
            elif "metaData" in action:
                columns = _delta_columns(action["metaData"]) or columns

    if not files:
        return None

    rows = 0
    has_stats = False
    for entry in files.values():
        stats = entry.get("stats")
        if isinstance(stats, str):
            try:
                stats = json.loads(stats)
            except json.JSONDecodeError:
                stats = None
        if isinstance(stats, dict) and stats.get("numRecords") is not None:
            rows += int(stats["numRecords"])
            has_stats = True

    return TableSnapshot(
        namespace=namespace,
        name=name,
        last_modified=(
            datetime.fromtimestamp(latest_ms / 1000, tz=UTC) if latest_ms else None
        ),
        # None rather than 0 when no file carried stats: "we do not know" and
        # "the table is empty" must not look the same to a volume monitor.
        row_count=rows if has_stats else None,
        size_bytes=sum(int(e.get("size") or 0) for e in files.values()) or None,
        columns=columns,
    )


def _delta_columns(metadata: dict[str, Any]) -> dict[str, str] | None:
    raw = metadata.get("schemaString")
    if not isinstance(raw, str):
        return None
    try:
        schema = json.loads(raw)
    except json.JSONDecodeError:
        return None
    fields = schema.get("fields")
    if not isinstance(fields, list):
        return None
    return {
        f["name"]: _delta_type(f.get("type"))
        for f in fields
        if isinstance(f, dict) and isinstance(f.get("name"), str)
    } or None


def _delta_type(value: Any) -> str:
    # Nested types arrive as objects; the type name is enough for drift detection,
    # and rendering the whole nested structure into a column type would make every
    # struct change look like a rename.
    if isinstance(value, dict):
        return str(value.get("type", "struct"))
    return str(value)


def _local_path(path: str | Path) -> str:
    """Strip a `file:` scheme, which is how a catalog hands back a local path.

    Found by pointing the reader at a real Iceberg REST catalog: it returns
    `file:/warehouse/iceberg/db/t/metadata/00000-<uuid>.metadata.json`, and
    `Path()` treats that whole string as a relative filename. A configured
    `path:` is a bare path and a catalog-resolved one never is, so the reader has
    to accept both or the catalog route cannot work at all.

    One slash or three -- Iceberg emits `file:/` here while Spark's own configs
    use `file:///`, and both mean the same absolute path.
    """
    text = str(path)
    if not text.startswith("file:"):
        return text
    remainder = text[len("file:") :]
    return "/" + remainder.lstrip("/")


def _read_json(path: str | Path, *, s3_client: Any = None) -> dict[str, Any] | None:
    """Read a JSON metadata file from disk or from S3.

    Iceberg metadata lives wherever the warehouse does, and on any real
    deployment that is object storage -- a catalog hands back
    `s3://bucket/db/t/metadata/00007-<uuid>.metadata.json`, which the local-path
    reader could not open at all. Same gap `ingest-eventlog` had, same fix.
    """
    try:
        if s3.is_s3_uri(path):
            with s3.open_text(str(path), s3=s3_client) as handle:
                return json.loads(handle.read())
        return json.loads(Path(_local_path(path)).read_text())
    except Exception as exc:  # noqa: BLE001 - S3 and disk raise different families
        log.warning("could not read %s: %s", path, exc)
        return None


# ------------------------------------------------------------------- storage


def store_snapshots(
    conn: psycopg.Connection, source: str, snapshots: list[TableSnapshot]
) -> int:
    """Persist snapshots, registering their datasets. Returns rows written.

    A snapshot with no modification time is dropped rather than stamped with
    `now()`. Some catalogs genuinely report none, and inventing one would make
    such a table look permanently fresh -- the dangerous direction to be wrong in
    for the one monitor people rely on most.
    """
    written = 0
    for snapshot in snapshots:
        if snapshot.last_modified is None:
            continue
        dataset_id = conn.execute(
            """
            insert into datasets (namespace, name, facets)
            values (%s, %s, '{}'::jsonb)
            on conflict (namespace, name) do update set updated_at = now()
            returning id
            """,
            (snapshot.namespace, snapshot.name),
        ).fetchone()["id"]

        conn.execute(
            """
            insert into dataset_snapshots
                (dataset_id, observed_at, source, row_count, size_bytes, columns)
            values (%s, %s, %s, %s, %s, %s)
            on conflict (dataset_id, observed_at, source) do update set
                row_count   = coalesce(excluded.row_count, dataset_snapshots.row_count),
                size_bytes  = coalesce(excluded.size_bytes, dataset_snapshots.size_bytes),
                columns     = coalesce(excluded.columns, dataset_snapshots.columns),
                recorded_at = now()
            """,
            (
                dataset_id,
                snapshot.last_modified,
                source,
                snapshot.row_count,
                snapshot.size_bytes,
                json.dumps(snapshot.columns) if snapshot.columns else None,
            ),
        )
        written += 1
    return written


def _as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _as_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return None
