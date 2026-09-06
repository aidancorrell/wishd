"""Bulk import of Spark event logs.

This is how a year of history arrives: point it at the directory (or S3 prefix)
that EMR writes event logs to and let it walk. A single unreadable file must
never abort the import -- in a year of logs there will be several, and stopping
on the first one means nobody ever completes a backfill.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import psycopg

from . import s3, spark_metrics, sparklog

log = logging.getLogger("dataspine.backfill")

# EMR/Spark event logs have no consistent extension: they are named after the
# application id, sometimes with .gz or .inprogress. Filter on what we can read
# rather than on a name pattern.
SKIP_SUFFIXES = (".txt", ".md", ".json", ".crc", ".log")


def _sources(directory: str | Path, *, s3_client: Any = None) -> list[str]:
    """Every candidate event log under `directory`, local or on S3.

    Returned as URI strings rather than paths because `source_uri` is what
    `resources.link_applications` regexes the cluster id out of, and on EMR the
    cluster id is only ever in the S3 key — `.../j-2ABCDEF/application_…`.
    Rewriting it to a local path would break that join silently.
    """
    if s3.is_s3_uri(directory):
        return sorted(
            uri
            for uri, _size in s3.list_prefix(str(directory), s3=s3_client)
            if not uri.endswith(SKIP_SUFFIXES)
        )
    return sorted(
        str(path)
        for path in Path(directory).rglob("*")
        if path.is_file() and path.suffix not in SKIP_SUFFIXES
    )


def backfill_directory(
    conn: psycopg.Connection,
    directory: str | Path,
    *,
    relink: bool = True,
    s3_client: Any = None,
) -> dict[str, Any]:
    """Ingest every event log under `directory`. Returns counts.

    `directory` is a local directory or an `s3://` prefix — the latter being
    where EMR puts them, and therefore the case a real backfill starts from.
    """
    stats = {"ingested": 0, "skipped": 0, "failed": 0, "linked": 0}

    for uri in _sources(directory, s3_client=s3_client):
        try:
            summary = sparklog.parse_event_log(uri, s3_client=s3_client)
        except Exception:
            log.exception("could not parse %s", uri)
            stats["failed"] += 1
            continue

        if not summary.app_id:
            # Not an event log, or too truncated to identify. Either way there
            # is nothing to key the metrics on.
            stats["skipped"] += 1
            continue

        run_id = spark_metrics.store(conn, summary, source_uri=uri)
        stats["ingested"] += 1
        if run_id:
            stats["linked"] += 1

    if relink:
        stats["relinked"] = spark_metrics.relink_orphans(conn)
    return stats
