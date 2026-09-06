"""Cluster inventory: the hardware that ran the work.

Cost attribution needs two numbers. The first — how much compute an application
actually held — comes free from the Spark event log, because
`SparkListenerExecutorAdded` and `...Removed` carry timestamps and core counts.
That is ADR-004 paying off a second time: read what Spark already writes.

The second is what the hardware *cost*, and no event log knows that. Which
instance types, how many, Spot or On-Demand — that needs an AWS call, and it is
the only part of Phase 05 that does. This module holds it.

D4 deferred this out of Phase 02 saying it exists to price resource-seconds, and
building it before the cost model meant guessing at the shape the cost model
would need. This is that cost model's first task, exactly as the deferral said.

**Validation status.** Application linking and the storage layer are tested
against real captured event logs. The EMR mapper is built from boto3's documented
`describe_cluster` / `list_instance_groups` / `list_instance_fleets` response
shapes and has **never been run against a real cluster** — the same honest
category as the bootstrap action (D5) and the warehouse pollers (D9).
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import psycopg

log = logging.getLogger("dataspine.resources")

# EMR cluster ids look like `j-2ABCDEFGHIJKL`. Event-log paths on S3 are
# conventionally `.../<cluster-id>/<application-id>`, which is how an application
# finds its cluster without an extra API call per application.
CLUSTER_ID_PATTERN = re.compile(r"(j-[A-Z0-9]{4,})")


@dataclass
class ClusterSpec:
    cluster_id: str
    name: str | None = None
    platform: str = "emr"
    started_at: datetime | None = None
    ended_at: datetime | None = None
    tags: dict[str, str] = field(default_factory=dict)
    instance_groups: list[dict[str, Any]] = field(default_factory=list)


# --------------------------------------------------------------------- storage


def store_cluster(conn: psycopg.Connection, spec: ClusterSpec) -> str:
    """Upsert a cluster and replace its instance groups.

    Groups are replaced wholesale because a live cluster scales between polls,
    and a sync that only added rows would accumulate a false history of capacity
    that never existed simultaneously.
    """
    conn.execute(
        """
        insert into clusters (cluster_id, platform, name, started_at, ended_at, tags, synced_at)
        values (%s, %s, %s, %s, %s, %s, now())
        on conflict (cluster_id) do update set
            platform   = excluded.platform,
            name       = excluded.name,
            started_at = coalesce(excluded.started_at, clusters.started_at),
            ended_at   = excluded.ended_at,
            tags       = excluded.tags,
            synced_at  = now()
        """,
        (
            spec.cluster_id,
            spec.platform,
            spec.name,
            spec.started_at,
            spec.ended_at,
            json.dumps(spec.tags or {}),
        ),
    )

    conn.execute(
        "delete from cluster_instance_groups where cluster_id = %s", (spec.cluster_id,)
    )
    for group in spec.instance_groups:
        conn.execute(
            """
            insert into cluster_instance_groups
                (cluster_id, role, instance_type, market, count)
            values (%s, %s, %s, %s, %s)
            on conflict (cluster_id, role, instance_type, market)
            do update set count = excluded.count
            """,
            (
                spec.cluster_id,
                group.get("role") or "CORE",
                group.get("instance_type") or "unknown",
                group.get("market") or "ON_DEMAND",
                int(group.get("count") or 0),
            ),
        )
    return spec.cluster_id


def get_cluster(conn: psycopg.Connection, cluster_id: str) -> dict[str, Any] | None:
    cluster = conn.execute(
        "select * from clusters where cluster_id = %s", (cluster_id,)
    ).fetchone()
    if cluster is None:
        return None
    cluster["instance_groups"] = conn.execute(
        """
        select role, instance_type, market, count
        from cluster_instance_groups
        where cluster_id = %s
        order by role, instance_type
        """,
        (cluster_id,),
    ).fetchall()
    return cluster


def list_clusters(conn: psycopg.Connection, *, limit: int = 200) -> list[dict[str, Any]]:
    return conn.execute(
        """
        select c.*,
               (select count(*) from spark_apps s where s.cluster_id = c.cluster_id)
                   as application_count,
               (select coalesce(sum((s.metrics ->> 'core_seconds')::numeric), 0)
                  from spark_apps s where s.cluster_id = c.cluster_id) as core_seconds
        from clusters c
        order by c.started_at desc nulls last
        limit %s
        """,
        (limit,),
    ).fetchall()


# ------------------------------------------------------------------- linking


def link_applications(conn: psycopg.Connection) -> int:
    """Attach Spark applications to the cluster that ran them. Returns links made.

    Matched on the cluster id in the event-log path *and* on the application
    falling inside the cluster's lifetime. Both are needed: EMR cluster ids are
    unique but a backfill routinely imports logs from clusters long gone, and a
    path alone would attribute a month-old run's cost to whatever is running now.
    """
    rows = conn.execute(
        """
        select app_id, source_uri, started_at, ended_at
        from spark_apps
        where cluster_id is null and source_uri is not null
        """
    ).fetchall()

    linked = 0
    for row in rows:
        match = CLUSTER_ID_PATTERN.search(row["source_uri"] or "")
        if not match:
            continue
        moment = row["started_at"] or row["ended_at"]
        updated = conn.execute(
            """
            update spark_apps s
            set cluster_id = c.cluster_id, updated_at = now()
            from clusters c
            where s.app_id = %(app_id)s
              and c.cluster_id = %(cluster_id)s
              and (%(moment)s::timestamptz is null
                   or (c.started_at is null or %(moment)s >= c.started_at)
                   and (c.ended_at is null or %(moment)s <= c.ended_at))
            """,
            {
                "app_id": row["app_id"],
                "cluster_id": match.group(1),
                "moment": moment,
            },
        )
        linked += updated.rowcount
    return linked


# ---------------------------------------------------------------- EMR mapping


def cluster_from_emr(
    described: dict[str, Any], groups: dict[str, Any] | None = None
) -> ClusterSpec:
    """Map boto3 EMR responses onto a ClusterSpec.

    Handles both capacity models. A cluster uses instance *groups* or instance
    *fleets*, never both, and fleet-based clusters are the common shape for the
    Spot-heavy analytics workloads this project targets — so supporting only
    groups would miss the clusters most worth costing.
    """
    cluster = described.get("Cluster") or described
    timeline = ((cluster.get("Status") or {}).get("Timeline")) or {}

    tags = {
        str(tag.get("Key")): str(tag.get("Value"))
        for tag in cluster.get("Tags") or []
        if tag.get("Key")
    }

    return ClusterSpec(
        cluster_id=str(cluster.get("Id")),
        name=cluster.get("Name"),
        platform="emr",
        started_at=_as_datetime(timeline.get("CreationDateTime")),
        # Absent while the cluster is alive, and left as None on purpose.
        ended_at=_as_datetime(timeline.get("EndDateTime")),
        tags=tags,
        instance_groups=_instance_groups(groups or {}),
    )


def _instance_groups(payload: dict[str, Any]) -> list[dict[str, Any]]:
    groups = []
    for group in payload.get("InstanceGroups") or []:
        groups.append(
            {
                "role": group.get("InstanceGroupType") or "CORE",
                "instance_type": group.get("InstanceType") or "unknown",
                "market": group.get("Market") or "ON_DEMAND",
                "count": int(
                    group.get("RunningInstanceCount")
                    or group.get("RequestedInstanceCount")
                    or 0
                ),
            }
        )

    for fleet in payload.get("InstanceFleets") or []:
        specs = fleet.get("InstanceTypeSpecifications") or [{}]
        # A fleet may be provisioned from several instance types at once. The
        # first is recorded as representative rather than fabricating a split we
        # were not told: the cost report prices what actually ran, and this
        # field exists to explain shape.
        instance_type = specs[0].get("InstanceType") or "mixed"
        role = fleet.get("InstanceFleetType") or "CORE"
        for market, key in (
            ("SPOT", "ProvisionedSpotCapacity"),
            ("ON_DEMAND", "ProvisionedOnDemandCapacity"),
        ):
            capacity = int(fleet.get(key) or 0)
            if capacity:
                groups.append(
                    {
                        "role": role,
                        "instance_type": instance_type,
                        "market": market,
                        "count": capacity,
                    }
                )
    return groups


def sync_emr(conn: psycopg.Connection, cluster_ids: list[str], *, client: Any) -> int:
    """Fetch and store EMR clusters. `client` is an injected boto3 EMR client.

    Injected rather than constructed here for the same reason the warehouse
    pollers take a connection: boto3 is a heavy optional dependency, and needing
    it installed before `dataspine check` will evaluate a freshness monitor is
    the install cost ADR-001 exists to avoid.
    """
    stored = 0
    for cluster_id in cluster_ids:
        try:
            described = client.describe_cluster(ClusterId=cluster_id)
            groups = _fetch_capacity(client, cluster_id)
            store_cluster(conn, cluster_from_emr(described, groups))
            stored += 1
        except Exception as exc:  # noqa: BLE001 - one bad cluster must not end the sync
            log.warning("could not sync cluster %s: %s", cluster_id, exc)
    return stored


def _fetch_capacity(client: Any, cluster_id: str) -> dict[str, Any]:
    """Instance groups if the cluster has them, otherwise fleets.

    EMR raises rather than returning an empty list when you ask for the wrong
    one, so this tries and falls back instead of branching on a flag we would
    have to fetch first.
    """
    try:
        return client.list_instance_groups(ClusterId=cluster_id)
    except Exception:
        try:
            return client.list_instance_fleets(ClusterId=cluster_id)
        except Exception as exc:  # noqa: BLE001
            log.warning("no capacity information for %s: %s", cluster_id, exc)
            return {}


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
