"""Monitor definitions, and reconciling them from the user's repo.

A monitor is declared in YAML that lives next to the dbt project it protects, so
changing a threshold goes through code review like everything else. `apply`
reconciles those files into the `monitors` table; nothing is ever configured by
clicking.

Two decisions worth stating up front, because both are the kind of thing that
looks arbitrary later:

  **`name` is the identity.** Rename a monitor in YAML and you get a new monitor
  with empty history. That is the honest outcome -- in practice a rename
  accompanies a redefinition, and silently carrying a baseline across a changed
  definition would make the baseline a lie.

  **Removing a monitor from a file disables it; it does not delete it.** Deleting
  would cascade its metric history away, and the most common cause of a monitor
  vanishing from a file is a bad merge rather than a decision. Disabling is
  reversible and visible; deletion is neither. `--prune` exists for when you
  really do mean it.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psycopg

# Kinds that watch a dataset, and kinds that watch a job. The split decides which
# key the YAML must carry (`dataset:` vs `job:`) and how the target resolves.
DATASET_KINDS = ("freshness", "row_count", "schema_drift", "column_stats")
JOB_KINDS = (
    "job_duration", "job_failure_rate", "queue_delay", "spark_spill", "job_retries",
    "cost_per_run",
)
QUERY_KINDS = ("custom_sql",)
KINDS = DATASET_KINDS + JOB_KINDS + QUERY_KINDS

SCHEDULES = ("hourly", "daily", "manual")

# Kinds whose observations are a numeric series a baseline can be learned from.
#
# `schema_drift` is absent because it is deterministic -- there is no number to
# be unusual. `job_failure_rate` is absent because it is already an aggregate
# over a window, and running a detector over an aggregate detects changes in the
# window as readily as changes in the data.
ANOMALY_KINDS = (
    "freshness", "row_count", "job_duration", "queue_delay", "spark_spill", "custom_sql",
    # Spend regression needs no cost-specific detector: "up 4x from last week" is
    # the Phase 03 seasonal baseline over a cost series.
    "cost_per_run",
    # A null rate creeping from 1% to 4% breaches no sane static threshold and is
    # exactly the kind of decay worth catching, so column stats want this most.
    "column_stats",
)

# threshold: the user states the bound. anomaly: the bound is learned.
# Threshold stays the default -- a stated number is auditable and arms instantly,
# and a tool that silently starts making statistical claims is harder to trust.
MODES = ("threshold", "anomaly")

# Config keys lifted from the top level of a YAML monitor into `config`. Flat
# beats nested here: `max_age_minutes: 90` is what someone writes without reading
# documentation, and a `config:` block would be one more thing to get wrong.
CONFIG_KEYS = (
    "max_age_minutes",
    "min",
    "max",
    "max_seconds",
    "max_rate",
    "window_hours",
    "namespace",
    "allow_removed_columns",
    "sensitivity",
    "min_training_points",
    "min_training_days",
    "max_gb",
    "timeout_seconds",
    "column",
    "metric",
)

# Keys that are structure rather than configuration.
RESERVED_KEYS = (
    "name", "kind", "dataset", "job", "query", "schedule", "enabled", "description", "mode",
)


class SpecError(ValueError):
    """A monitor file that cannot be applied. Carries the file and monitor name,
    because "invalid config" without a location is a scavenger hunt."""


@dataclass
class MonitorSpec:
    name: str
    kind: str
    target_kind: str
    target: str
    config: dict[str, Any] = field(default_factory=dict)
    schedule: str = "hourly"
    enabled: bool = True
    source: str | None = None
    mode: str = "threshold"


# --------------------------------------------------------------------- parsing


def parse_spec(raw: Any, *, source: str | None = None) -> list[MonitorSpec]:
    """Validate one parsed YAML document into monitor specs.

    Validation is strict on purpose, which is the opposite of the ingest path's
    fail-open rule -- and the difference is deliberate. An event we cannot parse
    is data already generated that we would otherwise lose, so we keep it. A
    monitor we cannot parse has produced nothing yet, and accepting it half-formed
    means a monitor that silently never fires. The dangerous failure for config is
    quiet acceptance.
    """
    where = f" in {source}" if source else ""
    if not isinstance(raw, dict):
        raise SpecError(f"monitor file{where} must be a mapping with a `monitors:` key")

    entries = raw.get("monitors")
    if entries is None:
        raise SpecError(f"no `monitors:` key{where}")
    if not isinstance(entries, list):
        raise SpecError(f"`monitors:`{where} must be a list")

    specs: list[MonitorSpec] = []
    seen: set[str] = set()
    for index, entry in enumerate(entries):
        spec = _parse_one(entry, index=index, source=source)
        if spec.name in seen:
            raise SpecError(f"duplicate monitor name `{spec.name}`{where}")
        seen.add(spec.name)
        specs.append(spec)
    return specs


def _parse_one(entry: Any, *, index: int, source: str | None) -> MonitorSpec:
    where = f" in {source}" if source else ""
    if not isinstance(entry, dict):
        raise SpecError(f"monitor #{index + 1}{where} is not a mapping")

    name = entry.get("name")
    if not isinstance(name, str) or not name.strip():
        raise SpecError(f"monitor #{index + 1}{where} has no `name`")
    name = name.strip()

    kind = entry.get("kind")
    if kind not in KINDS:
        raise SpecError(
            f"monitor `{name}`{where} has kind {kind!r}; expected one of {', '.join(KINDS)}"
        )

    dataset, job, query = entry.get("dataset"), entry.get("job"), entry.get("query")
    if kind in DATASET_KINDS:
        if not isinstance(dataset, str) or not dataset.strip():
            raise SpecError(f"monitor `{name}`{where} ({kind}) needs a `dataset:`")
        target_kind, target = "dataset", dataset.strip()
    elif kind in QUERY_KINDS:
        if not isinstance(query, str) or not query.strip():
            raise SpecError(f"monitor `{name}`{where} ({kind}) needs a `query:`")
        target_kind, target = "query", query.strip()
    else:
        if not isinstance(job, str) or not job.strip():
            raise SpecError(f"monitor `{name}`{where} ({kind}) needs a `job:`")
        target_kind, target = "job", job.strip()

    schedule = entry.get("schedule", "hourly")
    if schedule not in SCHEDULES:
        raise SpecError(
            f"monitor `{name}`{where} has schedule {schedule!r}; "
            f"expected one of {', '.join(SCHEDULES)}"
        )

    mode = entry.get("mode", "threshold")
    if mode not in MODES:
        raise SpecError(
            f"monitor `{name}`{where} has mode {mode!r}; expected one of {', '.join(MODES)}"
        )
    if mode == "anomaly" and kind not in ANOMALY_KINDS:
        raise SpecError(
            f"monitor `{name}`{where}: `{kind}` has no numeric series to learn from, so "
            f"mode: anomaly is not available. Kinds that support it: {', '.join(ANOMALY_KINDS)}"
        )

    # An unknown key is almost always a typo in a threshold name, and a typo'd
    # threshold means a monitor that never fires -- the failure mode that erodes
    # trust in every alert the tool sends. Better to refuse the file.
    unknown = set(entry) - set(RESERVED_KEYS) - set(CONFIG_KEYS)
    if unknown:
        raise SpecError(
            f"monitor `{name}`{where} has unknown key(s): {', '.join(sorted(unknown))}"
        )

    config = {key: entry[key] for key in CONFIG_KEYS if key in entry}
    _validate_config(name, kind, config, where, mode=mode)

    return MonitorSpec(
        name=name,
        kind=kind,
        target_kind=target_kind,
        target=target,
        config=config,
        schedule=schedule,
        enabled=bool(entry.get("enabled", True)),
        source=source,
        mode=mode,
    )


# What each kind needs before it can decide anything. schema_drift is absent
# because it is deterministic -- a column disappearing is a breach without anyone
# choosing a number, which is why the roadmap puts it first.
REQUIRED_CONFIG = {
    "freshness": ("max_age_minutes",),
    "job_duration": ("max_seconds",),
    "job_failure_rate": ("max_rate",),
    "queue_delay": ("max_seconds",),
    "spark_spill": ("max_gb",),
}

NUMERIC_KEYS = (
    "max_age_minutes", "min", "max", "max_seconds", "max_rate", "window_hours",
    "sensitivity", "min_training_points", "min_training_days",
)


def _validate_config(
    name: str, kind: str, config: dict[str, Any], where: str, *, mode: str = "threshold"
) -> None:
    if kind == "column_stats":
        # Structural, so it is checked in both modes: an anomaly-mode column
        # monitor still has to say which column and which statistic.
        from .profile import METRICS

        if not config.get("column"):
            raise SpecError(f"monitor `{name}`{where} (column_stats) needs a `column:`")
        metric = config.get("metric")
        if metric not in METRICS:
            raise SpecError(
                f"monitor `{name}`{where} has metric {metric!r}; "
                f"expected one of {', '.join(METRICS)}"
            )

    # In anomaly mode the bound is learned, so the threshold keys are not merely
    # optional -- requiring them would be requiring the number the mode exists to
    # avoid inventing.
    if mode == "threshold":
        for key in REQUIRED_CONFIG.get(kind, ()):
            if key not in config:
                raise SpecError(f"monitor `{name}`{where} ({kind}) needs `{key}`")

        # These take either bound, but a monitor with neither can never fire.
        # Accepting it would be accepting a decoration.
        if kind in (
            "row_count", "custom_sql", "job_retries", "column_stats", "cost_per_run"
        ) and not (
            "min" in config or "max" in config
        ):
            raise SpecError(f"monitor `{name}`{where} ({kind}) needs `min`, `max`, or both")

    for key in NUMERIC_KEYS:
        if key in config and not isinstance(config[key], int | float):
            raise SpecError(f"monitor `{name}`{where}: `{key}` must be a number")

    if "min" in config and "max" in config and config["min"] > config["max"]:
        raise SpecError(f"monitor `{name}`{where}: `min` is greater than `max`")


def load_specs(path: Path) -> list[MonitorSpec]:
    """Load every monitor from a file, or from every *.yml/*.yaml under a directory."""
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover - dependency is declared
        raise SpecError("PyYAML is required to read monitor files") from exc

    files = sorted(
        p for p in (path.rglob("*.y*ml") if path.is_dir() else [path]) if p.is_file()
    )
    if not files:
        raise SpecError(f"no monitor files found at {path}")

    specs: list[MonitorSpec] = []
    names: dict[str, str] = {}
    for file in files:
        try:
            raw = yaml.safe_load(file.read_text())
        except Exception as exc:
            raise SpecError(f"{file}: {exc}") from exc
        # A directory of dbt YAML will contain plenty of files that are not ours.
        # Skipping them quietly is right for a directory scan; an explicitly named
        # file with no `monitors:` key is a mistake worth reporting.
        if path.is_dir() and not (isinstance(raw, dict) and "monitors" in raw):
            continue
        for spec in parse_spec(raw, source=str(file)):
            if spec.name in names:
                raise SpecError(
                    f"monitor `{spec.name}` is defined in both {names[spec.name]} and {file}"
                )
            names[spec.name] = str(file)
            specs.append(spec)
    return specs


# ---------------------------------------------------------------- reconciliation


def apply_specs(
    conn: psycopg.Connection, specs: list[MonitorSpec], *, sources: list[str], prune: bool = False
) -> dict[str, list[str]]:
    """Reconcile specs into the monitors table. Returns what changed, by action.

    Scoped to `sources`: applying one file must never disable monitors defined in
    another. Without that, `dataspine apply monitors/freshness.yml` in a repo with
    five monitor files would silently switch the other four off.
    """
    result: dict[str, list[str]] = {
        "created": [], "updated": [], "unchanged": [], "disabled": [], "pruned": []
    }

    for spec in specs:
        existing = conn.execute(
            "select * from monitors where name = %s", (spec.name,)
        ).fetchone()
        conn.execute(
            """
            insert into monitors (name, kind, target_kind, target, config, schedule,
                                  enabled, source, mode)
            values (%(name)s, %(kind)s, %(target_kind)s, %(target)s, %(config)s,
                    %(schedule)s, %(enabled)s, %(source)s, %(mode)s)
            on conflict (name) do update set
                kind        = excluded.kind,
                target_kind = excluded.target_kind,
                target      = excluded.target,
                config      = excluded.config,
                schedule    = excluded.schedule,
                enabled     = excluded.enabled,
                source      = excluded.source,
                mode        = excluded.mode,
                updated_at  = now()
            """,
            {
                "name": spec.name,
                "kind": spec.kind,
                "target_kind": spec.target_kind,
                "target": spec.target,
                "config": json.dumps(spec.config),
                "schedule": spec.schedule,
                "enabled": spec.enabled,
                "source": spec.source,
                "mode": spec.mode,
            },
        )

        if existing is None:
            result["created"].append(spec.name)
        elif _changed(existing, spec):
            result["updated"].append(spec.name)
        else:
            result["unchanged"].append(spec.name)

    # Anything previously applied from these same files, now absent from them.
    live = [s.name for s in specs]
    stale = conn.execute(
        """
        select name from monitors
        where source = any(%(sources)s)
          and not (name = any(%(live)s))
        """,
        {"sources": sources, "live": live},
    ).fetchall()

    for entry in stale:
        if prune:
            conn.execute("delete from monitors where name = %s", (entry["name"],))
            result["pruned"].append(entry["name"])
        else:
            conn.execute(
                "update monitors set enabled = false, updated_at = now() where name = %s",
                (entry["name"],),
            )
            result["disabled"].append(entry["name"])

    return result


def _changed(existing: dict[str, Any], spec: MonitorSpec) -> bool:
    return (
        existing["kind"] != spec.kind
        or existing["target_kind"] != spec.target_kind
        or existing["target"] != spec.target
        or (existing["config"] or {}) != spec.config
        or existing["schedule"] != spec.schedule
        or existing["enabled"] != spec.enabled
        or (existing.get("mode") or "threshold") != spec.mode
    )


# ------------------------------------------------------------------- read side


def list_monitors(
    conn: psycopg.Connection, *, enabled_only: bool = False, schedule: str | None = None
) -> list[dict[str, Any]]:
    where = ["1 = 1"]
    params: dict[str, Any] = {}
    if enabled_only:
        where.append("enabled")
    if schedule:
        where.append("schedule = %(schedule)s")
        params["schedule"] = schedule
    return conn.execute(
        f"""
        select * from monitors
        where {' and '.join(where)}
        order by (last_status = 'breach') desc, name
        """,
        params,
    ).fetchall()


def get_monitor(conn: psycopg.Connection, name: str) -> dict[str, Any] | None:
    return conn.execute("select * from monitors where name = %s", (name,)).fetchone()


def recent_points(
    conn: psycopg.Connection, monitor_id: int, *, limit: int = 200
) -> list[dict[str, Any]]:
    """Metric history for one monitor, newest first."""
    return conn.execute(
        """
        select observed_at, subject, value, context, feedback
        from metric_points
        where monitor_id = %(id)s
        order by observed_at desc
        limit %(limit)s
        """,
        {"id": monitor_id, "limit": limit},
    ).fetchall()


def recent_results(
    conn: psycopg.Connection, monitor_id: int, *, limit: int = 50
) -> list[dict[str, Any]]:
    return conn.execute(
        """
        select * from monitor_results
        where monitor_id = %(id)s
        order by evaluated_at desc
        limit %(limit)s
        """,
        {"id": monitor_id, "limit": limit},
    ).fetchall()


FEEDBACK_VALUES = ("expected", "anomaly")


def set_feedback(
    conn: psycopg.Connection, monitor_id: int, subject: str, value: str | None
) -> bool:
    """Label one observation. Returns whether a point was found.

    The two labels do opposite things to the baseline on purpose -- `expected`
    keeps the point (it was normal), `anomaly` drops it (it was not, and leaving
    it in would widen the band enough to hide a recurrence). Passing None clears
    the label, because a misclick must be undoable without a SQL prompt.
    """
    if value is not None and value not in FEEDBACK_VALUES:
        raise ValueError(f"feedback must be one of {FEEDBACK_VALUES} or None")
    result = conn.execute(
        "update metric_points set feedback = %s where monitor_id = %s and subject = %s",
        (value, monitor_id, subject),
    )
    return result.rowcount > 0


def open_breaches(conn: psycopg.Connection) -> list[dict[str, Any]]:
    """Monitors currently in breach. The "what is broken right now" query."""
    return conn.execute(
        """
        select m.*, r.message, r.value, r.evaluated_at as breached_at
        from monitors m
        join lateral (
            select message, value, evaluated_at
            from monitor_results
            where monitor_id = m.id
            order by evaluated_at desc
            limit 1
        ) r on true
        where m.enabled and m.last_status = 'breach'
        order by r.evaluated_at desc
        """
    ).fetchall()
