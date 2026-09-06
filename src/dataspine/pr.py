"""Blast radius on a pull request, before the merge rather than after it.

Everything else in this project reports on what already happened. This is the one
surface where the answer arrives in time to change the decision: a dbt author
opens a PR touching `stg_orders`, and the comment says which forty tables are
built from it and what it costs to run.

The content is entirely Phase 04's lineage graph and Phase 05's cost attribution.
What is new is the addressing — the input is *changed file paths*, because that
is what a CI job actually has, and `models/marts/fct_orders.sql` has to become a
node in a graph that has never heard of a filename.

Two rules, both about not over-claiming:

  **Upstream is not blast radius.** Changing a model does not endanger what it
  reads. Listing upstream tables would pad the comment with things that are not
  at risk, and a comment people skim is a comment that stops working.

  **An unknown model is reported as unknown.** A brand-new model has no lineage
  yet, and "no downstream impact" would be a reassurance we have not earned --
  it is indistinguishable from "we have never seen this run".
"""

from __future__ import annotations

import logging
from pathlib import PurePosixPath
from typing import Any

import psycopg

from . import identity, lineage

log = logging.getLogger("dataspine.pr")

# Only these are models. A PR touching `dbt_project.yml` or a README must not
# post an empty box on every push.
MODEL_SUFFIXES = (".sql", ".py")

DEFAULT_DEPTH = 5


def changed_models(paths: list[str]) -> list[str]:
    """Model names from changed file paths.

    dbt's own convention is that the file stem is the model name, which is also
    what the lineage graph knows a table by.
    """
    models = []
    for path in paths:
        candidate = PurePosixPath(path)
        if candidate.suffix.lower() not in MODEL_SUFFIXES:
            continue
        # `models/` is dbt's default, but projects rename it; anything under a
        # path segment called models/snapshots counts, and so does a bare .sql
        # file, because being generous here costs a mention and being strict
        # costs a missed impact warning.
        if candidate.stem and candidate.stem not in models:
            models.append(candidate.stem)
    return models


def impact(
    conn: psycopg.Connection, paths: list[str], *, depth: int = DEFAULT_DEPTH
) -> list[dict[str, Any]]:
    """Downstream impact and cost for each changed model."""
    from . import cost as cost_mod

    results = []
    for model in changed_models(paths):
        matches = identity.find(conn, model)
        entity = next((m for m in matches if m["name"] == model), None)
        if entity is None:
            results.append({"model": model, "known": False, "downstream": [], "cost_usd": None})
            continue

        downstream = lineage.downstream(conn, entity["id"], depth=depth)
        results.append(
            {
                "model": model,
                "known": True,
                "entity_id": entity["id"],
                "downstream": sorted(downstream, key=lambda n: (n["distance"], n["name"])),
                "cost_usd": _recent_cost(conn, model, cost_mod),
            }
        )
    return results


def _recent_cost(conn: psycopg.Connection, model: str, cost_mod: Any) -> float | None:
    """What this model has recently cost to build, if anything priced it.

    "This costs $34 a night and four things depend on it" is a better merge
    decision than either half alone.
    """
    for row in cost_mod.by_job(conn):
        if row["job_name"].rsplit(".", 1)[-1] == model:
            return float(row["cost_usd"])
    return None


def comment(
    conn: psycopg.Connection, paths: list[str], *, depth: int = DEFAULT_DEPTH
) -> str:
    """Render the PR comment. Empty string when there is nothing to say.

    Deterministic for the same input, so a CI job can update one comment in place
    rather than posting a new one on every push.
    """
    results = impact(conn, paths, depth=depth)
    if not results:
        return ""

    lines = ["### wish:d — downstream impact", ""]
    for entry in results:
        if not entry["known"]:
            lines += [
                f"**`{entry['model']}`** — no lineage recorded yet, so the impact of "
                f"this change is unknown. That is not the same as no impact: nothing "
                f"has been observed building or reading it.",
                "",
            ]
            continue

        cost_note = (
            f" · recently **${entry['cost_usd']:,.2f}** to build"
            if entry["cost_usd"] is not None
            else ""
        )
        downstream = entry["downstream"]
        if not downstream:
            lines += [
                f"**`{entry['model']}`** — nothing downstream depends on this{cost_note}.",
                "",
            ]
            continue

        lines += [
            f"**`{entry['model']}`** — {len(downstream)} downstream "
            f"table{'s' if len(downstream) != 1 else ''}{cost_note}:",
            "",
            "| Table | Hops |",
            "| --- | ---: |",
        ]
        lines += [f"| `{node['name']}` | {node['distance']} |" for node in downstream]
        lines.append("")

    lines.append(
        "<sub>Impact is computed from observed lineage — what has actually run — "
        "not from parsing the dbt DAG.</sub>"
    )
    return "\n".join(lines)
