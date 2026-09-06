"""OpenLineage event model.

Deliberately permissive. The ingest gateway's job is to accept anything a real
producer emits and normalise it, not to be a spec validator -- a rejected event
is a hole in the run tree, and a hole in the run tree is the one failure mode
this whole system exists to prevent.

We validate the handful of fields we actually key on (eventTime, run.runId,
job.namespace, job.name) and pass everything else through as opaque facets.

Spec reference: https://openlineage.io/spec/2-0-2/OpenLineage.json
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

EventType = Literal["START", "RUNNING", "COMPLETE", "ABORT", "FAIL", "OTHER"]

# Run state, our normalisation of eventType. Rank matters: state only ever moves
# forward, so a late-arriving START cannot resurrect a run that already failed.
STATE_RANK = {"UNKNOWN": 0, "RUNNING": 1, "COMPLETED": 2, "FAILED": 2, "ABORTED": 2}

EVENT_TYPE_TO_STATE: dict[str, str | None] = {
    "START": "RUNNING",
    "RUNNING": "RUNNING",
    "COMPLETE": "COMPLETED",
    "FAIL": "FAILED",
    "ABORT": "ABORTED",
    "OTHER": None,  # metadata-only event; never changes state
}

TERMINAL_STATES = {"COMPLETED", "FAILED", "ABORTED"}


class _Loose(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)


class Run(_Loose):
    runId: UUID
    facets: dict[str, Any] = Field(default_factory=dict)


class Job(_Loose):
    namespace: str
    name: str
    facets: dict[str, Any] = Field(default_factory=dict)


class Dataset(_Loose):
    namespace: str
    name: str
    facets: dict[str, Any] = Field(default_factory=dict)
    # InputDataset / OutputDataset add exactly one field each.
    inputFacets: dict[str, Any] = Field(default_factory=dict)
    outputFacets: dict[str, Any] = Field(default_factory=dict)


class RunEvent(_Loose):
    eventTime: datetime
    producer: str = ""
    schemaURL: str = ""
    eventType: EventType | None = None
    run: Run
    job: Job
    inputs: list[Dataset] = Field(default_factory=list)
    outputs: list[Dataset] = Field(default_factory=list)


# --------------------------------------------------------------- facet readers
#
# Facet keys are fixed by the spec; the values are not, and producers disagree
# about which optional fields they populate. Every reader below tolerates a
# missing or malformed facet by returning None rather than raising.


def parent_facet(run_facets: dict[str, Any]) -> dict[str, Any] | None:
    """ParentRunFacet: the single most important facet in the system.

    Shape (spec/facets/1-2-0/ParentRunFacet.json)::

        {"parent": {"run": {"runId": ...},
                    "job": {"namespace": ..., "name": ...},
                    "root": {"run": {"runId": ...}, "job": {...}}}}

    `root` was added later and is emitted by newer Airflow/Spark integrations.
    When absent we derive the root by walking up the chain instead.
    """
    parent = run_facets.get("parent")
    if not isinstance(parent, dict):
        return None
    run = parent.get("run") or {}
    job = parent.get("job") or {}
    if not run.get("runId") or not job.get("namespace") or not job.get("name"):
        return None
    return parent


def nominal_times(run_facets: dict[str, Any]) -> tuple[str | None, str | None]:
    """NominalTimeRunFacet -- the *scheduled* window, not the actual one.

    This is what makes "the 02:00 run is 40 minutes late" answerable, as opposed
    to just "a run happened at 02:41".
    """
    facet = run_facets.get("nominalTime")
    if not isinstance(facet, dict):
        return None, None
    return facet.get("nominalStartTime"), facet.get("nominalEndTime")


def error_details(run_facets: dict[str, Any]) -> tuple[str | None, str | None]:
    """ErrorMessageRunFacet. Populated by the dbt structured-log consumer and by
    the Spark listener on job failure -- this is the text you currently go
    digging through EMR logs for."""
    facet = run_facets.get("errorMessage")
    if not isinstance(facet, dict):
        return None, None
    return facet.get("message"), facet.get("stackTrace")


def job_type(job_facets: dict[str, Any]) -> tuple[str | None, str | None, str | None]:
    """JobTypeJobFacet -> (integration, job_type, processing_type)."""
    facet = job_facets.get("jobType")
    if not isinstance(facet, dict):
        return None, None, None
    return facet.get("integration"), facet.get("jobType"), facet.get("processingType")


def documentation(job_facets: dict[str, Any]) -> str | None:
    facet = job_facets.get("documentation")
    if isinstance(facet, dict):
        return facet.get("description")
    return None


def output_statistics(output_facets: dict[str, Any]) -> tuple[int | None, int | None]:
    """OutputStatisticsOutputDatasetFacet -> (row_count, size_bytes).

    Free volume metrics straight off the write path, no table scan. Phase 03's
    volume monitors read these.
    """
    facet = output_facets.get("outputStatistics")
    if not isinstance(facet, dict):
        return None, None
    return facet.get("rowCount"), facet.get("size")


def input_statistics(input_facets: dict[str, Any]) -> tuple[int | None, int | None]:
    """InputStatisticsInputDatasetFacet -> (row_count, size_bytes)."""
    facet = input_facets.get("inputStatistics")
    if not isinstance(facet, dict):
        return None, None
    return facet.get("rowCount"), facet.get("size")
