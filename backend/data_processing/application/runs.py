"""Application-layer orchestration for tracked run lookups and listing."""

from __future__ import annotations

from data_processing.contracts import RunDetailPayload, RunListRequestPayload, RunListResponsePayload, RunSummaryPayload
from .errors import RunNotFoundError
from data_processing.services.run_payloads import serialize_run_detail, serialize_run_summary
from data_processing.services.run_tracking import get_run, list_runs


def get_run_status_payload(run_id: int) -> RunDetailPayload:
    """Return the serialized polling payload for one tracked run."""

    run = get_run(run_id)
    if run is None:
        raise RunNotFoundError("The requested processing run could not be found.")
    return serialize_run_detail(run)


def list_recent_runs_payload(validated_data: RunListRequestPayload) -> RunListResponsePayload:
    """Return recent runs for the jobs tray using existing query filters."""

    object_key = validated_data.get("object_key")
    limit = validated_data["limit"]
    runs = list_runs(object_key=object_key, limit=limit)
    return {"runs": [serialize_run_summary(run) for run in runs]}
