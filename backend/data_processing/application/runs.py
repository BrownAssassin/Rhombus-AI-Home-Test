"""Application-layer orchestration for tracked run lookups and listing."""

from __future__ import annotations

from .errors import RunNotFoundError
from data_processing.services.run_payloads import serialize_run
from data_processing.services.run_tracking import get_run, list_runs


def get_run_status_payload(run_id: int) -> dict[str, object]:
    """Return the serialized polling payload for one tracked run."""

    run = get_run(run_id)
    if run is None:
        raise RunNotFoundError("The requested processing run could not be found.")
    return serialize_run(run)


def list_recent_runs_payload(validated_data: dict) -> dict[str, object]:
    """Return recent runs for the jobs tray using existing query filters."""

    object_key = validated_data.get("object_key")
    limit = validated_data["limit"]
    runs = list_runs(object_key=object_key, limit=limit)
    return {"runs": [serialize_run(run, include_payload=False) for run in runs]}
