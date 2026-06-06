"""Serialization helpers for stable tracked-run API payloads."""

from __future__ import annotations

from typing import Any

from data_processing.models import ProcessingRun


def serialize_run(run: ProcessingRun, *, include_payload: bool = True) -> dict[str, Any]:
    """Return the stable API payload for one tracked processing run."""

    payload = {
        "runId": run.id,
        "taskId": run.task_id,
        "runType": run.run_type,
        "sourceRunId": run.source_run_id,
        "status": run.status,
        "engine": run.engine,
        "bucket": run.bucket,
        "objectKey": run.object_key,
        "progressStage": run.progress_stage,
        "progressPercent": run.progress_percent,
        "errorMessage": run.error_message,
        "createdAt": run.created_at.isoformat(),
        "startedAt": run.started_at.isoformat() if run.started_at else None,
        "completedAt": run.completed_at.isoformat() if run.completed_at else None,
        "fileType": run.file_type,
        "selectedSheet": run.sheet_name,
    }
    if not include_payload or run.status != "completed":
        return payload
    if run.run_type == "spark_compare":
        payload["sparkComparison"] = run.comparison_payload
        return payload
    payload.update(
        {
            "rowCount": run.row_count,
            "schema": run.schema,
            "previewColumns": run.preview_columns,
            "previewRows": run.preview_rows,
            "previewPage": run.preview_page,
            "warnings": run.warnings,
            "processingMetadata": run.processing_metadata,
        }
    )
    return payload
