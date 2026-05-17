"""Helpers for persisting and serializing processing-run lifecycle state."""

from __future__ import annotations

from typing import Any

from django.utils import timezone

from data_processing.models import ProcessingRun


def serialize_run(run: ProcessingRun) -> dict[str, Any]:
    """Return the stable API payload for one tracked processing run."""

    payload: dict[str, Any] = {
        "runId": run.id,
        "taskId": run.task_id,
        "status": run.status,
        "engine": run.engine,
        "progressStage": run.progress_stage,
        "progressPercent": run.progress_percent,
        "errorMessage": run.error_message,
    }
    if run.status == "completed":
        payload.update(
            {
                "rowCount": run.row_count,
                "schema": run.schema,
                "previewColumns": run.preview_columns,
                "previewRows": run.preview_rows,
                "previewPage": run.preview_page,
                "warnings": run.warnings,
                "processingMetadata": run.processing_metadata,
                "selectedSheet": run.sheet_name,
                "fileType": run.file_type,
            }
        )
    return payload


def mark_run_queued(run: ProcessingRun, *, task_id: str, engine: str = "pandas") -> ProcessingRun:
    """Persist the queued state once Celery returns a task identifier."""

    run.task_id = task_id
    run.engine = engine
    run.status = "queued"
    run.progress_stage = "queued"
    run.progress_percent = 0
    run.error_message = ""
    run.save(
        update_fields=[
            "task_id",
            "engine",
            "status",
            "progress_stage",
            "progress_percent",
            "error_message",
        ]
    )
    return run


def mark_run_processing(
    run: ProcessingRun,
    *,
    progress_stage: str,
    progress_percent: int,
) -> ProcessingRun:
    """Update the current background-processing stage for a run."""

    run.status = "processing"
    run.progress_stage = progress_stage
    run.progress_percent = progress_percent
    if run.started_at is None:
        run.started_at = timezone.now()
    run.save(update_fields=["status", "progress_stage", "progress_percent", "started_at"])
    return run


def mark_run_completed(run: ProcessingRun, result: dict[str, Any]) -> ProcessingRun:
    """Persist the finished processing payload in the tracked run record."""

    run.status = "completed"
    run.error_message = ""
    run.progress_stage = "completed"
    run.progress_percent = 100
    run.row_count = result["rowCount"]
    run.schema = result["schema"]
    run.warnings = result["warnings"]
    run.preview_columns = result["previewColumns"]
    run.preview_rows = result["previewRows"]
    run.preview_page = result["previewPage"]
    run.processing_metadata = result["processingMetadata"]
    run.sheet_name = result["selectedSheet"]
    run.file_type = result["fileType"]
    if run.started_at is None:
        run.started_at = timezone.now()
    run.completed_at = timezone.now()
    run.save(
        update_fields=[
            "status",
            "error_message",
            "progress_stage",
            "progress_percent",
            "row_count",
            "schema",
            "warnings",
            "preview_columns",
            "preview_rows",
            "preview_page",
            "processing_metadata",
            "sheet_name",
            "file_type",
            "started_at",
            "completed_at",
        ]
    )
    return run


def mark_run_failed(run: ProcessingRun, message: str) -> ProcessingRun:
    """Persist a terminal failure state for a tracked processing run."""

    run.status = "failed"
    run.error_message = message
    run.progress_stage = "failed"
    if run.completed_at is None:
        run.completed_at = timezone.now()
    run.save(update_fields=["status", "error_message", "progress_stage", "completed_at"])
    return run
