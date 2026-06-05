"""Helpers for persisting tracked processing-run lifecycle state."""

from __future__ import annotations

from django.utils import timezone

from data_processing.contracts import ProcessResultPayload, SparkComparisonResultPayload
from data_processing.models import ProcessingRun


def get_run(run_id: int) -> ProcessingRun | None:
    """Return one tracked run by id or None when it no longer exists."""

    return ProcessingRun.objects.detail_fields().filter(pk=run_id).first()


def get_active_run(run_id: int) -> ProcessingRun | None:
    """Return one active tracked run or None when it is no longer pending."""

    return ProcessingRun.objects.active().detail_fields().filter(pk=run_id).first()


def list_runs(*, object_key: str | None = None, limit: int = 10):
    """Return recent runs ordered newest-first with an optional file filter."""

    runs = ProcessingRun.objects.summary_fields()
    if object_key is not None:
        runs = runs.for_object(object_key)
    return runs.recent(limit)


def create_completed_process_run(result: ProcessResultPayload) -> ProcessingRun:
    """Persist the synchronous process response as a completed tracked run."""

    now = timezone.now()
    return ProcessingRun.objects.create(
        bucket=result["bucket"],
        object_key=result["objectKey"],
        file_type=result["fileType"],
        sheet_name=result["selectedSheet"],
        run_type="process",
        status="completed",
        engine="pandas",
        progress_stage="completed",
        progress_percent=100,
        row_count=result["rowCount"],
        schema=result["schema"],
        warnings=result["warnings"],
        preview_columns=result["previewColumns"],
        preview_rows=result["previewRows"],
        preview_page=result["previewPage"],
        processing_metadata=result["processingMetadata"],
        started_at=now,
        completed_at=now,
    )


def create_queued_process_run(
    *,
    bucket: str,
    object_key: str,
    file_type: str,
    sheet_name: str = "",
) -> ProcessingRun:
    """Create a queued Pandas processing run before Celery picks it up."""

    return ProcessingRun.objects.create(
        bucket=bucket,
        object_key=object_key,
        file_type=file_type,
        sheet_name=sheet_name,
        run_type="process",
        status="queued",
        engine="pandas",
        progress_stage="queued",
        progress_percent=0,
    )


def create_queued_spark_comparison_run(source_run: ProcessingRun) -> ProcessingRun:
    """Create a queued Spark comparison run linked to its source process run."""

    return ProcessingRun.objects.create(
        bucket=source_run.bucket,
        object_key=source_run.object_key,
        file_type=source_run.file_type,
        sheet_name=source_run.sheet_name,
        run_type="spark_compare",
        source_run=source_run,
        status="queued",
        engine="spark",
        progress_stage="queued",
        progress_percent=0,
    )


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


def mark_run_completed(run: ProcessingRun, result: ProcessResultPayload) -> ProcessingRun:
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
    run.comparison_payload = {}
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
            "comparison_payload",
            "processing_metadata",
            "sheet_name",
            "file_type",
            "started_at",
            "completed_at",
        ]
    )
    return run


def mark_run_comparison_completed(run: ProcessingRun, result: SparkComparisonResultPayload) -> ProcessingRun:
    """Persist a completed Spark comparison without flattening it into the Pandas shape."""

    run.status = "completed"
    run.error_message = ""
    run.progress_stage = "completed"
    run.progress_percent = 100
    run.row_count = result["rowCount"]
    run.warnings = result["notes"]
    run.preview_columns = result["previewColumns"]
    run.preview_rows = result["previewRows"]
    run.preview_page = result["previewPage"]
    run.comparison_payload = result
    run.processing_metadata = result["processingMetadata"]
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
            "warnings",
            "preview_columns",
            "preview_rows",
            "preview_page",
            "comparison_payload",
            "processing_metadata",
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
    run.progress_percent = 100
    if run.completed_at is None:
        run.completed_at = timezone.now()
    run.save(update_fields=["status", "error_message", "progress_stage", "progress_percent", "completed_at"])
    return run
