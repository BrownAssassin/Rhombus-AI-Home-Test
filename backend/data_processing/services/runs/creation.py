"""Creation helpers for tracked processing runs."""

from __future__ import annotations

from django.utils import timezone

from data_processing.contracts import ProcessResultPayload
from data_processing.models import ProcessingRun


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
