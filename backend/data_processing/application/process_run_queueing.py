"""Application use cases for queueing process and Spark comparison runs."""

from __future__ import annotations

from .errors import InvalidSourceRunError, RunNotFoundError, SourceRunNotCompletedError, TaskQueueError
from .process_run_payloads import build_processing_task_request, build_spark_task_request
from .request_data import build_credentials
from data_processing.services.processing import UnsupportedFileTypeError, resolve_supported_file_type
from data_processing.services.run_tracking import (
    create_queued_process_run,
    create_queued_spark_comparison_run,
    get_run,
    mark_run_failed,
    mark_run_queued,
)


def queue_process_run(validated_data: dict, *, delay_callable) -> dict[str, object]:
    """Create a queued processing run and enqueue the Celery task."""

    credentials = build_credentials(validated_data)
    file_type = resolve_supported_file_type(validated_data["object_key"])
    run = create_queued_process_run(
        bucket=credentials.bucket,
        object_key=validated_data["object_key"],
        file_type=file_type,
        sheet_name=validated_data.get("sheet_name", ""),
    )
    request_payload = build_processing_task_request(credentials, validated_data)

    try:
        task_result = delay_callable(run_id=run.id, request_payload=request_payload)
    except Exception:
        mark_run_failed(run, "Background processing could not be queued in the current environment.")
        raise TaskQueueError("Background processing could not be queued in the current environment.")

    mark_run_queued(run, task_id=task_result.id)
    return {
        "runId": run.id,
        "taskId": task_result.id,
        "runType": run.run_type,
        "status": run.status,
        "engine": run.engine,
    }


def queue_spark_comparison(validated_data: dict, *, delay_callable) -> dict[str, object]:
    """Create a queued Spark comparison run and enqueue the Celery task."""

    credentials = build_credentials(validated_data)
    source_run = get_run(validated_data["source_run_id"])
    if source_run is None:
        raise RunNotFoundError("The requested source processing run could not be found.")
    if source_run.run_type != "process":
        raise InvalidSourceRunError("Spark comparisons must reference a completed Pandas processing run.")
    if source_run.status != "completed":
        raise SourceRunNotCompletedError("The requested source processing run has not completed yet.")
    if source_run.file_type != "csv":
        raise UnsupportedFileTypeError("Spark comparison currently supports CSV files only.")

    comparison_run = create_queued_spark_comparison_run(source_run)
    request_payload = build_spark_task_request(
        credentials,
        object_key=source_run.object_key,
        page=validated_data["page"],
        page_size=validated_data["page_size"],
    )

    try:
        task_result = delay_callable(run_id=comparison_run.id, request_payload=request_payload)
    except Exception:
        mark_run_failed(comparison_run, "Spark comparison could not be queued in the current environment.")
        raise TaskQueueError("Spark comparison could not be queued in the current environment.")

    mark_run_queued(comparison_run, task_id=task_result.id, engine="spark")
    return {
        "runId": comparison_run.id,
        "taskId": task_result.id,
        "status": comparison_run.status,
        "engine": comparison_run.engine,
        "runType": comparison_run.run_type,
        "sourceRunId": comparison_run.source_run_id,
    }
