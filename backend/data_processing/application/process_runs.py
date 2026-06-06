"""Application-layer orchestration for sync, async, and Spark process runs."""

from __future__ import annotations

from .errors import InvalidSourceRunError, RunNotFoundError, SourceRunNotCompletedError, TaskQueueError
from .request_data import build_credentials, build_overrides
from data_processing.services.processing import (
    ProcessingServiceError,
    S3Credentials,
    UnsupportedFileTypeError,
    process_s3_object,
    resolve_supported_file_type,
)
from data_processing.services.run_tracking import (
    create_completed_process_run,
    create_queued_process_run,
    create_queued_spark_comparison_run,
    get_run,
    mark_run_completed,
    mark_run_comparison_completed,
    mark_run_failed,
    mark_run_processing,
    mark_run_queued,
)


def process_sync_run(validated_data: dict) -> dict[str, object]:
    """Process a file synchronously and return the stable API payload."""

    credentials = build_credentials(validated_data)
    overrides = build_overrides(validated_data)
    result = process_s3_object(
        credentials=credentials,
        object_key=validated_data["object_key"],
        sheet_name=validated_data.get("sheet_name", ""),
        overrides=overrides,
        preview_row_limit=validated_data["preview_row_limit"],
    )
    run = create_completed_process_run(result)
    return {
        "runId": run.id,
        "rowCount": result["rowCount"],
        "schema": result["schema"],
        "previewColumns": result["previewColumns"],
        "previewRows": result["previewRows"],
        "previewPage": result["previewPage"],
        "warnings": result["warnings"],
        "processingMetadata": result["processingMetadata"],
        "selectedSheet": result["selectedSheet"],
        "fileType": result["fileType"],
    }


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
    request_payload = _build_processing_request_payload(credentials, validated_data)

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
    request_payload = {
        "access_key_id": credentials.access_key_id,
        "secret_access_key": credentials.secret_access_key,
        "session_token": credentials.session_token,
        "region": credentials.region,
        "bucket": credentials.bucket,
        "prefix": credentials.prefix,
        "object_key": source_run.object_key,
        "page": validated_data["page"],
        "page_size": validated_data["page_size"],
    }

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


def run_background_process(*, run_id: int, request_payload: dict[str, object]) -> dict[str, object]:
    """Execute the queued Pandas processing flow and persist run lifecycle updates."""

    run = get_run(run_id)
    if run is None:
        raise RunNotFoundError("The requested processing run could not be found.")
    credentials = _build_credentials_from_payload(request_payload)

    def report(stage: str, percent: int) -> None:
        mark_run_processing(run, progress_stage=stage, progress_percent=percent)

    try:
        report("staging_file", 10)
        result = process_s3_object(
            credentials=credentials,
            object_key=str(request_payload["object_key"]),
            sheet_name=str(request_payload.get("sheet_name", "")),
            overrides=dict(request_payload.get("overrides", {})),
            preview_row_limit=int(request_payload.get("preview_row_limit", 100)),
            progress_callback=report,
        )
        mark_run_completed(run, result)
        return {"runId": run.id, "status": run.status}
    except (ProcessingServiceError, ValueError) as exc:
        mark_run_failed(run, str(exc))
        raise


def run_background_spark_comparison(*, run_id: int, request_payload: dict[str, object]) -> dict[str, object]:
    """Execute the queued experimental Spark comparison and persist lifecycle updates."""

    from data_processing.services.spark_processing import run_spark_csv_comparison

    run = get_run(run_id)
    if run is None:
        raise RunNotFoundError("The requested processing run could not be found.")
    credentials = _build_credentials_from_payload(request_payload)

    try:
        mark_run_processing(run, progress_stage="staging_file", progress_percent=10)
        result = run_spark_csv_comparison(
            credentials=credentials,
            object_key=str(request_payload["object_key"]),
            page=int(request_payload.get("page", 1)),
            page_size=int(request_payload.get("page_size", 100)),
        )
        mark_run_comparison_completed(run, result)
        return {"runId": run.id, "status": run.status}
    except (ProcessingServiceError, ValueError) as exc:
        mark_run_failed(run, str(exc))
        raise


def _build_processing_request_payload(credentials: S3Credentials, validated_data: dict) -> dict[str, object]:
    """Serialize the validated process request for a background task."""

    return {
        "access_key_id": credentials.access_key_id,
        "secret_access_key": credentials.secret_access_key,
        "session_token": credentials.session_token,
        "region": credentials.region,
        "bucket": credentials.bucket,
        "prefix": credentials.prefix,
        "object_key": validated_data["object_key"],
        "sheet_name": validated_data.get("sheet_name", ""),
        "preview_row_limit": validated_data["preview_row_limit"],
        "overrides": build_overrides(validated_data),
    }


def _build_credentials_from_payload(payload: dict[str, object]) -> S3Credentials:
    """Rehydrate request-scoped S3 credentials inside a background task."""

    return S3Credentials(
        access_key_id=str(payload["access_key_id"]),
        secret_access_key=str(payload["secret_access_key"]),
        session_token=str(payload.get("session_token", "")),
        region=str(payload["region"]),
        bucket=str(payload["bucket"]),
        prefix=str(payload.get("prefix", "")),
    )
