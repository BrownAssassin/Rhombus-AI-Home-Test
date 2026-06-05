"""Application use cases for executing queued background runs."""

from __future__ import annotations

from data_processing.contracts import ProcessingTaskRequestPayload, SparkTaskRequestPayload
from .errors import RunNotFoundError
from .process_run_payloads import build_credentials_from_task_payload
from data_processing.services.processing import ProcessingServiceError, process_s3_object
from data_processing.services.run_tracking import (
    get_run,
    mark_run_completed,
    mark_run_comparison_completed,
    mark_run_failed,
    mark_run_processing,
)
from data_processing.services.spark_processing import run_spark_csv_comparison


def run_background_process(*, run_id: int, request_payload: ProcessingTaskRequestPayload) -> dict[str, object]:
    """Execute the queued Pandas processing flow and persist run lifecycle updates."""

    run = get_run(run_id)
    if run is None:
        raise RunNotFoundError("The requested processing run could not be found.")
    credentials = build_credentials_from_task_payload(request_payload)

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


def run_background_spark_comparison(*, run_id: int, request_payload: SparkTaskRequestPayload) -> dict[str, object]:
    """Execute the queued experimental Spark comparison and persist lifecycle updates."""

    run = get_run(run_id)
    if run is None:
        raise RunNotFoundError("The requested processing run could not be found.")
    credentials = build_credentials_from_task_payload(request_payload)

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
