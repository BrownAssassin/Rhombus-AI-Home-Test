"""Celery tasks for background dataset processing."""

from __future__ import annotations

from celery import shared_task

from data_processing.models import ProcessingRun
from data_processing.services.processing import ProcessingServiceError, S3Credentials, process_s3_object
from data_processing.services.run_tracking import mark_run_completed, mark_run_failed, mark_run_processing
from data_processing.services.spark_processing import run_spark_csv_comparison


def _build_credentials(payload: dict[str, str]) -> S3Credentials:
    """Rehydrate request-scoped S3 credentials inside a background task."""

    return S3Credentials(
        access_key_id=payload["access_key_id"],
        secret_access_key=payload["secret_access_key"],
        session_token=payload.get("session_token", ""),
        region=payload["region"],
        bucket=payload["bucket"],
        prefix=payload.get("prefix", ""),
    )


@shared_task(bind=True)
def process_s3_object_async(self, *, run_id: int, request_payload: dict[str, object]) -> dict[str, object]:
    """Process a dataset in the background and persist run progress as it changes."""

    run = ProcessingRun.objects.get(pk=run_id)
    credentials = _build_credentials(request_payload)

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


@shared_task(bind=True)
def run_spark_comparison(self, *, run_id: int, request_payload: dict[str, object]) -> dict[str, object]:
    """Run the experimental Spark comparison in the background for future use."""

    run = ProcessingRun.objects.get(pk=run_id)
    credentials = _build_credentials(request_payload)

    try:
        mark_run_processing(run, progress_stage="staging_file", progress_percent=10)
        result = run_spark_csv_comparison(
            credentials=credentials,
            object_key=str(request_payload["object_key"]),
            page=int(request_payload.get("page", 1)),
            page_size=int(request_payload.get("page_size", 100)),
        )
        mark_run_completed(
            run,
            {
                "rowCount": result["rowCount"],
                "schema": result["sparkSchema"],
                "previewColumns": result["previewColumns"],
                "previewRows": result["previewRows"],
                "previewPage": result["previewPage"],
                "warnings": result["notes"],
                "processingMetadata": result["processingMetadata"],
                "selectedSheet": "",
                "fileType": result["fileType"],
            },
        )
        return {"runId": run.id, "status": run.status}
    except (ProcessingServiceError, ValueError) as exc:
        mark_run_failed(run, str(exc))
        raise
