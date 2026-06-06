"""Application use case for synchronous file processing."""

from __future__ import annotations

from data_processing.contracts import ProcessRequestPayload, ProcessResponsePayload
from .request_data import build_credentials, build_overrides
from data_processing.services.processing import process_s3_object
from data_processing.services.run_tracking import create_completed_process_run


def process_sync_run(validated_data: ProcessRequestPayload) -> ProcessResponsePayload:
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
