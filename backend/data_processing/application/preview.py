"""Application-layer orchestration for preview-page loading."""

from __future__ import annotations

from .errors import RunNotCompletedError, RunNotFoundError
from .request_data import build_credentials, build_preview_context
from data_processing.services.processing import fetch_s3_preview_page
from data_processing.services.run_tracking import get_run


def load_preview_page(validated_data: dict) -> dict[str, object]:
    """Page through processed rows using a saved run or stateless preview context."""

    credentials = build_credentials(validated_data)
    preview_context = build_preview_context(validated_data)
    run_id = validated_data.get("run_id")
    run = get_run(run_id) if run_id is not None else None

    if run is not None:
        if run.status != "completed":
            raise RunNotCompletedError("The requested processing run has not completed yet.")
        preview_context = {
            "object_key": run.object_key,
            "file_type": run.file_type,
            "selected_sheet": run.sheet_name,
            "row_count": run.row_count,
            "schema": run.schema,
            "preview_columns": run.preview_columns,
        }
        run_id = run.id
    elif preview_context is None:
        raise RunNotFoundError("The requested processing run could not be found.")

    preview = fetch_s3_preview_page(
        credentials=credentials,
        object_key=preview_context["object_key"],
        file_type=preview_context["file_type"],
        selected_sheet=preview_context["selected_sheet"],
        schema=preview_context["schema"],
        row_count=preview_context["row_count"],
        page=validated_data["page"],
        page_size=validated_data["page_size"],
        preview_columns=preview_context["preview_columns"],
    )

    return {
        "runId": run_id,
        "rowCount": preview["rowCount"],
        "previewColumns": preview["previewColumns"],
        "previewRows": preview["previewRows"],
        "previewPage": preview["previewPage"],
    }
