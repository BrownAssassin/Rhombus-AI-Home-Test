"""Top-level processing and preview orchestration for local and S3-backed files."""

from __future__ import annotations

import logging
from pathlib import Path
from time import perf_counter
from typing import Callable

from data_processing.contracts import PreviewResultPayload, ProcessResultPayload, SchemaItem

from .csv_pipeline import fetch_csv_preview_page, process_staged_csv
from .errors import ProcessingServiceError, ResourceLimitError, RESOURCE_LIMIT_MESSAGE
from .excel_pipeline import fetch_excel_preview_page, process_staged_excel
from .local_files import CSV_CHUNK_SIZE, load_local_excel_dataframe, process_local_csv_file
from .preview import process_dataframe
from .s3 import S3Credentials, build_s3_client, resolve_supported_file_type


ProgressReporter = Callable[[str, int], None]
logger = logging.getLogger(__name__)


def process_s3_object(
    credentials: S3Credentials,
    object_key: str,
    sheet_name: str = "",
    overrides: dict[str, str] | None = None,
    preview_row_limit: int = 100,
    progress_callback: ProgressReporter | None = None,
) -> ProcessResultPayload:
    """Process an S3 object and return the first preview page plus schema."""

    try:
        file_type = resolve_supported_file_type(object_key)
        client = build_s3_client(credentials)
        started = perf_counter()
        if progress_callback is not None:
            progress_callback("staging_file", 10)

        if file_type == "csv":
            result = process_staged_csv(
                client,
                credentials.bucket,
                object_key,
                overrides or {},
                preview_row_limit,
                progress_callback=progress_callback,
            )
        else:
            result = process_staged_excel(
                client,
                credentials.bucket,
                object_key,
                sheet_name,
                overrides or {},
                preview_row_limit,
                progress_callback=progress_callback,
            )

        duration_ms = round((perf_counter() - started) * 1000, 2)
        result["processingMetadata"] = {
            "durationMs": duration_ms,
            "previewRowLimit": preview_row_limit,
            "chunkSize": CSV_CHUNK_SIZE if file_type == "csv" else None,
            "appliedOverrides": overrides or {},
        }
        logger.info(
            "processing.pipeline.completed",
            extra={
                "bucket": credentials.bucket,
                "object_key": object_key,
                "file_type": file_type,
                "duration_ms": duration_ms,
                "row_count": result["rowCount"],
            },
        )
        if progress_callback is not None:
            progress_callback("completed", 100)
        return result
    except MemoryError as exc:
        raise ResourceLimitError(RESOURCE_LIMIT_MESSAGE) from exc


def fetch_s3_preview_page(
    *,
    credentials: S3Credentials,
    object_key: str,
    file_type: str,
    selected_sheet: str,
    schema: list[SchemaItem],
    row_count: int,
    page: int,
    page_size: int,
    preview_columns: list[str] | None = None,
) -> PreviewResultPayload:
    """Fetch a later processed preview page for the current file context."""

    try:
        client = build_s3_client(credentials)
        if file_type == "csv":
            preview = fetch_csv_preview_page(
                client,
                credentials.bucket,
                object_key,
                schema=schema,
                row_count=row_count,
                page=page,
                page_size=page_size,
                preview_columns=preview_columns,
            )
        else:
            preview = fetch_excel_preview_page(
                client,
                credentials.bucket,
                object_key,
                selected_sheet=selected_sheet,
                schema=schema,
                row_count=row_count,
                page=page,
                page_size=page_size,
                preview_columns=preview_columns,
            )
        logger.info(
            "processing.preview_page.completed",
            extra={
                "bucket": credentials.bucket,
                "object_key": object_key,
                "file_type": file_type,
                "page": page,
                "page_size": page_size,
                "row_count": row_count,
            },
        )
        return preview
    except MemoryError as exc:
        raise ResourceLimitError(RESOURCE_LIMIT_MESSAGE) from exc


def process_local_file(
    file_path: str | Path,
    *,
    sheet_name: str = "",
    overrides: dict[str, str] | None = None,
    preview_row_limit: int = 100,
) -> ProcessResultPayload:
    """Process a local CSV or Excel file through the shared service layer."""

    path = Path(file_path)
    if not path.exists():
        raise ProcessingServiceError(f"Local file '{path}' does not exist.")

    file_type = resolve_supported_file_type(path.name)
    started = perf_counter()

    try:
        if file_type == "csv":
            result = process_local_csv_file(path, overrides or {}, preview_row_limit)
        else:
            df, selected_sheet = load_local_excel_dataframe(path, sheet_name)
            result = process_dataframe(
                df,
                overrides=overrides,
                preview_row_limit=preview_row_limit,
                file_type=file_type,
                object_key=str(path),
                selected_sheet=selected_sheet,
            )

        duration_ms = round((perf_counter() - started) * 1000, 2)
        result["processingMetadata"] = {
            "durationMs": duration_ms,
            "previewRowLimit": preview_row_limit,
            "chunkSize": CSV_CHUNK_SIZE if file_type == "csv" else None,
            "appliedOverrides": overrides or {},
        }
        logger.info(
            "processing.local.completed",
            extra={"file_path": str(path), "file_type": file_type, "duration_ms": duration_ms, "row_count": result["rowCount"]},
        )
        return result
    except MemoryError as exc:
        raise ResourceLimitError(RESOURCE_LIMIT_MESSAGE) from exc
