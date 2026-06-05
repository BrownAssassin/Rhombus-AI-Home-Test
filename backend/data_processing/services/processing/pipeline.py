"""Top-level processing and preview orchestration for local and S3-backed files."""

from __future__ import annotations

from pathlib import Path
from time import perf_counter
from typing import Callable

from .errors import ProcessingServiceError, ResourceLimitError, RESOURCE_LIMIT_MESSAGE
from .local_files import CSV_CHUNK_SIZE, load_local_excel_dataframe, process_local_csv_file, read_local_csv_chunks
from .preview import build_preview_page_metadata, convert_preview_slice, fetch_local_csv_preview_page, process_dataframe
from .s3 import MAX_EXCEL_SIZE_BYTES, S3Credentials, build_s3_client, resolve_supported_file_type
from .staging import get_staged_s3_object_path, release_staged_file


ProgressReporter = Callable[[str, int], None]


def process_s3_object(
    credentials: S3Credentials,
    object_key: str,
    sheet_name: str = "",
    overrides: dict[str, str] | None = None,
    preview_row_limit: int = 100,
    progress_callback: ProgressReporter | None = None,
) -> dict[str, object]:
    """Process an S3 object and return the first preview page plus schema."""

    try:
        file_type = resolve_supported_file_type(object_key)
        client = build_s3_client(credentials)
        started = perf_counter()
        if progress_callback is not None:
            progress_callback("staging_file", 10)

        if file_type == "csv":
            result = _process_csv(
                client,
                credentials.bucket,
                object_key,
                overrides or {},
                preview_row_limit,
                progress_callback=progress_callback,
            )
        else:
            result = _process_excel(
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
    schema: list[dict[str, object]],
    row_count: int,
    page: int,
    page_size: int,
    preview_columns: list[str] | None = None,
) -> dict[str, object]:
    """Fetch a later processed preview page for the current file context."""

    try:
        client = build_s3_client(credentials)
        if file_type == "csv":
            staged_file = get_staged_s3_object_path(client, credentials.bucket, object_key)
            try:
                return fetch_local_csv_preview_page(
                    staged_file.path,
                    read_local_csv_chunks=read_local_csv_chunks,
                    schema=schema,
                    row_count=row_count,
                    page=page,
                    page_size=page_size,
                    preview_columns=preview_columns,
                )
            finally:
                release_staged_file(staged_file)

        staged_file = get_staged_s3_object_path(
            client,
            credentials.bucket,
            object_key,
            max_size_bytes=MAX_EXCEL_SIZE_BYTES,
        )
        try:
            df, _ = load_local_excel_dataframe(staged_file.path, selected_sheet)
            preview_page = build_preview_page_metadata(row_count, page=page, page_size=page_size)
            page_columns, page_rows = convert_preview_slice(
                df.iloc[(page - 1) * page_size : page * page_size],
                schema,
                limit=page_size,
            )
        finally:
            release_staged_file(staged_file)

        return {
            "previewColumns": page_columns or preview_columns or [item["column"] for item in schema],
            "previewRows": page_rows,
            "previewPage": preview_page,
            "rowCount": row_count,
        }
    except MemoryError as exc:
        raise ResourceLimitError(RESOURCE_LIMIT_MESSAGE) from exc


def process_local_file(
    file_path: str | Path,
    *,
    sheet_name: str = "",
    overrides: dict[str, str] | None = None,
    preview_row_limit: int = 100,
) -> dict[str, object]:
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
        return result
    except MemoryError as exc:
        raise ResourceLimitError(RESOURCE_LIMIT_MESSAGE) from exc


def _process_csv(
    client,
    bucket: str,
    object_key: str,
    overrides: dict[str, str],
    preview_row_limit: int,
    *,
    progress_callback: ProgressReporter | None = None,
) -> dict[str, object]:
    """Process a staged S3 CSV through the local chunked CSV pipeline."""

    staged_file = get_staged_s3_object_path(client, bucket, object_key)
    try:
        if progress_callback is not None:
            progress_callback("profiling_schema", 45)
        result = process_local_csv_file(
            staged_file.path,
            overrides,
            preview_row_limit,
            progress_callback=progress_callback,
        )
        return {
            "bucket": bucket,
            **result,
            "objectKey": object_key,
        }
    finally:
        release_staged_file(staged_file)


def _process_excel(
    client,
    bucket: str,
    object_key: str,
    sheet_name: str,
    overrides: dict[str, str],
    preview_row_limit: int,
    *,
    progress_callback: ProgressReporter | None = None,
) -> dict[str, object]:
    """Process a staged S3 Excel file through the in-memory Excel path."""

    staged_file = get_staged_s3_object_path(
        client,
        bucket,
        object_key,
        max_size_bytes=MAX_EXCEL_SIZE_BYTES,
    )
    try:
        if progress_callback is not None:
            progress_callback("profiling_schema", 45)
        df, selected_sheet = load_local_excel_dataframe(staged_file.path, sheet_name)
        if progress_callback is not None:
            progress_callback("building_preview", 85)

        result = process_dataframe(
            df,
            overrides=overrides,
            preview_row_limit=preview_row_limit,
            file_type="excel",
            object_key=object_key,
            selected_sheet=selected_sheet,
        )
        return {"bucket": bucket, **result}
    finally:
        release_staged_file(staged_file)
