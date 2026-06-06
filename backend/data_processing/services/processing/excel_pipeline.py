"""Excel-specific processing and preview helpers built on the staged-file contract."""

from __future__ import annotations

import logging

from data_processing.contracts import PreviewResultPayload, ProcessResultPayload, SchemaItem
from data_processing.services.observability import log_stage_event

from .local_files import load_local_excel_dataframe
from .preview import build_preview_page_metadata, convert_preview_slice, process_dataframe
from .s3 import MAX_EXCEL_SIZE_BYTES
from .staging import lease_staged_s3_object


logger = logging.getLogger(__name__)


def process_staged_excel(
    client,
    bucket: str,
    object_key: str,
    sheet_name: str,
    overrides: dict[str, str],
    preview_row_limit: int,
    *,
    progress_callback=None,
) -> ProcessResultPayload:
    """Process a staged S3 Excel file through the in-memory Excel path."""

    with lease_staged_s3_object(
        client,
        bucket,
        object_key,
        max_size_bytes=MAX_EXCEL_SIZE_BYTES,
    ) as staged_file:
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
        log_stage_event(
            logger,
            "processing.excel.completed",
            bucket=bucket,
            object_key=object_key,
            row_count=result["rowCount"],
        )
        return {"bucket": bucket, **result}


def fetch_excel_preview_page(
    client,
    bucket: str,
    object_key: str,
    *,
    selected_sheet: str,
    schema: list[SchemaItem],
    row_count: int,
    page: int,
    page_size: int,
    preview_columns: list[str] | None = None,
) -> PreviewResultPayload:
    """Load one processed Excel preview page from a staged local file."""

    with lease_staged_s3_object(
        client,
        bucket,
        object_key,
        max_size_bytes=MAX_EXCEL_SIZE_BYTES,
    ) as staged_file:
        df, _ = load_local_excel_dataframe(staged_file.path, selected_sheet)
        preview_page = build_preview_page_metadata(row_count, page=page, page_size=page_size)
        page_columns, page_rows = convert_preview_slice(
            df.iloc[(page - 1) * page_size : page * page_size],
            schema,
            limit=page_size,
        )
        return {
            "previewColumns": page_columns or preview_columns or [item["column"] for item in schema],
            "previewRows": page_rows,
            "previewPage": preview_page,
            "rowCount": row_count,
        }
