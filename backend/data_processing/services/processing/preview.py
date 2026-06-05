"""Preview payload shaping, schema aggregation, and page slicing helpers."""

from __future__ import annotations

from collections.abc import Iterator
import logging
from time import perf_counter
from typing import Any

import pandas as pd

from data_processing.contracts import PreviewPagePayload, PreviewResultPayload, ProcessResultPayload, SchemaItem

from data_processing.services.inference import (
    convert_dataframe,
    create_profiles,
    dataframe_preview,
    infer_profiles,
    update_profiles_from_dataframe,
    validate_overrides,
)

from .errors import InvalidPreviewPageError


logger = logging.getLogger(__name__)


def build_schema_from_profiles(profiles, overrides: dict[str, str]) -> tuple[list[SchemaItem], list[str]]:
    """Infer the schema and flatten any per-column warnings into one list."""

    schema = infer_profiles(profiles)
    schema = validate_overrides(profiles, schema, overrides)
    warnings = sorted({warning for item in schema for warning in item["warnings"]})
    return schema, warnings


def build_preview_page_metadata(row_count: int, page: int, page_size: int) -> PreviewPagePayload:
    """Return stable preview-page metadata for the requested slice."""

    total_pages = max(1, (row_count + page_size - 1) // page_size) if row_count else 1
    if page > total_pages:
        raise InvalidPreviewPageError("The requested preview page is outside the available row range.")

    return {
        "page": page,
        "pageSize": page_size,
        "totalRows": row_count,
        "totalPages": total_pages,
        "hasPreviousPage": page > 1,
        "hasNextPage": page < total_pages,
    }


def convert_preview_slice(
    df: pd.DataFrame,
    schema: list[SchemaItem],
    *,
    limit: int,
) -> tuple[list[str], list[dict[str, Any]]]:
    """Convert only the rows needed for the current preview slice."""

    if limit <= 0 or df.empty:
        return [item["column"] for item in schema], []

    preview_frame = df.iloc[:limit]
    converted_preview = convert_dataframe(preview_frame, schema)
    return dataframe_preview(converted_preview, len(converted_preview), schema=schema)


def capture_preview_frame(
    preview_frames: list[pd.DataFrame],
    chunk: pd.DataFrame,
    *,
    collected_rows: int,
    preview_row_limit: int,
) -> int:
    """Retain only the raw rows needed to build the initial preview page."""

    if collected_rows >= preview_row_limit or chunk.empty:
        return collected_rows

    remaining = preview_row_limit - collected_rows
    preview_frames.append(chunk.iloc[:remaining].copy())
    return collected_rows + min(len(chunk), remaining)


def paginate_converted_chunks(
    chunks: Iterator[pd.DataFrame],
    schema: list[SchemaItem],
    *,
    page: int,
    page_size: int,
) -> tuple[list[str], list[dict[str, Any]]]:
    """Convert just the requested page while streaming through CSV chunks."""

    start = (page - 1) * page_size
    end = start + page_size
    seen_rows = 0
    page_columns = [item["column"] for item in schema]
    page_rows: list[dict[str, Any]] = []

    for chunk in chunks:
        chunk_end = seen_rows + len(chunk)
        if chunk_end <= start:
            seen_rows = chunk_end
            continue

        local_start = max(start - seen_rows, 0)
        local_end = min(end - seen_rows, len(chunk))
        if local_start < local_end:
            page_columns, chunk_rows = convert_preview_slice(
                chunk.iloc[local_start:local_end],
                schema,
                limit=local_end - local_start,
            )
            page_rows.extend(chunk_rows)
        seen_rows = chunk_end
        if seen_rows >= end:
            break

    return page_columns, page_rows


def process_dataframe(
    df: pd.DataFrame,
    *,
    overrides: dict[str, str] | None = None,
    preview_row_limit: int = 100,
    file_type: str = "csv",
    object_key: str = "",
    selected_sheet: str = "",
) -> ProcessResultPayload:
    """Process an in-memory dataframe and return schema plus preview payloads."""

    started = perf_counter()
    profiles = create_profiles(df.columns)
    update_profiles_from_dataframe(profiles, df)
    schema, warnings = build_schema_from_profiles(profiles, overrides or {})
    preview_columns, preview_rows = convert_preview_slice(df, schema, limit=preview_row_limit)
    preview_page = build_preview_page_metadata(len(df), page=1, page_size=preview_row_limit)
    logger.info(
        "processing.preview.process_dataframe.completed",
        extra={
            "object_key": object_key,
            "file_type": file_type,
            "row_count": len(df),
            "preview_row_limit": preview_row_limit,
            "duration_ms": round((perf_counter() - started) * 1000, 2),
        },
    )

    return {
        "objectKey": object_key,
        "fileType": file_type,
        "selectedSheet": selected_sheet,
        "rowCount": len(df),
        "schema": schema,
        "previewColumns": preview_columns,
        "previewRows": preview_rows,
        "previewPage": preview_page,
        "warnings": warnings,
    }


def fetch_local_csv_preview_page(
    file_path,
    *,
    read_local_csv_chunks,
    schema: list[SchemaItem],
    row_count: int,
    page: int,
    page_size: int,
    preview_columns: list[str] | None = None,
) -> PreviewResultPayload:
    """Load one processed CSV preview page from a staged local file."""

    started = perf_counter()
    preview_page = build_preview_page_metadata(row_count, page=page, page_size=page_size)
    page_columns = preview_columns or [item["column"] for item in schema]
    if row_count == 0:
        return {
            "previewColumns": page_columns,
            "previewRows": [],
            "previewPage": preview_page,
            "rowCount": row_count,
        }

    page_columns, page_rows = paginate_converted_chunks(
        read_local_csv_chunks(file_path),
        schema,
        page=page,
        page_size=page_size,
    )
    logger.info(
        "processing.preview.fetch_local_csv_preview_page.completed",
        extra={
            "file_path": str(file_path),
            "page": page,
            "page_size": page_size,
            "row_count": row_count,
            "duration_ms": round((perf_counter() - started) * 1000, 2),
        },
    )
    return {
        "previewColumns": page_columns or preview_columns or [item["column"] for item in schema],
        "previewRows": page_rows,
        "previewPage": preview_page,
        "rowCount": row_count,
    }
