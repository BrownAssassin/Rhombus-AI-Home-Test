"""Local CSV and Excel file readers shared by S3 and CLI processing paths."""

from __future__ import annotations

from collections.abc import Iterator
import logging
import os
from pathlib import Path
from typing import Callable

import pandas as pd

from data_processing.contracts import PreviewRow, ProcessResultPayload
from data_processing.services.observability import elapsed_ms, log_stage_event, stage_started

from .errors import FileTooLargeError, ProcessingServiceError, ResourceLimitError, RESOURCE_LIMIT_MESSAGE, S3AccessError
from .preview import build_preview_page_metadata, build_schema_from_profiles, capture_preview_frame, convert_preview_slice
from .s3 import MAX_EXCEL_SIZE_BYTES


CSV_CHUNK_SIZE = int(os.getenv("CSV_CHUNK_SIZE", "500"))
ProgressReporter = Callable[[str, int], None]
logger = logging.getLogger(__name__)


def read_local_csv_chunks(file_path: Path) -> Iterator[pd.DataFrame]:
    """Yield CSV chunks as strings so inference can control all conversions."""

    try:
        yield from pd.read_csv(
            file_path,
            dtype=str,
            keep_default_na=False,
            na_filter=False,
            chunksize=CSV_CHUNK_SIZE,
        )
    except MemoryError as exc:
        raise ResourceLimitError(RESOURCE_LIMIT_MESSAGE) from exc
    except pd.errors.ParserError as exc:
        raise ProcessingServiceError("The selected CSV file could not be parsed.") from exc


def fetch_local_csv_columns(file_path: Path) -> list[str]:
    """Read just the header row to preserve source column ordering."""

    try:
        return list(pd.read_csv(file_path, dtype=str, keep_default_na=False, na_filter=False, nrows=0).columns)
    except MemoryError as exc:
        raise ResourceLimitError(RESOURCE_LIMIT_MESSAGE) from exc
    except pd.errors.ParserError as exc:
        raise ProcessingServiceError("The selected CSV file could not be parsed.") from exc


def load_local_excel_dataframe(file_path: Path, sheet_name: str) -> tuple[pd.DataFrame, str]:
    """Load an Excel sheet into memory after enforcing the MVP size guardrail."""

    if file_path.stat().st_size > MAX_EXCEL_SIZE_BYTES:
        raise FileTooLargeError(
            f"Excel files larger than {MAX_EXCEL_SIZE_BYTES // (1024 * 1024)} MB are rejected in this MVP."
        )

    target_sheet = sheet_name or 0
    try:
        df = pd.read_excel(
            file_path,
            sheet_name=target_sheet,
            dtype=str,
            keep_default_na=False,
        )
    except MemoryError as exc:
        raise ResourceLimitError(RESOURCE_LIMIT_MESSAGE) from exc
    except ValueError as exc:
        raise S3AccessError("The requested Excel sheet could not be found.") from exc

    selected_sheet = sheet_name if isinstance(target_sheet, str) else ""
    return df, selected_sheet


def process_local_csv_file(
    file_path: Path,
    overrides: dict[str, str],
    preview_row_limit: int,
    *,
    progress_callback: ProgressReporter | None = None,
) -> ProcessResultPayload:
    """Infer schema from a local CSV while keeping preview work bounded."""

    started = stage_started()
    columns = fetch_local_csv_columns(file_path)
    from data_processing.services.inference import create_profiles, update_profiles_from_dataframe

    profiles = create_profiles(columns)
    row_count = 0
    preview_frames: list[pd.DataFrame] = []
    collected_preview_rows = 0

    for chunk in read_local_csv_chunks(file_path):
        row_count += len(chunk)
        update_profiles_from_dataframe(profiles, chunk)
        collected_preview_rows = capture_preview_frame(
            preview_frames,
            chunk,
            collected_rows=collected_preview_rows,
            preview_row_limit=preview_row_limit,
        )

    schema, warnings = build_schema_from_profiles(profiles, overrides)
    log_stage_event(
        logger,
        "processing.local_csv.profiled",
        file_path=str(file_path),
        row_count=row_count,
        column_count=len(columns),
        duration_ms=elapsed_ms(started),
    )
    if progress_callback is not None:
        progress_callback("building_preview", 85)

    preview_rows: list[PreviewRow] = []
    preview_columns: list[str] = columns
    if preview_frames:
        preview_source = pd.concat(preview_frames, ignore_index=True)
        preview_columns, preview_rows = convert_preview_slice(preview_source, schema, limit=preview_row_limit)

    return {
        "objectKey": str(file_path),
        "fileType": "csv",
        "selectedSheet": "",
        "rowCount": row_count,
        "schema": schema,
        "previewColumns": preview_columns,
        "previewRows": preview_rows[:preview_row_limit],
        "previewPage": build_preview_page_metadata(row_count, page=1, page_size=preview_row_limit),
        "warnings": warnings,
    }
