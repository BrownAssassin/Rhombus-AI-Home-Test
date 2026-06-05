"""CSV-specific processing and preview helpers built on the staged-file contract."""

from __future__ import annotations

import logging

from data_processing.contracts import PreviewResultPayload, ProcessResultPayload, SchemaItem
from data_processing.services.observability import log_stage_event

from .local_files import process_local_csv_file, read_local_csv_chunks
from .preview import fetch_local_csv_preview_page
from .staging import lease_staged_s3_object


logger = logging.getLogger(__name__)


def process_staged_csv(
    client,
    bucket: str,
    object_key: str,
    overrides: dict[str, str],
    preview_row_limit: int,
    *,
    progress_callback=None,
) -> ProcessResultPayload:
    """Process a staged S3 CSV through the local chunked CSV pipeline."""

    with lease_staged_s3_object(client, bucket, object_key) as staged_file:
        if progress_callback is not None:
            progress_callback("profiling_schema", 45)
        result = process_local_csv_file(
            staged_file.path,
            overrides,
            preview_row_limit,
            progress_callback=progress_callback,
        )
        log_stage_event(
            logger,
            "processing.csv.completed",
            bucket=bucket,
            object_key=object_key,
            row_count=result["rowCount"],
        )
        return {"bucket": bucket, **result, "objectKey": object_key}


def fetch_csv_preview_page(
    client,
    bucket: str,
    object_key: str,
    *,
    schema: list[SchemaItem],
    row_count: int,
    page: int,
    page_size: int,
    preview_columns: list[str] | None = None,
) -> PreviewResultPayload:
    """Load one processed CSV preview page from a staged local file."""

    with lease_staged_s3_object(client, bucket, object_key) as staged_file:
        return fetch_local_csv_preview_page(
            staged_file.path,
            read_local_csv_chunks=read_local_csv_chunks,
            schema=schema,
            row_count=row_count,
            page=page,
            page_size=page_size,
            preview_columns=preview_columns,
        )
