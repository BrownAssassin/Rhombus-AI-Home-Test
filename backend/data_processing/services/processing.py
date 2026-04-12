"""Shared processing services for S3-backed dataset profiling and previewing."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterator
from dataclasses import dataclass
import os
from pathlib import Path
import tempfile
import threading
import time
from time import perf_counter
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError
import pandas as pd

from .inference import (
    create_profiles,
    convert_dataframe,
    dataframe_preview,
    infer_profiles,
    update_profiles_from_dataframe,
    validate_overrides,
)


SUPPORTED_EXTENSIONS = {
    ".csv": "csv",
    ".xls": "excel",
    ".xlsx": "excel",
}
MAX_EXCEL_SIZE_BYTES = 20 * 1024 * 1024
CSV_CHUNK_SIZE = int(os.getenv("CSV_CHUNK_SIZE", "500"))
STAGED_FILE_CACHE_MAX_ITEMS = max(0, int(os.getenv("STAGED_FILE_CACHE_MAX_ITEMS", "2")))
STAGED_FILE_CACHE_TTL_SECONDS = max(0, int(os.getenv("STAGED_FILE_CACHE_TTL_SECONDS", "900")))
RESOURCE_LIMIT_MESSAGE = (
    "The selected file exceeded the available processing resources. "
    "Try a smaller preview page, a smaller file, or redeploy with more memory."
)


class ProcessingServiceError(Exception):
    """Base class for service-layer errors that map cleanly to API responses."""

    status_code = 400
    code = "processing_error"


class InvalidCredentialsError(ProcessingServiceError):
    """Raised when AWS credentials cannot access the requested bucket/object."""

    status_code = 401
    code = "invalid_credentials"


class S3AccessError(ProcessingServiceError):
    """Raised when the requested bucket, object, or sheet cannot be reached."""

    status_code = 404
    code = "s3_access_error"


class UnsupportedFileTypeError(ProcessingServiceError):
    """Raised when a file extension falls outside the supported formats."""

    status_code = 400
    code = "unsupported_file_type"


class FileTooLargeError(ProcessingServiceError):
    """Raised when an Excel file exceeds the MVP memory guardrail."""

    status_code = 413
    code = "file_too_large"


class InvalidPreviewPageError(ProcessingServiceError):
    """Raised when a preview-page request falls outside the dataset bounds."""

    status_code = 400
    code = "invalid_preview_page"


class ResourceLimitError(ProcessingServiceError):
    """Raised when the runtime exhausts memory or similar processing limits."""

    status_code = 413
    code = "resource_limit_exceeded"


@dataclass
class S3Credentials:
    """Runtime S3 credentials and bucket context supplied by the user."""

    access_key_id: str
    secret_access_key: str
    region: str
    bucket: str
    session_token: str = ""
    prefix: str = ""


@dataclass(frozen=True)
class S3ObjectMetadata:
    """Stable object metadata used to validate staged-file reuse."""

    content_length: int
    etag: str


@dataclass
class StagedFileCacheEntry:
    """Cached local copy of an S3 object plus the metadata that validated it."""

    path: Path
    content_length: int
    etag: str
    cached_at: float


@dataclass(frozen=True)
class StagedFileLease:
    """Local staged-file handle that knows whether it must be cleaned up."""

    path: Path
    content_length: int
    release_when_done: bool = False


class StagedFileCache:
    """Small disk cache for recently staged S3 objects."""

    def __init__(self, *, max_items: int, ttl_seconds: int) -> None:
        """Create a bounded cache for staged S3 files."""

        self.max_items = max_items
        self.ttl_seconds = ttl_seconds
        # A tiny disk-backed cache keeps the single-instance deployment from
        # re-downloading the same S3 object for every page change or override.
        self._entries: OrderedDict[tuple[str, str], StagedFileCacheEntry] = OrderedDict()
        self._lock = threading.Lock()

    def clear(self) -> None:
        """Remove every staged file currently tracked by the cache."""

        with self._lock:
            for entry in self._entries.values():
                entry.path.unlink(missing_ok=True)
            self._entries.clear()

    def get(self, bucket: str, object_key: str, *, metadata: S3ObjectMetadata) -> Path | None:
        """Return a still-valid staged file for the object, if one exists."""

        cache_key = (bucket, object_key)
        now = time.time()
        with self._lock:
            self._purge_expired_locked(now)
            entry = self._entries.get(cache_key)
            if entry is None:
                return None
            if (
                not entry.path.exists()
                or entry.content_length != metadata.content_length
                or entry.etag != metadata.etag
            ):
                self._remove_entry_locked(cache_key)
                return None
            self._entries.move_to_end(cache_key)
            return entry.path

    def put(self, bucket: str, object_key: str, *, metadata: S3ObjectMetadata, path: Path) -> Path:
        """Store a freshly staged file and evict older entries if needed."""

        cache_key = (bucket, object_key)
        entry = StagedFileCacheEntry(
            path=path,
            content_length=metadata.content_length,
            etag=metadata.etag,
            cached_at=time.time(),
        )
        with self._lock:
            self._purge_expired_locked(entry.cached_at)
            existing = self._entries.pop(cache_key, None)
            if existing is not None and existing.path != path:
                existing.path.unlink(missing_ok=True)
            self._entries[cache_key] = entry
            self._entries.move_to_end(cache_key)
            self._evict_overflow_locked()
        return path

    def _purge_expired_locked(self, now: float) -> None:
        """Drop expired or missing cache entries while the lock is held."""

        if self.ttl_seconds <= 0:
            while self._entries:
                self._remove_entry_locked(next(iter(self._entries)))
            return

        for cache_key in list(self._entries):
            entry = self._entries[cache_key]
            if not entry.path.exists() or now - entry.cached_at > self.ttl_seconds:
                self._remove_entry_locked(cache_key)

    def _evict_overflow_locked(self) -> None:
        """Enforce the configured max entry count while the lock is held."""

        while len(self._entries) > self.max_items:
            self._remove_entry_locked(next(iter(self._entries)))

    def _remove_entry_locked(self, cache_key: tuple[str, str]) -> None:
        """Remove one cached entry and delete its staged file if present."""

        entry = self._entries.pop(cache_key, None)
        if entry is not None:
            entry.path.unlink(missing_ok=True)


STAGED_FILE_CACHE = StagedFileCache(
    max_items=STAGED_FILE_CACHE_MAX_ITEMS,
    ttl_seconds=STAGED_FILE_CACHE_TTL_SECONDS,
)


def build_s3_client(credentials: S3Credentials):
    """Build a boto3 client from request-scoped credentials."""

    session = boto3.session.Session(
        aws_access_key_id=credentials.access_key_id,
        aws_secret_access_key=credentials.secret_access_key,
        aws_session_token=credentials.session_token or None,
        region_name=credentials.region,
    )
    return session.client("s3")


def map_client_error(exc: ClientError) -> ProcessingServiceError:
    """Translate AWS client errors into stable API-facing service errors."""

    code = exc.response.get("Error", {}).get("Code", "")
    if code in {"InvalidAccessKeyId", "SignatureDoesNotMatch", "AccessDenied", "ExpiredToken"}:
        return InvalidCredentialsError("AWS credentials could not be validated.")
    if code in {"NoSuchBucket", "NoSuchKey", "404"}:
        return S3AccessError("The requested S3 bucket or object could not be found.")
    return ProcessingServiceError(exc.response.get("Error", {}).get("Message", "An AWS error occurred."))


def list_supported_files(credentials: S3Credentials) -> list[dict[str, Any]]:
    """List supported CSV and Excel objects for the selected bucket/prefix."""

    client = build_s3_client(credentials)
    paginator = client.get_paginator("list_objects_v2")
    files: list[dict[str, Any]] = []

    try:
        pages = paginator.paginate(Bucket=credentials.bucket, Prefix=credentials.prefix or "")
        for page in pages:
            for item in page.get("Contents", []):
                key = item["Key"]
                extension = Path(key).suffix.lower()
                if extension not in SUPPORTED_EXTENSIONS:
                    continue
                files.append(
                    {
                        "key": key,
                        "size": item.get("Size", 0),
                        "lastModified": item.get("LastModified").isoformat() if item.get("LastModified") else None,
                        "format": SUPPORTED_EXTENSIONS[extension],
                    }
                )
    except ClientError as exc:
        raise map_client_error(exc) from exc
    except BotoCoreError as exc:
        raise ProcessingServiceError("Unable to communicate with S3.") from exc

    return sorted(files, key=lambda item: item["key"].lower())


def resolve_supported_file_type(file_name: str) -> str:
    """Map a supported filename extension to the internal file-type label."""

    extension = Path(file_name).suffix.lower()
    if extension not in SUPPORTED_EXTENSIONS:
        raise UnsupportedFileTypeError("Only CSV, XLS, and XLSX files are supported.")
    return SUPPORTED_EXTENSIONS[extension]


def build_schema_from_profiles(profiles, overrides: dict[str, str]) -> tuple[list[dict[str, Any]], list[str]]:
    """Infer the schema and flatten any per-column warnings into one list."""

    schema = infer_profiles(profiles)
    schema = validate_overrides(profiles, schema, overrides)
    warnings = sorted({warning for item in schema for warning in item["warnings"]})
    return schema, warnings


def clear_staged_file_cache() -> None:
    """Remove any staged S3 files kept on disk for reuse."""

    STAGED_FILE_CACHE.clear()


def _build_preview_page_metadata(row_count: int, page: int, page_size: int) -> dict[str, Any]:
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


def _convert_preview_slice(
    df: pd.DataFrame,
    schema: list[dict[str, Any]],
    *,
    limit: int,
) -> tuple[list[str], list[dict[str, Any]]]:
    """Convert only the rows needed for the current preview slice."""

    if limit <= 0 or df.empty:
        return [item["column"] for item in schema], []

    preview_frame = df.iloc[:limit]
    converted_preview = convert_dataframe(preview_frame, schema)
    return dataframe_preview(converted_preview, len(converted_preview), schema=schema)


def _capture_preview_frame(
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
    # Keep only the raw rows needed for the first preview page so we can apply
    # the finalized schema without paying for a second CSV download and parse.
    preview_frames.append(chunk.iloc[:remaining].copy())
    return collected_rows + min(len(chunk), remaining)


def _paginate_converted_chunks(
    chunks: Iterator[pd.DataFrame],
    schema: list[dict[str, Any]],
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
            page_columns, chunk_rows = _convert_preview_slice(
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
) -> dict[str, Any]:
    """Process an in-memory dataframe and return schema plus preview payloads."""

    profiles = create_profiles(df.columns)
    update_profiles_from_dataframe(profiles, df)
    schema, warnings = build_schema_from_profiles(profiles, overrides or {})
    preview_columns, preview_rows = _convert_preview_slice(df, schema, limit=preview_row_limit)
    preview_page = _build_preview_page_metadata(len(df), page=1, page_size=preview_row_limit)

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


def _head_object_metadata(
    client,
    bucket: str,
    object_key: str,
    *,
    max_size_bytes: int | None = None,
) -> S3ObjectMetadata:
    """Load object metadata and enforce optional size limits before staging."""

    try:
        head = client.head_object(Bucket=bucket, Key=object_key)
    except ClientError as exc:
        raise map_client_error(exc) from exc
    except BotoCoreError as exc:
        raise ProcessingServiceError("Unable to communicate with S3.") from exc

    content_length = int(head.get("ContentLength", 0))
    if max_size_bytes is not None and content_length > max_size_bytes:
        raise FileTooLargeError(
            f"Excel files larger than {max_size_bytes // (1024 * 1024)} MB are rejected in this MVP."
        )

    return S3ObjectMetadata(
        content_length=content_length,
        etag=str(head.get("ETag", "")).strip('"'),
    )


def _download_object_to_temp_file(
    client,
    bucket: str,
    object_key: str,
    *,
    metadata: S3ObjectMetadata | None = None,
    max_size_bytes: int | None = None,
) -> tuple[Path, int]:
    """Stage an S3 object to a local temp file for deterministic processing."""

    temp_file: tempfile.NamedTemporaryFile | None = None

    def cleanup_temp_file() -> None:
        if temp_file is None:
            return
        temp_path = Path(temp_file.name)
        temp_file.close()
        temp_path.unlink(missing_ok=True)

    try:
        resolved_metadata = metadata or _head_object_metadata(
            client,
            bucket,
            object_key,
            max_size_bytes=max_size_bytes,
        )
        # Staging S3 objects on disk keeps the processing path deterministic and
        # lets the chunked local readers handle larger CSVs without relying on a
        # long-lived streaming response body.
        temp_file = tempfile.NamedTemporaryFile(suffix=Path(object_key).suffix, delete=False)
        client.download_fileobj(bucket, object_key, temp_file)
        temp_file.flush()
        temp_path = Path(temp_file.name)
        temp_file.close()
        temp_file = None
        return temp_path, resolved_metadata.content_length
    except ClientError as exc:
        cleanup_temp_file()
        raise map_client_error(exc) from exc
    except BotoCoreError as exc:
        cleanup_temp_file()
        raise ProcessingServiceError("Unable to communicate with S3.") from exc
    except Exception:
        cleanup_temp_file()
        raise


def _get_staged_s3_object_path(
    client,
    bucket: str,
    object_key: str,
    *,
    max_size_bytes: int | None = None,
) -> StagedFileLease:
    """Return a staged-file lease, reusing the cache only when enabled."""

    metadata = _head_object_metadata(
        client,
        bucket,
        object_key,
        max_size_bytes=max_size_bytes,
    )
    if STAGED_FILE_CACHE.max_items <= 0:
        temp_path, _ = _download_object_to_temp_file(
            client,
            bucket,
            object_key,
            metadata=metadata,
        )
        return StagedFileLease(
            path=temp_path,
            content_length=metadata.content_length,
            release_when_done=True,
        )

    cached_path = STAGED_FILE_CACHE.get(bucket, object_key, metadata=metadata)
    if cached_path is not None:
        return StagedFileLease(path=cached_path, content_length=metadata.content_length)

    temp_path, _ = _download_object_to_temp_file(
        client,
        bucket,
        object_key,
        metadata=metadata,
    )
    cached_path = STAGED_FILE_CACHE.put(bucket, object_key, metadata=metadata, path=temp_path)
    return StagedFileLease(path=cached_path, content_length=metadata.content_length)


def _release_staged_file(lease: StagedFileLease) -> None:
    """Clean up request-scoped staged files when cache reuse is disabled."""

    if lease.release_when_done:
        lease.path.unlink(missing_ok=True)


def _read_local_csv_chunks(file_path: Path) -> Iterator[pd.DataFrame]:
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


def _fetch_local_csv_columns(file_path: Path) -> list[str]:
    """Read just the header row to preserve source column ordering."""

    try:
        return list(pd.read_csv(file_path, dtype=str, keep_default_na=False, na_filter=False, nrows=0).columns)
    except MemoryError as exc:
        raise ResourceLimitError(RESOURCE_LIMIT_MESSAGE) from exc
    except pd.errors.ParserError as exc:
        raise ProcessingServiceError("The selected CSV file could not be parsed.") from exc


def _fetch_local_csv_preview_page(
    file_path: Path,
    *,
    schema: list[dict[str, Any]],
    row_count: int,
    page: int,
    page_size: int,
    preview_columns: list[str] | None = None,
) -> dict[str, Any]:
    """Load one processed CSV preview page from a staged local file."""

    preview_page = _build_preview_page_metadata(row_count, page=page, page_size=page_size)
    page_columns = preview_columns or [item["column"] for item in schema]
    if row_count == 0:
        return {
            "previewColumns": page_columns,
            "previewRows": [],
            "previewPage": preview_page,
            "rowCount": row_count,
        }

    page_columns, page_rows = _paginate_converted_chunks(
        _read_local_csv_chunks(file_path),
        schema,
        page=page,
        page_size=page_size,
    )
    return {
        "previewColumns": page_columns or preview_columns or [item["column"] for item in schema],
        "previewRows": page_rows,
        "previewPage": preview_page,
        "rowCount": row_count,
    }


def _load_local_excel_dataframe(file_path: Path, sheet_name: str) -> tuple[pd.DataFrame, str]:
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


def process_s3_object(
    credentials: S3Credentials,
    object_key: str,
    sheet_name: str = "",
    overrides: dict[str, str] | None = None,
    preview_row_limit: int = 100,
) -> dict[str, Any]:
    """Process an S3 object and return the first preview page plus schema."""

    try:
        file_type = resolve_supported_file_type(object_key)

        client = build_s3_client(credentials)
        started = perf_counter()

        if file_type == "csv":
            result = _process_csv(client, credentials.bucket, object_key, overrides or {}, preview_row_limit)
        else:
            result = _process_excel(client, credentials.bucket, object_key, sheet_name, overrides or {}, preview_row_limit)

        duration_ms = round((perf_counter() - started) * 1000, 2)
        result["processingMetadata"] = {
            "durationMs": duration_ms,
            "previewRowLimit": preview_row_limit,
            "chunkSize": CSV_CHUNK_SIZE if file_type == "csv" else None,
        }
        return result
    except MemoryError as exc:
        raise ResourceLimitError(RESOURCE_LIMIT_MESSAGE) from exc


def fetch_s3_preview_page(
    *,
    credentials: S3Credentials,
    object_key: str,
    file_type: str,
    selected_sheet: str,
    schema: list[dict[str, Any]],
    row_count: int,
    page: int,
    page_size: int,
    preview_columns: list[str] | None = None,
) -> dict[str, Any]:
    """Fetch a later processed preview page for the current file context."""

    try:
        client = build_s3_client(credentials)
        if file_type == "csv":
            staged_file = _get_staged_s3_object_path(client, credentials.bucket, object_key)
            try:
                return _fetch_local_csv_preview_page(
                    staged_file.path,
                    schema=schema,
                    row_count=row_count,
                    page=page,
                    page_size=page_size,
                    preview_columns=preview_columns,
                )
            finally:
                _release_staged_file(staged_file)
        else:
            staged_file = _get_staged_s3_object_path(
                client,
                credentials.bucket,
                object_key,
                max_size_bytes=MAX_EXCEL_SIZE_BYTES,
            )
            try:
                df, _ = _load_local_excel_dataframe(staged_file.path, selected_sheet)

                preview_page = _build_preview_page_metadata(row_count, page=page, page_size=page_size)
                page_columns, page_rows = _convert_preview_slice(
                    df.iloc[(page - 1) * page_size : page * page_size],
                    schema,
                    limit=page_size,
                )
            finally:
                _release_staged_file(staged_file)

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
) -> dict[str, Any]:
    """Process a local CSV or Excel file through the shared service layer."""

    path = Path(file_path)
    if not path.exists():
        raise ProcessingServiceError(f"Local file '{path}' does not exist.")

    file_type = resolve_supported_file_type(path.name)
    started = perf_counter()

    try:
        if file_type == "csv":
            result = _process_local_csv(path, overrides or {}, preview_row_limit)
        else:
            df, selected_sheet = _load_local_excel_dataframe(path, sheet_name)
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
        }
        return result
    except MemoryError as exc:
        raise ResourceLimitError(RESOURCE_LIMIT_MESSAGE) from exc


def _process_csv(client, bucket: str, object_key: str, overrides: dict[str, str], preview_row_limit: int) -> dict[str, Any]:
    """Process a staged S3 CSV through the local chunked CSV pipeline."""

    staged_file = _get_staged_s3_object_path(client, bucket, object_key)
    try:
        result = _process_local_csv(staged_file.path, overrides, preview_row_limit)
        return {
            "bucket": bucket,
            "objectKey": object_key,
            **result,
        }
    finally:
        _release_staged_file(staged_file)


def _process_excel(
    client,
    bucket: str,
    object_key: str,
    sheet_name: str,
    overrides: dict[str, str],
    preview_row_limit: int,
) -> dict[str, Any]:
    """Process a staged S3 Excel file through the in-memory Excel path."""

    staged_file = _get_staged_s3_object_path(
        client,
        bucket,
        object_key,
        max_size_bytes=MAX_EXCEL_SIZE_BYTES,
    )
    try:
        df, selected_sheet = _load_local_excel_dataframe(
            staged_file.path,
            sheet_name,
        )

        result = process_dataframe(
            df,
            overrides=overrides,
            preview_row_limit=preview_row_limit,
            file_type="excel",
            object_key=object_key,
            selected_sheet=selected_sheet,
        )
        return {
            "bucket": bucket,
            **result,
        }
    finally:
        _release_staged_file(staged_file)


def _process_local_csv(file_path: Path, overrides: dict[str, str], preview_row_limit: int) -> dict[str, Any]:
    """Infer schema from a local CSV while keeping preview work bounded."""

    columns = _fetch_local_csv_columns(file_path)
    profiles = create_profiles(columns)
    row_count = 0
    preview_frames: list[pd.DataFrame] = []
    collected_preview_rows = 0

    for chunk in _read_local_csv_chunks(file_path):
        row_count += len(chunk)
        update_profiles_from_dataframe(profiles, chunk)
        collected_preview_rows = _capture_preview_frame(
            preview_frames,
            chunk,
            collected_rows=collected_preview_rows,
            preview_row_limit=preview_row_limit,
        )

    schema, warnings = build_schema_from_profiles(profiles, overrides)

    preview_rows: list[dict[str, Any]] = []
    preview_columns: list[str] = columns
    if preview_frames:
        preview_source = pd.concat(preview_frames, ignore_index=True)
        preview_columns, preview_rows = _convert_preview_slice(preview_source, schema, limit=preview_row_limit)

    return {
        "objectKey": str(file_path),
        "fileType": "csv",
        "selectedSheet": "",
        "rowCount": row_count,
        "schema": schema,
        "previewColumns": preview_columns,
        "previewRows": preview_rows[:preview_row_limit],
        "previewPage": _build_preview_page_metadata(row_count, page=1, page_size=preview_row_limit),
        "warnings": warnings,
    }
