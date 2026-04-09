from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
import tempfile
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
CSV_CHUNK_SIZE = 5000


class ProcessingServiceError(Exception):
    status_code = 400
    code = "processing_error"


class InvalidCredentialsError(ProcessingServiceError):
    status_code = 401
    code = "invalid_credentials"


class S3AccessError(ProcessingServiceError):
    status_code = 404
    code = "s3_access_error"


class UnsupportedFileTypeError(ProcessingServiceError):
    status_code = 400
    code = "unsupported_file_type"


class FileTooLargeError(ProcessingServiceError):
    status_code = 413
    code = "file_too_large"


class InvalidPreviewPageError(ProcessingServiceError):
    status_code = 400
    code = "invalid_preview_page"


@dataclass
class S3Credentials:
    access_key_id: str
    secret_access_key: str
    region: str
    bucket: str
    session_token: str = ""
    prefix: str = ""


def build_s3_client(credentials: S3Credentials):
    session = boto3.session.Session(
        aws_access_key_id=credentials.access_key_id,
        aws_secret_access_key=credentials.secret_access_key,
        aws_session_token=credentials.session_token or None,
        region_name=credentials.region,
    )
    return session.client("s3")


def map_client_error(exc: ClientError) -> ProcessingServiceError:
    code = exc.response.get("Error", {}).get("Code", "")
    if code in {"InvalidAccessKeyId", "SignatureDoesNotMatch", "AccessDenied", "ExpiredToken"}:
        return InvalidCredentialsError("AWS credentials could not be validated.")
    if code in {"NoSuchBucket", "NoSuchKey", "404"}:
        return S3AccessError("The requested S3 bucket or object could not be found.")
    return ProcessingServiceError(exc.response.get("Error", {}).get("Message", "An AWS error occurred."))


def list_supported_files(credentials: S3Credentials) -> list[dict[str, Any]]:
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
    extension = Path(file_name).suffix.lower()
    if extension not in SUPPORTED_EXTENSIONS:
        raise UnsupportedFileTypeError("Only CSV, XLS, and XLSX files are supported.")
    return SUPPORTED_EXTENSIONS[extension]


def build_schema_from_profiles(profiles, overrides: dict[str, str]) -> tuple[list[dict[str, Any]], list[str]]:
    schema = infer_profiles(profiles)
    schema = validate_overrides(profiles, schema, overrides)
    warnings = sorted({warning for item in schema for warning in item["warnings"]})
    return schema, warnings


def _build_preview_page_metadata(row_count: int, page: int, page_size: int) -> dict[str, Any]:
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


def _slice_dataframe_page(df: pd.DataFrame, page: int, page_size: int) -> tuple[list[str], list[dict[str, Any]]]:
    start = (page - 1) * page_size
    end = start + page_size
    page_df = df.iloc[start:end]
    return dataframe_preview(page_df, len(page_df))


def _paginate_converted_chunks(
    chunks: Iterator[pd.DataFrame],
    schema: list[dict[str, Any]],
    *,
    page: int,
    page_size: int,
) -> tuple[list[str], list[dict[str, Any]]]:
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

        converted = convert_dataframe(chunk, schema)
        local_start = max(start - seen_rows, 0)
        local_end = min(end - seen_rows, len(converted))
        if local_start < local_end:
            page_columns, chunk_rows = _slice_dataframe_page(
                converted.iloc[local_start:local_end],
                page=1,
                page_size=local_end - local_start,
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
    profiles = create_profiles(df.columns)
    update_profiles_from_dataframe(profiles, df)
    schema, warnings = build_schema_from_profiles(profiles, overrides or {})
    converted = convert_dataframe(df, schema)
    preview_columns, preview_rows = dataframe_preview(converted, preview_row_limit)
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


def _download_object_to_temp_file(client, bucket: str, object_key: str) -> tuple[tempfile.NamedTemporaryFile, int]:
    try:
        head = client.head_object(Bucket=bucket, Key=object_key)
        content_length = int(head.get("ContentLength", 0))
        if content_length > MAX_EXCEL_SIZE_BYTES:
            raise FileTooLargeError(
                f"Excel files larger than {MAX_EXCEL_SIZE_BYTES // (1024 * 1024)} MB are rejected in this MVP."
            )
        # Pandas' Excel readers expect a local file and load whole sheets into
        # memory, so we cap size early and stage the object in temp storage.
        temp_file = tempfile.NamedTemporaryFile(suffix=Path(object_key).suffix, delete=False)
        client.download_fileobj(bucket, object_key, temp_file)
        temp_file.flush()
        return temp_file, content_length
    except ClientError as exc:
        raise map_client_error(exc) from exc


def _fetch_csv_columns(client, bucket: str, object_key: str) -> list[str]:
    try:
        response = client.get_object(Bucket=bucket, Key=object_key)
        with response["Body"] as body:
            return list(pd.read_csv(body, dtype=str, keep_default_na=False, na_filter=False, nrows=0).columns)
    except ClientError as exc:
        raise map_client_error(exc) from exc
    except pd.errors.ParserError as exc:
        raise ProcessingServiceError("The selected CSV file could not be parsed.") from exc


def _read_csv_chunks(client, bucket: str, object_key: str):
    try:
        response = client.get_object(Bucket=bucket, Key=object_key)
        with response["Body"] as body:
            yield from pd.read_csv(
                body,
                dtype=str,
                keep_default_na=False,
                na_filter=False,
                chunksize=CSV_CHUNK_SIZE,
            )
    except ClientError as exc:
        raise map_client_error(exc) from exc
    except pd.errors.ParserError as exc:
        raise ProcessingServiceError("The selected CSV file could not be parsed.") from exc


def _read_local_csv_chunks(file_path: Path) -> Iterator[pd.DataFrame]:
    try:
        yield from pd.read_csv(
            file_path,
            dtype=str,
            keep_default_na=False,
            na_filter=False,
            chunksize=CSV_CHUNK_SIZE,
        )
    except pd.errors.ParserError as exc:
        raise ProcessingServiceError("The selected CSV file could not be parsed.") from exc


def _fetch_local_csv_columns(file_path: Path) -> list[str]:
    try:
        return list(pd.read_csv(file_path, dtype=str, keep_default_na=False, na_filter=False, nrows=0).columns)
    except pd.errors.ParserError as exc:
        raise ProcessingServiceError("The selected CSV file could not be parsed.") from exc


def _load_local_excel_dataframe(file_path: Path, sheet_name: str) -> tuple[pd.DataFrame, str]:
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
    preview_page = _build_preview_page_metadata(row_count, page=page, page_size=page_size)
    page_columns = preview_columns or [item["column"] for item in schema]

    client = build_s3_client(credentials)
    if row_count == 0:
        return {
            "previewColumns": page_columns,
            "previewRows": [],
            "previewPage": preview_page,
            "rowCount": row_count,
        }

    if file_type == "csv":
        page_columns, page_rows = _paginate_converted_chunks(
            _read_csv_chunks(client, credentials.bucket, object_key),
            schema,
            page=page,
            page_size=page_size,
        )
    else:
        temp_file, _ = _download_object_to_temp_file(client, credentials.bucket, object_key)
        temp_path = Path(temp_file.name)
        temp_file.close()
        try:
            df, _ = _load_local_excel_dataframe(temp_path, selected_sheet)
        finally:
            temp_path.unlink(missing_ok=True)

        converted = convert_dataframe(df, schema)
        page_columns, page_rows = _slice_dataframe_page(converted, page=page, page_size=page_size)

    return {
        "previewColumns": page_columns or preview_columns or [item["column"] for item in schema],
        "previewRows": page_rows,
        "previewPage": preview_page,
        "rowCount": row_count,
    }


def process_local_file(
    file_path: str | Path,
    *,
    sheet_name: str = "",
    overrides: dict[str, str] | None = None,
    preview_row_limit: int = 100,
) -> dict[str, Any]:
    path = Path(file_path)
    if not path.exists():
        raise ProcessingServiceError(f"Local file '{path}' does not exist.")

    file_type = resolve_supported_file_type(path.name)
    started = perf_counter()

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


def _process_csv(client, bucket: str, object_key: str, overrides: dict[str, str], preview_row_limit: int) -> dict[str, Any]:
    columns = _fetch_csv_columns(client, bucket, object_key)
    profiles = create_profiles(columns)
    row_count = 0

    for chunk in _read_csv_chunks(client, bucket, object_key):
        row_count += len(chunk)
        update_profiles_from_dataframe(profiles, chunk)

    schema, warnings = build_schema_from_profiles(profiles, overrides)

    preview_rows: list[dict[str, Any]] = []
    preview_columns: list[str] = columns
    if row_count > 0:
        # The second streaming pass keeps memory bounded while still returning
        # preview rows after inference and conversions have been finalized.
        for chunk in _read_csv_chunks(client, bucket, object_key):
            converted = convert_dataframe(chunk, schema)
            preview_columns, chunk_rows = dataframe_preview(converted, max(preview_row_limit - len(preview_rows), 0))
            preview_rows.extend(chunk_rows)
            if len(preview_rows) >= preview_row_limit:
                break

    return {
        "bucket": bucket,
        "objectKey": object_key,
        "fileType": "csv",
        "selectedSheet": "",
        "rowCount": row_count,
        "schema": schema,
        "previewColumns": preview_columns,
        "previewRows": preview_rows[:preview_row_limit],
        "previewPage": _build_preview_page_metadata(row_count, page=1, page_size=preview_row_limit),
        "warnings": warnings,
    }


def _process_excel(
    client,
    bucket: str,
    object_key: str,
    sheet_name: str,
    overrides: dict[str, str],
    preview_row_limit: int,
) -> dict[str, Any]:
    temp_file, _ = _download_object_to_temp_file(client, bucket, object_key)
    temp_path = Path(temp_file.name)
    temp_file.close()

    try:
        df, selected_sheet = _load_local_excel_dataframe(
            temp_path,
            sheet_name,
        )
    finally:
        temp_path.unlink(missing_ok=True)

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


def _process_local_csv(file_path: Path, overrides: dict[str, str], preview_row_limit: int) -> dict[str, Any]:
    columns = _fetch_local_csv_columns(file_path)
    profiles = create_profiles(columns)
    row_count = 0

    for chunk in _read_local_csv_chunks(file_path):
        row_count += len(chunk)
        update_profiles_from_dataframe(profiles, chunk)

    schema, warnings = build_schema_from_profiles(profiles, overrides)

    preview_rows: list[dict[str, Any]] = []
    preview_columns: list[str] = columns
    if row_count > 0:
        # Mirror the S3 flow locally so preview output matches the chunked
        # inference behavior users see through the web application.
        for chunk in _read_local_csv_chunks(file_path):
            converted = convert_dataframe(chunk, schema)
            preview_columns, chunk_rows = dataframe_preview(converted, max(preview_row_limit - len(preview_rows), 0))
            preview_rows.extend(chunk_rows)
            if len(preview_rows) >= preview_row_limit:
                break

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
