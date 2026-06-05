"""S3-facing helpers, file-type resolution, and object metadata access."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from pathlib import Path
import tempfile
from time import perf_counter

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from data_processing.contracts import SupportedFilePayload

from .errors import FileTooLargeError, InvalidCredentialsError, ProcessingServiceError, S3AccessError, UnsupportedFileTypeError


SUPPORTED_EXTENSIONS = {
    ".csv": "csv",
    ".xls": "excel",
    ".xlsx": "excel",
}
MAX_EXCEL_SIZE_BYTES = 20 * 1024 * 1024
logger = logging.getLogger(__name__)


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


def list_supported_files(credentials: S3Credentials) -> list[SupportedFilePayload]:
    """List supported CSV and Excel objects for the selected bucket/prefix."""

    client = build_s3_client(credentials)
    paginator = client.get_paginator("list_objects_v2")
    files: list[SupportedFilePayload] = []
    started = perf_counter()

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

    sorted_files = sorted(files, key=lambda item: item["key"].lower())
    logger.info(
        "processing.s3.list_supported_files.completed",
        extra={
            "bucket": credentials.bucket,
            "prefix": credentials.prefix,
            "file_count": len(sorted_files),
            "duration_ms": round((perf_counter() - started) * 1000, 2),
        },
    )
    return sorted_files


def resolve_supported_file_type(file_name: str) -> str:
    """Map a supported filename extension to the internal file-type label."""

    extension = Path(file_name).suffix.lower()
    if extension not in SUPPORTED_EXTENSIONS:
        raise UnsupportedFileTypeError("Only CSV, XLS, and XLSX files are supported.")
    return SUPPORTED_EXTENSIONS[extension]


def head_object_metadata(
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


def download_object_to_temp_file(
    client,
    bucket: str,
    object_key: str,
    *,
    metadata: S3ObjectMetadata | None = None,
    max_size_bytes: int | None = None,
) -> tuple[Path, int]:
    """Stage an S3 object to a local temp file for deterministic processing."""

    temp_file: tempfile.NamedTemporaryFile | None = None
    started = perf_counter()

    def cleanup_temp_file() -> None:
        if temp_file is None:
            return
        temp_path = Path(temp_file.name)
        temp_file.close()
        temp_path.unlink(missing_ok=True)

    try:
        resolved_metadata = metadata or head_object_metadata(
            client,
            bucket,
            object_key,
            max_size_bytes=max_size_bytes,
        )
        temp_file = tempfile.NamedTemporaryFile(suffix=Path(object_key).suffix, delete=False)
        client.download_fileobj(bucket, object_key, temp_file)
        temp_file.flush()
        temp_path = Path(temp_file.name)
        temp_file.close()
        temp_file = None
        logger.info(
            "processing.s3.download_object_to_temp_file.completed",
            extra={
                "bucket": bucket,
                "object_key": object_key,
                "content_length": resolved_metadata.content_length,
                "duration_ms": round((perf_counter() - started) * 1000, 2),
            },
        )
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
