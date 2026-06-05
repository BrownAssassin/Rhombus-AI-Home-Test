"""Shared processing services for S3-backed dataset profiling and previewing."""

from .errors import (
    FileTooLargeError,
    InvalidCredentialsError,
    InvalidPreviewPageError,
    ProcessingServiceError,
    ResourceLimitError,
    RESOURCE_LIMIT_MESSAGE,
    S3AccessError,
    UnsupportedFileTypeError,
)
from .local_files import CSV_CHUNK_SIZE
from .pipeline import ProgressReporter, fetch_s3_preview_page, process_local_file, process_s3_object
from .preview import (
    build_preview_page_metadata,
    process_dataframe,
)
from .s3 import (
    MAX_EXCEL_SIZE_BYTES,
    S3Credentials,
    S3ObjectMetadata,
    build_s3_client,
    list_supported_files,
    map_client_error,
    resolve_supported_file_type,
)
from .staging import (
    STAGED_FILE_CACHE,
    STAGED_FILE_CACHE_CONFIG,
    StagedFileCache,
    StagedFileCacheEntry,
    StagedFileCacheConfig,
    StagedFileLease,
    clear_staged_file_cache,
    lease_staged_s3_object,
)

__all__ = [
    "CSV_CHUNK_SIZE",
    "FileTooLargeError",
    "InvalidCredentialsError",
    "InvalidPreviewPageError",
    "MAX_EXCEL_SIZE_BYTES",
    "ProcessingServiceError",
    "ProgressReporter",
    "RESOURCE_LIMIT_MESSAGE",
    "ResourceLimitError",
    "S3AccessError",
    "S3Credentials",
    "S3ObjectMetadata",
    "STAGED_FILE_CACHE",
    "STAGED_FILE_CACHE_CONFIG",
    "StagedFileCache",
    "StagedFileCacheEntry",
    "StagedFileCacheConfig",
    "StagedFileLease",
    "UnsupportedFileTypeError",
    "build_s3_client",
    "build_preview_page_metadata",
    "clear_staged_file_cache",
    "fetch_s3_preview_page",
    "lease_staged_s3_object",
    "list_supported_files",
    "map_client_error",
    "process_dataframe",
    "process_local_file",
    "process_s3_object",
    "resolve_supported_file_type",
]
