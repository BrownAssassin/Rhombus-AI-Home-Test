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
from .local_files import CSV_CHUNK_SIZE, load_local_excel_dataframe as _load_local_excel_dataframe
from .local_files import process_local_csv_file as _process_local_csv
from .local_files import read_local_csv_chunks as _read_local_csv_chunks
from .local_files import fetch_local_csv_columns as _fetch_local_csv_columns
from .pipeline import ProgressReporter, fetch_s3_preview_page, process_local_file, process_s3_object
from .preview import (
    build_preview_page_metadata as _build_preview_page_metadata,
    convert_preview_slice as _convert_preview_slice,
    fetch_local_csv_preview_page as _fetch_local_csv_preview_page,
    process_dataframe,
)
from .s3 import (
    MAX_EXCEL_SIZE_BYTES,
    S3Credentials,
    S3ObjectMetadata,
    build_s3_client,
    download_object_to_temp_file as _download_object_to_temp_file,
    head_object_metadata as _head_object_metadata,
    list_supported_files,
    map_client_error,
    resolve_supported_file_type,
)
from .staging import (
    STAGED_FILE_CACHE,
    StagedFileCache,
    StagedFileCacheEntry,
    StagedFileLease,
    clear_staged_file_cache,
    get_staged_s3_object_path as _get_staged_s3_object_path,
    release_staged_file as _release_staged_file,
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
    "StagedFileCache",
    "StagedFileCacheEntry",
    "StagedFileLease",
    "UnsupportedFileTypeError",
    "build_s3_client",
    "clear_staged_file_cache",
    "fetch_s3_preview_page",
    "list_supported_files",
    "map_client_error",
    "process_dataframe",
    "process_local_file",
    "process_s3_object",
    "resolve_supported_file_type",
    "_build_preview_page_metadata",
    "_convert_preview_slice",
    "_download_object_to_temp_file",
    "_fetch_local_csv_columns",
    "_fetch_local_csv_preview_page",
    "_get_staged_s3_object_path",
    "_head_object_metadata",
    "_load_local_excel_dataframe",
    "_process_local_csv",
    "_read_local_csv_chunks",
    "_release_staged_file",
]
