"""Stable service-layer errors for processing and preview workflows."""

from __future__ import annotations


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
