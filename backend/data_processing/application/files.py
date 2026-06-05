"""Application-layer orchestration for supported-file browsing."""

from __future__ import annotations

from data_processing.contracts import SupportedFilePayload, ValidatedCredentialsPayload
from .request_data import build_credentials
from data_processing.services.processing import list_supported_files


def list_files(validated_data: ValidatedCredentialsPayload) -> list[SupportedFilePayload]:
    """List supported files for the provided S3 bucket context."""

    return list_supported_files(build_credentials(validated_data))
