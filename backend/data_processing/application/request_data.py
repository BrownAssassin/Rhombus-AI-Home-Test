"""Helpers that map validated serializer data into domain-friendly shapes."""

from __future__ import annotations

from data_processing.contracts import PreviewContextPayload, PreviewRequestPayload, ProcessRequestPayload, ValidatedCredentialsPayload
from data_processing.services.processing import S3Credentials


def build_credentials(validated_data: ValidatedCredentialsPayload) -> S3Credentials:
    """Build service-layer credentials from validated request data."""

    return S3Credentials(
        access_key_id=validated_data["access_key_id"],
        secret_access_key=validated_data["secret_access_key"],
        session_token=validated_data.get("session_token", ""),
        region=validated_data["region"],
        bucket=validated_data["bucket"],
        prefix=validated_data.get("prefix", ""),
    )


def build_overrides(validated_data: ProcessRequestPayload) -> dict[str, str]:
    """Flatten validated override rows into the service-layer mapping."""

    return {item["column"]: item["target_type"] for item in validated_data.get("overrides", [])}


def build_preview_context(validated_data: PreviewRequestPayload) -> PreviewContextPayload | None:
    """Reconstruct preview context when the saved run is unavailable."""

    required_fields = ("object_key", "file_type", "row_count", "schema")
    if not all(field in validated_data for field in required_fields):
        return None

    return {
        "object_key": validated_data["object_key"],
        "file_type": validated_data["file_type"],
        "selected_sheet": validated_data.get("selected_sheet", ""),
        "row_count": validated_data["row_count"],
        "schema": validated_data["schema"],
        "preview_columns": validated_data.get("preview_columns", []),
    }
