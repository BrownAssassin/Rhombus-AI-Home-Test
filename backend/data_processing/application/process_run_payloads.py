"""Helpers for serializing and rehydrating background-task payloads."""

from __future__ import annotations

from data_processing.contracts import ProcessingTaskRequestPayload, SparkTaskRequestPayload
from .request_data import build_overrides
from data_processing.services.processing import S3Credentials


def build_processing_task_request(
    credentials: S3Credentials,
    validated_data: dict,
) -> ProcessingTaskRequestPayload:
    """Serialize the validated process request for a background task."""

    return {
        "access_key_id": credentials.access_key_id,
        "secret_access_key": credentials.secret_access_key,
        "session_token": credentials.session_token,
        "region": credentials.region,
        "bucket": credentials.bucket,
        "prefix": credentials.prefix,
        "object_key": validated_data["object_key"],
        "sheet_name": validated_data.get("sheet_name", ""),
        "preview_row_limit": validated_data["preview_row_limit"],
        "overrides": build_overrides(validated_data),
    }


def build_spark_task_request(
    credentials: S3Credentials,
    *,
    object_key: str,
    page: int,
    page_size: int,
) -> SparkTaskRequestPayload:
    """Serialize a Spark comparison request for a background task."""

    return {
        "access_key_id": credentials.access_key_id,
        "secret_access_key": credentials.secret_access_key,
        "session_token": credentials.session_token,
        "region": credentials.region,
        "bucket": credentials.bucket,
        "prefix": credentials.prefix,
        "object_key": object_key,
        "page": page,
        "page_size": page_size,
    }


def build_credentials_from_task_payload(
    payload: ProcessingTaskRequestPayload | SparkTaskRequestPayload,
) -> S3Credentials:
    """Rehydrate request-scoped S3 credentials inside a background task."""

    return S3Credentials(
        access_key_id=str(payload["access_key_id"]),
        secret_access_key=str(payload["secret_access_key"]),
        session_token=str(payload.get("session_token", "")),
        region=str(payload["region"]),
        bucket=str(payload["bucket"]),
        prefix=str(payload.get("prefix", "")),
    )
