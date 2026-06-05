"""Typed internal payload contracts shared across the backend layers."""

from __future__ import annotations

from typing import Any, NotRequired, TypedDict


SchemaItem = dict[str, Any]
PreviewRow = dict[str, object]


class PreviewPagePayload(TypedDict):
    """Preview-page metadata returned to the UI."""

    page: int
    pageSize: int
    totalRows: int
    totalPages: int
    hasPreviousPage: bool
    hasNextPage: bool


class ProcessingMetadataPayload(TypedDict, total=False):
    """Timing and runtime metadata attached to processed results."""

    durationMs: float
    previewRowLimit: int
    chunkSize: int | None
    appliedOverrides: dict[str, str]
    pageSize: int
    sparkMaster: str


class ProcessResultPayload(TypedDict):
    """Full Pandas processing result payload used across sync and async paths."""

    bucket: str
    objectKey: str
    fileType: str
    selectedSheet: str
    rowCount: int
    schema: list[SchemaItem]
    previewColumns: list[str]
    previewRows: list[PreviewRow]
    previewPage: PreviewPagePayload
    warnings: list[str]
    processingMetadata: ProcessingMetadataPayload


class PreviewResultPayload(TypedDict):
    """Later preview-page payload used by the preview endpoint."""

    rowCount: int
    previewColumns: list[str]
    previewRows: list[PreviewRow]
    previewPage: PreviewPagePayload


class ProcessResponsePayload(ProcessResultPayload):
    """Process endpoint response payload with the persisted run id."""

    runId: int


class PreviewResponsePayload(PreviewResultPayload):
    """Preview endpoint response payload with the originating run id when known."""

    runId: int | None


class SparkSchemaItem(TypedDict):
    """One Spark-native schema entry exposed to the comparison UI."""

    column: str
    sparkType: str
    mappedType: str
    displayType: str
    nullable: bool


class SparkComparisonResultPayload(TypedDict):
    """Full Spark comparison payload stored on completed comparison runs."""

    engine: str
    fileType: str
    objectKey: str
    rowCount: int
    sparkSchema: list[SparkSchemaItem]
    previewColumns: list[str]
    previewRows: list[PreviewRow]
    previewPage: PreviewPagePayload
    processingMetadata: ProcessingMetadataPayload
    notes: list[str]


class ProcessingTaskRequestPayload(TypedDict):
    """Serialized background-task request payload for Pandas processing."""

    access_key_id: str
    secret_access_key: str
    session_token: str
    region: str
    bucket: str
    prefix: str
    object_key: str
    sheet_name: str
    preview_row_limit: int
    overrides: dict[str, str]


class SparkTaskRequestPayload(TypedDict):
    """Serialized background-task request payload for Spark comparison."""

    access_key_id: str
    secret_access_key: str
    session_token: str
    region: str
    bucket: str
    prefix: str
    object_key: str
    page: int
    page_size: int


class RunSummaryPayload(TypedDict):
    """Run payload returned by jobs-tray style listing endpoints."""

    runId: int
    taskId: str
    runType: str
    sourceRunId: int | None
    status: str
    engine: str
    bucket: str
    objectKey: str
    progressStage: str
    progressPercent: int
    errorMessage: str
    createdAt: str
    startedAt: str | None
    completedAt: str | None
    fileType: str
    selectedSheet: str


class RunDetailPayload(RunSummaryPayload, total=False):
    """Run payload returned by run-status/detail endpoints."""

    rowCount: int
    schema: list[SchemaItem]
    previewColumns: list[str]
    previewRows: list[PreviewRow]
    previewPage: PreviewPagePayload
    warnings: list[str]
    processingMetadata: ProcessingMetadataPayload
    sparkComparison: SparkComparisonResultPayload
