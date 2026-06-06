"""Typed internal payload contracts shared across the backend layers."""

from __future__ import annotations

from typing import NotRequired, TypeAlias, TypedDict


JSONScalar: TypeAlias = str | int | float | bool | None
JSONValue: TypeAlias = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]
PreviewRow: TypeAlias = dict[str, JSONValue]


class SchemaItem(TypedDict):
    """One inferred schema entry returned from the Pandas inference pipeline."""

    column: str
    inferred_type: str
    storage_type: str
    display_type: str
    nullable: bool
    confidence: float
    warnings: list[str]
    null_token_count: int
    sample_values: list[str]
    allowed_overrides: list[str]


class SupportedFilePayload(TypedDict):
    """One supported S3 object surfaced to the file browser UI."""

    key: str
    size: int
    lastModified: str | None
    format: str


class OverrideItemPayload(TypedDict):
    """One validated override row coming from the UI."""

    column: str
    target_type: str


class ValidatedCredentialsPayload(TypedDict):
    """Validated S3 credential and bucket fields shared by request shapes."""

    access_key_id: str
    secret_access_key: str
    session_token: str
    region: str
    bucket: str
    prefix: str


class ProcessRequestPayload(ValidatedCredentialsPayload):
    """Validated request payload for sync and async dataset processing."""

    object_key: str
    sheet_name: str
    preview_row_limit: int
    overrides: list[OverrideItemPayload]


class SparkCompareRequestPayload(ValidatedCredentialsPayload):
    """Validated request payload for experimental Spark comparison queueing."""

    source_run_id: int
    page: int
    page_size: int


class RunListRequestPayload(TypedDict, total=False):
    """Validated query params for recent-run listing."""

    object_key: str
    limit: int


class PreviewContextPayload(TypedDict):
    """Stateless preview context for loading later pages without a saved run."""

    object_key: str
    file_type: str
    selected_sheet: str
    row_count: int
    schema: list[SchemaItem]
    preview_columns: list[str]


class PreviewRequestPayload(ValidatedCredentialsPayload, total=False):
    """Validated request payload for loading later preview pages."""

    run_id: int
    object_key: str
    file_type: str
    selected_sheet: str
    row_count: int
    schema: list[SchemaItem]
    preview_columns: list[str]
    page: int
    page_size: int


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


class QueuedRunPayload(TypedDict, total=False):
    """Stable queueing response payload returned by async endpoints."""

    runId: int
    taskId: str
    runType: str
    status: str
    engine: str
    sourceRunId: int


class TaskExecutionPayload(TypedDict):
    """Stable background-task result payload used by Celery wrappers."""

    runId: int
    status: str


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


class RunListResponsePayload(TypedDict):
    """Stable recent-run list response returned from the application layer."""

    runs: list[RunSummaryPayload]
