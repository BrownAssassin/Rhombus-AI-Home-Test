"""Experimental PySpark helpers for CSV schema and preview comparison."""

from __future__ import annotations

from contextlib import suppress
from datetime import date, datetime
from decimal import Decimal
import logging
from time import perf_counter
from typing import Any

from data_processing.contracts import PreviewRow, SparkComparisonResultPayload, SparkSchemaItem

from .processing import (
    build_preview_page_metadata,
    InvalidPreviewPageError,
    ProcessingServiceError,
    S3Credentials,
    UnsupportedFileTypeError,
    build_s3_client,
    lease_staged_s3_object,
    resolve_supported_file_type,
)


class SparkUnavailableError(ProcessingServiceError):
    """Raised when the local runtime cannot start PySpark."""

    status_code = 503
    code = "spark_unavailable"


logger = logging.getLogger(__name__)


def _import_spark_session():
    """Import Spark lazily so the main app stays usable without local Spark."""

    try:
        from pyspark.sql import SparkSession  # type: ignore
    except ImportError as exc:
        raise SparkUnavailableError(
            "PySpark is not installed in this environment. Install the optional Spark dependencies first."
        ) from exc
    return SparkSession


def _create_spark_session():
    """Create the local Spark session with the service's default config."""

    SparkSession = _import_spark_session()
    try:
        return (
            SparkSession.builder.appName("rhombus-spark-comparison")
            .master("local[*]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
    except Exception as exc:  # pragma: no cover - depends on local Spark/Java runtime
        raise SparkUnavailableError(
            "PySpark could not start. Verify that Java is installed and the Spark runtime is available."
        ) from exc


def _map_spark_type(data_type_name: str) -> tuple[str, str]:
    """Translate Spark-native types into the app's user-facing labels."""

    normalized = data_type_name.lower()
    if normalized in {"byte", "short", "int", "integer", "long", "bigint"}:
        return "integer", "Integer"
    if normalized in {"float", "double", "decimal"}:
        return "float", "Float"
    if normalized == "boolean":
        return "boolean", "Boolean"
    if normalized == "date":
        return "date", "Date"
    if normalized in {"timestamp", "timestamp_ntz"}:
        return "datetime", "DateTime"
    return "text", "Text"


def _serialize_spark_value(value: Any) -> object:
    """Normalize Spark-native values into JSON-safe primitives."""

    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def _slice_spark_preview(dataframe, *, page: int, page_size: int) -> list[PreviewRow]:
    """Collect just the requested preview slice from a Spark DataFrame."""

    start = (page - 1) * page_size
    end = start + page_size
    rows = (
        dataframe.rdd.zipWithIndex()
        .filter(lambda item: start <= item[1] < end)
        .map(lambda item: item[0].asDict(recursive=True))
        .collect()
    )
    return [{key: _serialize_spark_value(value) for key, value in row.items()} for row in rows]


def run_spark_csv_comparison(
    *,
    credentials: S3Credentials,
    object_key: str,
    page: int = 1,
    page_size: int = 100,
) -> SparkComparisonResultPayload:
    """Stage a CSV locally, then compare its shape using a local Spark session."""

    file_type = resolve_supported_file_type(object_key)
    if file_type != "csv":
        raise UnsupportedFileTypeError("Spark comparison currently supports CSV files only.")

    client = build_s3_client(credentials)
    spark = None

    try:
        started = perf_counter()
        spark = _create_spark_session()

        with lease_staged_s3_object(client, credentials.bucket, object_key) as staged_file:
            dataframe = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(str(staged_file.path))
            )
            row_count = dataframe.count()
            preview_page = build_preview_page_metadata(row_count, page=page, page_size=page_size)
            preview_columns = list(dataframe.columns)
            preview_rows = _slice_spark_preview(dataframe, page=page, page_size=page_size)
            spark_schema: list[SparkSchemaItem] = []
            for field in dataframe.schema.fields:
                mapped_type, display_type = _map_spark_type(field.dataType.simpleString())
                spark_schema.append(
                    {
                        "column": field.name,
                        "sparkType": field.dataType.simpleString(),
                        "mappedType": mapped_type,
                        "displayType": display_type,
                        "nullable": field.nullable,
                    }
                )

        duration_ms = round((perf_counter() - started) * 1000, 2)
        logger.info(
            "processing.spark.completed",
            extra={
                "bucket": credentials.bucket,
                "object_key": object_key,
                "page": page,
                "page_size": page_size,
                "row_count": row_count,
                "duration_ms": duration_ms,
            },
        )
        return {
            "engine": "spark",
            "fileType": file_type,
            "objectKey": object_key,
            "rowCount": row_count,
            "sparkSchema": spark_schema,
            "previewColumns": preview_columns,
            "previewRows": preview_rows,
            "previewPage": preview_page,
            "processingMetadata": {
                "durationMs": duration_ms,
                "pageSize": page_size,
                "sparkMaster": "local[*]",
            },
            "notes": [
                "Experimental comparison mode. The existing Pandas pipeline remains the authoritative inference path."
            ],
        }
    except InvalidPreviewPageError:
        raise
    except ProcessingServiceError:
        raise
    except Exception as exc:  # pragma: no cover - defensive Spark failure mapping
        raise SparkUnavailableError("Spark comparison failed before a preview could be generated.") from exc
    finally:
        if spark is not None:
            with suppress(Exception):
                spark.stop()
