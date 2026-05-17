"""Experimental PySpark helpers for CSV schema and preview comparison."""

from __future__ import annotations

from contextlib import suppress
from time import perf_counter
from typing import Any

from .processing import (
    InvalidPreviewPageError,
    ProcessingServiceError,
    S3Credentials,
    UnsupportedFileTypeError,
    _build_preview_page_metadata,
    _get_staged_s3_object_path,
    _release_staged_file,
    build_s3_client,
    resolve_supported_file_type,
)


class SparkUnavailableError(ProcessingServiceError):
    """Raised when the local runtime cannot start PySpark."""

    status_code = 503
    code = "spark_unavailable"


def _import_spark_session():
    """Import Spark lazily so the main app stays usable without local Spark."""

    try:
        from pyspark.sql import SparkSession  # type: ignore
    except ImportError as exc:
        raise SparkUnavailableError(
            "PySpark is not installed in this environment. Install the optional Spark dependencies first."
        ) from exc
    return SparkSession


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


def _slice_spark_preview(raw_df, *, page: int, page_size: int) -> list[dict[str, Any]]:
    """Collect just the requested preview slice from a Spark DataFrame."""

    start = (page - 1) * page_size
    end = start + page_size
    rows = (
        raw_df.rdd.zipWithIndex()
        .filter(lambda item: start <= item[1] < end)
        .map(lambda item: item[0].asDict(recursive=True))
        .collect()
    )
    return [dict(row) for row in rows]


def run_spark_csv_comparison(
    *,
    credentials: S3Credentials,
    object_key: str,
    page: int = 1,
    page_size: int = 100,
) -> dict[str, Any]:
    """Stage a CSV locally, then compare its shape using a local Spark session."""

    file_type = resolve_supported_file_type(object_key)
    if file_type != "csv":
        raise UnsupportedFileTypeError("Spark comparison currently supports CSV files only.")

    client = build_s3_client(credentials)
    staged_file = _get_staged_s3_object_path(client, credentials.bucket, object_key)
    SparkSession = _import_spark_session()
    spark = None

    try:
        started = perf_counter()
        try:
            spark = (
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

        raw_df = (
            spark.read.option("header", True)
            .option("inferSchema", False)
            .csv(str(staged_file.path))
        )
        schema_df = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(str(staged_file.path))
        )
        row_count = raw_df.count()
        preview_page = _build_preview_page_metadata(row_count, page=page, page_size=page_size)
        preview_columns = list(raw_df.columns)
        preview_rows = _slice_spark_preview(raw_df, page=page, page_size=page_size)
        spark_schema = []
        for field in schema_df.schema.fields:
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
        _release_staged_file(staged_file)
