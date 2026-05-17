"""API views for S3 browsing, processing, and preview pagination."""

from __future__ import annotations

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView
from django.utils import timezone

from .models import ProcessingRun
from .serializers import (
    ListFilesRequestSerializer,
    PreviewPageRequestSerializer,
    ProcessFileAsyncRequestSerializer,
    ProcessFileRequestSerializer,
    SparkCompareRequestSerializer,
)
from .services.processing import (
    ProcessingServiceError,
    S3Credentials,
    fetch_s3_preview_page,
    list_supported_files,
    process_s3_object,
    resolve_supported_file_type,
)
from .services.run_tracking import mark_run_failed, mark_run_queued, serialize_run
from .services.spark_processing import run_spark_csv_comparison
from .tasks import process_s3_object_async


def _build_credentials(validated_data: dict) -> S3Credentials:
    """Build service-layer credentials from validated request data."""

    return S3Credentials(
        access_key_id=validated_data["access_key_id"],
        secret_access_key=validated_data["secret_access_key"],
        session_token=validated_data.get("session_token", ""),
        region=validated_data["region"],
        bucket=validated_data["bucket"],
        prefix=validated_data.get("prefix", ""),
    )


def _build_preview_context(validated_data: dict) -> dict | None:
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


def _build_overrides(validated_data: dict) -> dict[str, str]:
    """Flatten validated override rows into the service-layer mapping."""

    return {item["column"]: item["target_type"] for item in validated_data.get("overrides", [])}


class HealthCheckView(APIView):
    """Minimal health check used by local smoke tests and Render."""

    authentication_classes = []
    permission_classes = []

    def get(self, request):
        """Return a stable liveness payload."""

        return Response({"status": "ok"})


class S3FileListView(APIView):
    """List supported files from the requested S3 bucket or prefix."""

    authentication_classes = []
    permission_classes = []

    def post(self, request):
        """Validate credentials and return supported S3 objects."""

        serializer = ListFilesRequestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        credentials = _build_credentials(serializer.validated_data)

        try:
            files = list_supported_files(credentials)
        except ProcessingServiceError as exc:
            return Response(
                {"detail": str(exc), "code": exc.code},
                status=exc.status_code,
            )

        return Response({"files": files})


class ProcessDataView(APIView):
    """Process a selected S3 object and persist sanitized run metadata."""

    authentication_classes = []
    permission_classes = []

    def post(self, request):
        """Infer the schema, persist the run, and return the first preview page."""

        serializer = ProcessFileRequestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        validated_data = serializer.validated_data
        credentials = _build_credentials(validated_data)
        overrides = _build_overrides(validated_data)

        try:
            result = process_s3_object(
                credentials=credentials,
                object_key=validated_data["object_key"],
                sheet_name=validated_data.get("sheet_name", ""),
                overrides=overrides,
                preview_row_limit=validated_data["preview_row_limit"],
            )
        except ProcessingServiceError as exc:
            return Response(
                {"detail": str(exc), "code": exc.code},
                status=exc.status_code,
            )
        except ValueError as exc:
            return Response({"detail": str(exc), "code": "invalid_override"}, status=status.HTTP_400_BAD_REQUEST)

        run = ProcessingRun.objects.create(
            bucket=result["bucket"],
            object_key=result["objectKey"],
            file_type=result["fileType"],
            sheet_name=result["selectedSheet"],
            status="completed",
            engine="pandas",
            progress_stage="completed",
            progress_percent=100,
            row_count=result["rowCount"],
            schema=result["schema"],
            warnings=result["warnings"],
            preview_columns=result["previewColumns"],
            preview_rows=result["previewRows"],
            preview_page=result["previewPage"],
            processing_metadata=result["processingMetadata"],
            started_at=timezone.now(),
            completed_at=timezone.now(),
        )

        return Response(
            {
                "runId": run.id,
                "rowCount": result["rowCount"],
                "schema": result["schema"],
                "previewColumns": result["previewColumns"],
                "previewRows": result["previewRows"],
                "previewPage": result["previewPage"],
                "warnings": result["warnings"],
                "processingMetadata": result["processingMetadata"],
                "selectedSheet": result["selectedSheet"],
                "fileType": result["fileType"],
            }
        )


class ProcessDataAsyncView(APIView):
    """Queue background processing while keeping the sync path unchanged."""

    authentication_classes = []
    permission_classes = []

    def post(self, request):
        """Validate the request, persist a queued run, and enqueue a Celery task."""

        serializer = ProcessFileAsyncRequestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        validated_data = serializer.validated_data
        credentials = _build_credentials(validated_data)
        try:
            file_type = resolve_supported_file_type(validated_data["object_key"])
        except ProcessingServiceError as exc:
            return Response({"detail": str(exc), "code": exc.code}, status=exc.status_code)

        run = ProcessingRun.objects.create(
            bucket=credentials.bucket,
            object_key=validated_data["object_key"],
            file_type=file_type,
            sheet_name=validated_data.get("sheet_name", ""),
            status="queued",
            engine="pandas",
            progress_stage="queued",
            progress_percent=0,
        )
        request_payload = {
            "access_key_id": credentials.access_key_id,
            "secret_access_key": credentials.secret_access_key,
            "session_token": credentials.session_token,
            "region": credentials.region,
            "bucket": credentials.bucket,
            "prefix": credentials.prefix,
            "object_key": validated_data["object_key"],
            "sheet_name": validated_data.get("sheet_name", ""),
            "preview_row_limit": validated_data["preview_row_limit"],
            "overrides": _build_overrides(validated_data),
        }

        try:
            task_result = process_s3_object_async.delay(run_id=run.id, request_payload=request_payload)
        except Exception:
            mark_run_failed(run, "Background processing could not be queued in the current environment.")
            return Response(
                {
                    "detail": "Background processing could not be queued in the current environment.",
                    "code": "task_queue_error",
                },
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        mark_run_queued(run, task_id=task_result.id)
        return Response(
            {
                "runId": run.id,
                "taskId": task_result.id,
                "status": run.status,
                "engine": run.engine,
            },
            status=status.HTTP_202_ACCEPTED,
        )


class RunStatusView(APIView):
    """Expose queued/background run status for frontend polling."""

    authentication_classes = []
    permission_classes = []

    def get(self, request, run_id: int):
        """Return the stored lifecycle state for the requested processing run."""

        run = ProcessingRun.objects.filter(pk=run_id).first()
        if run is None:
            return Response(
                {"detail": "The requested processing run could not be found.", "code": "run_not_found"},
                status=status.HTTP_404_NOT_FOUND,
            )
        return Response(serialize_run(run))


class SparkCompareView(APIView):
    """Run the experimental PySpark CSV comparison path."""

    authentication_classes = []
    permission_classes = []

    def post(self, request):
        """Compare CSV ingestion and preview generation through a local Spark session."""

        serializer = SparkCompareRequestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        validated_data = serializer.validated_data
        credentials = _build_credentials(validated_data)

        try:
            result = run_spark_csv_comparison(
                credentials=credentials,
                object_key=validated_data["object_key"],
                page=validated_data["page"],
                page_size=validated_data["page_size"],
            )
        except ProcessingServiceError as exc:
            return Response(
                {"detail": str(exc), "code": exc.code},
                status=exc.status_code,
            )

        return Response(result)


class PreviewPageView(APIView):
    """Load a later processed preview page for the current file context."""

    authentication_classes = []
    permission_classes = []

    def post(self, request):
        """Page through processed rows using a saved run or stateless preview context."""

        serializer = PreviewPageRequestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        validated_data = serializer.validated_data
        credentials = _build_credentials(validated_data)
        preview_context = _build_preview_context(validated_data)
        run = None
        run_id = validated_data.get("run_id")

        if run_id is not None:
            run = ProcessingRun.objects.filter(pk=run_id).first()

        if run is not None:
            if run.status != "completed":
                return Response(
                    {
                        "detail": "The requested processing run has not completed yet.",
                        "code": "run_not_completed",
                    },
                    status=status.HTTP_409_CONFLICT,
                )
            preview_context = {
                "object_key": run.object_key,
                "file_type": run.file_type,
                "selected_sheet": run.sheet_name,
                "row_count": run.row_count,
                "schema": run.schema,
                "preview_columns": run.preview_columns,
            }
            run_id = run.id
        elif preview_context is None:
            return Response(
                {"detail": "The requested processing run could not be found.", "code": "run_not_found"},
                status=status.HTTP_404_NOT_FOUND,
            )

        try:
            preview = fetch_s3_preview_page(
                credentials=credentials,
                object_key=preview_context["object_key"],
                file_type=preview_context["file_type"],
                selected_sheet=preview_context["selected_sheet"],
                schema=preview_context["schema"],
                row_count=preview_context["row_count"],
                page=validated_data["page"],
                page_size=validated_data["page_size"],
                preview_columns=preview_context["preview_columns"],
            )
        except ProcessingServiceError as exc:
            return Response(
                {"detail": str(exc), "code": exc.code},
                status=exc.status_code,
            )

        return Response(
            {
                "runId": run_id,
                "rowCount": preview["rowCount"],
                "previewColumns": preview["previewColumns"],
                "previewRows": preview["previewRows"],
                "previewPage": preview["previewPage"],
            }
        )
