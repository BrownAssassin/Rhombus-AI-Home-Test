"""API views for S3 browsing, processing, and preview pagination."""

from __future__ import annotations

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from .models import ProcessingRun
from .serializers import (
    ListFilesRequestSerializer,
    PreviewPageRequestSerializer,
    ProcessFileRequestSerializer,
)
from .services.processing import (
    ProcessingServiceError,
    S3Credentials,
    fetch_s3_preview_page,
    list_supported_files,
    process_s3_object,
)


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
        overrides = {
            item["column"]: item["target_type"]
            for item in validated_data.get("overrides", [])
        }

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
            row_count=result["rowCount"],
            schema=result["schema"],
            warnings=result["warnings"],
            preview_columns=result["previewColumns"],
            processing_metadata=result["processingMetadata"],
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
            run = ProcessingRun.objects.filter(pk=run_id, status="completed").first()

        if run is not None:
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
