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
    return S3Credentials(
        access_key_id=validated_data["access_key_id"],
        secret_access_key=validated_data["secret_access_key"],
        session_token=validated_data.get("session_token", ""),
        region=validated_data["region"],
        bucket=validated_data["bucket"],
        prefix=validated_data.get("prefix", ""),
    )


class HealthCheckView(APIView):
    authentication_classes = []
    permission_classes = []

    def get(self, request):
        return Response({"status": "ok"})


class S3FileListView(APIView):
    authentication_classes = []
    permission_classes = []

    def post(self, request):
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
    authentication_classes = []
    permission_classes = []

    def post(self, request):
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
    authentication_classes = []
    permission_classes = []

    def post(self, request):
        serializer = PreviewPageRequestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        validated_data = serializer.validated_data
        credentials = _build_credentials(validated_data)

        run = ProcessingRun.objects.filter(pk=validated_data["run_id"], status="completed").first()
        if run is None:
            return Response(
                {"detail": "The requested processing run could not be found.", "code": "run_not_found"},
                status=status.HTTP_404_NOT_FOUND,
            )

        try:
            preview = fetch_s3_preview_page(
                credentials=credentials,
                object_key=run.object_key,
                file_type=run.file_type,
                selected_sheet=run.sheet_name,
                schema=run.schema,
                row_count=run.row_count,
                page=validated_data["page"],
                page_size=validated_data["page_size"],
                preview_columns=run.preview_columns,
            )
        except ProcessingServiceError as exc:
            return Response(
                {"detail": str(exc), "code": exc.code},
                status=exc.status_code,
            )

        return Response(
            {
                "runId": run.id,
                "rowCount": preview["rowCount"],
                "previewColumns": preview["previewColumns"],
                "previewRows": preview["previewRows"],
                "previewPage": preview["previewPage"],
            }
        )
