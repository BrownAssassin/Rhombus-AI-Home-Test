"""API views for S3 browsing, processing, and preview pagination."""

from __future__ import annotations

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from .application.errors import ApplicationError
from .application.files import list_files
from .application.preview import load_preview_page
from .application.process_runs import process_sync_run, queue_process_run, queue_spark_comparison
from .application.runs import get_run_status_payload, list_recent_runs_payload
from .serializers import (
    ListFilesRequestSerializer,
    PreviewPageRequestSerializer,
    ProcessFileAsyncRequestSerializer,
    ProcessFileRequestSerializer,
    RunListRequestSerializer,
    SparkCompareRequestSerializer,
)
from .services.processing import ProcessingServiceError
from .tasks import process_s3_object_async, run_spark_comparison


def _error_response(exc: Exception) -> Response:
    """Map stable service and application errors into API responses."""

    if isinstance(exc, ProcessingServiceError):
        return Response({"detail": str(exc), "code": exc.code}, status=exc.status_code)
    if isinstance(exc, ApplicationError):
        return Response({"detail": str(exc), "code": exc.code}, status=exc.status_code)
    if isinstance(exc, ValueError):
        return Response({"detail": str(exc), "code": "invalid_override"}, status=status.HTTP_400_BAD_REQUEST)
    raise exc


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

        try:
            files = list_files(serializer.validated_data)
        except (ProcessingServiceError, ApplicationError, ValueError) as exc:
            return _error_response(exc)

        return Response({"files": files})


class ProcessDataView(APIView):
    """Process a selected S3 object and persist sanitized run metadata."""

    authentication_classes = []
    permission_classes = []

    def post(self, request):
        """Infer the schema, persist the run, and return the first preview page."""

        serializer = ProcessFileRequestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        try:
            payload = process_sync_run(serializer.validated_data)
        except (ProcessingServiceError, ApplicationError, ValueError) as exc:
            return _error_response(exc)

        return Response(payload)


class ProcessDataAsyncView(APIView):
    """Queue background processing while keeping the sync path unchanged."""

    authentication_classes = []
    permission_classes = []

    def post(self, request):
        """Validate the request, persist a queued run, and enqueue a Celery task."""

        serializer = ProcessFileAsyncRequestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        try:
            payload = queue_process_run(
                serializer.validated_data,
                delay_callable=process_s3_object_async.delay,
            )
        except (ProcessingServiceError, ApplicationError, ValueError) as exc:
            return _error_response(exc)

        return Response(payload, status=status.HTTP_202_ACCEPTED)


class RunStatusView(APIView):
    """Expose queued/background run status for frontend polling."""

    authentication_classes = []
    permission_classes = []

    def get(self, request, run_id: int):
        """Return the stored lifecycle state for the requested processing run."""

        try:
            payload = get_run_status_payload(run_id)
        except (ProcessingServiceError, ApplicationError, ValueError) as exc:
            return _error_response(exc)
        return Response(payload)


class RunListView(APIView):
    """Return a compact list of recent runs for the workbench jobs tray."""

    authentication_classes = []
    permission_classes = []

    def get(self, request):
        """List recent processing or comparison runs, newest first."""

        serializer = RunListRequestSerializer(data=request.query_params)
        serializer.is_valid(raise_exception=True)
        try:
            payload = list_recent_runs_payload(serializer.validated_data)
        except (ProcessingServiceError, ApplicationError, ValueError) as exc:
            return _error_response(exc)
        return Response(payload)


class SparkCompareView(APIView):
    """Run the experimental PySpark CSV comparison path."""

    authentication_classes = []
    permission_classes = []

    def post(self, request):
        """Queue a Spark comparison against a completed CSV Pandas run."""

        serializer = SparkCompareRequestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        try:
            payload = queue_spark_comparison(
                serializer.validated_data,
                delay_callable=run_spark_comparison.delay,
            )
        except (ProcessingServiceError, ApplicationError, ValueError) as exc:
            return _error_response(exc)

        return Response(payload, status=status.HTTP_202_ACCEPTED)


class PreviewPageView(APIView):
    """Load a later processed preview page for the current file context."""

    authentication_classes = []
    permission_classes = []

    def post(self, request):
        """Page through processed rows using a saved run or stateless preview context."""

        serializer = PreviewPageRequestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        try:
            payload = load_preview_page(serializer.validated_data)
        except (ProcessingServiceError, ApplicationError, ValueError) as exc:
            return _error_response(exc)

        return Response(payload)
