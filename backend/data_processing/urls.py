"""URL routes for the data-processing API."""

from django.urls import path

from .views import (
    HealthCheckView,
    PreviewPageView,
    ProcessDataAsyncView,
    ProcessDataView,
    RunStatusView,
    S3FileListView,
    SparkCompareView,
)


urlpatterns = [
    path("health/", HealthCheckView.as_view(), name="health"),
    path("s3/files", S3FileListView.as_view(), name="list-s3-files"),
    path("data/process", ProcessDataView.as_view(), name="process-data"),
    path("data/process-async", ProcessDataAsyncView.as_view(), name="process-data-async"),
    path("data/preview", PreviewPageView.as_view(), name="preview-page"),
    path("data/runs/<int:run_id>", RunStatusView.as_view(), name="run-status"),
    path("data/spark-compare", SparkCompareView.as_view(), name="spark-compare"),
]
