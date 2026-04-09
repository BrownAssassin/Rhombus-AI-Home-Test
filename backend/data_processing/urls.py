from django.urls import path

from .views import HealthCheckView, PreviewPageView, ProcessDataView, S3FileListView


urlpatterns = [
    path("health/", HealthCheckView.as_view(), name="health"),
    path("s3/files", S3FileListView.as_view(), name="list-s3-files"),
    path("data/process", ProcessDataView.as_view(), name="process-data"),
    path("data/preview", PreviewPageView.as_view(), name="preview-page"),
]
