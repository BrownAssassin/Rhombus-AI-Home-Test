"""Root URL configuration for the Django project."""

from django.contrib import admin
from django.urls import include, path, re_path

from .frontend import frontend_app


urlpatterns = [
    path("admin/", admin.site.urls),
    path("api/", include("data_processing.urls")),
    re_path(r"^(?!static/).*$", frontend_app),
]
