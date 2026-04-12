"""Django app configuration for the data-processing domain."""

from django.apps import AppConfig


class DataProcessingConfig(AppConfig):
    """Register the data-processing app with Django."""

    default_auto_field = "django.db.models.BigAutoField"
    name = "data_processing"
