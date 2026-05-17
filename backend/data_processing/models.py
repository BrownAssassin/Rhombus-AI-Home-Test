"""Database models used by the data-processing application."""

from django.db import models


class ProcessingRun(models.Model):
    """Sanitized metadata for one sync or async processing request."""

    STATUS_CHOICES = [
        ("queued", "Queued"),
        ("processing", "Processing"),
        ("completed", "Completed"),
        ("failed", "Failed"),
    ]
    ENGINE_CHOICES = [
        ("pandas", "Pandas"),
        ("spark", "Spark"),
    ]

    bucket = models.CharField(max_length=255)
    object_key = models.CharField(max_length=1024)
    file_type = models.CharField(max_length=32)
    sheet_name = models.CharField(max_length=255, blank=True)
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default="queued")
    engine = models.CharField(max_length=16, choices=ENGINE_CHOICES, default="pandas")
    task_id = models.CharField(max_length=255, blank=True)
    progress_stage = models.CharField(max_length=64, blank=True)
    progress_percent = models.PositiveSmallIntegerField(default=0)
    error_message = models.TextField(blank=True)
    row_count = models.PositiveBigIntegerField(default=0)
    schema = models.JSONField(default=list)
    warnings = models.JSONField(default=list)
    preview_columns = models.JSONField(default=list)
    preview_rows = models.JSONField(default=list)
    preview_page = models.JSONField(default=dict)
    processing_metadata = models.JSONField(default=dict)
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        """Keep the newest processing runs first in the admin and API lookups."""

        ordering = ["-created_at"]

    def __str__(self) -> str:
        """Return a readable identifier for the admin and shell."""

        return f"{self.bucket}/{self.object_key} ({self.created_at:%Y-%m-%d %H:%M:%S})"
