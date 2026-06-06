"""Database models used by the data-processing application."""

from django.db import models


HEAVY_RUN_PAYLOAD_FIELDS = (
    "schema",
    "warnings",
    "preview_columns",
    "preview_rows",
    "preview_page",
    "comparison_payload",
    "processing_metadata",
)


class ProcessingRunQuerySet(models.QuerySet):
    """Custom queryset helpers for common run-store access patterns."""

    def recent(self, limit: int):
        """Return the newest runs first with the requested cap."""

        return self.order_by("-created_at", "-id")[:limit]

    def for_object(self, object_key: str):
        """Restrict the queryset to one processed S3 object key."""

        return self.filter(object_key=object_key)

    def active(self):
        """Return runs that are still queued or processing."""

        return self.filter(status__in=("queued", "processing"))

    def summary_fields(self):
        """Defer heavyweight JSON payloads for jobs-tray style queries."""

        return self.defer(*HEAVY_RUN_PAYLOAD_FIELDS)

    def detail_fields(self):
        """Load the complete run plus its optional source-run relationship."""

        return self.select_related("source_run")


class ProcessingRunManager(models.Manager.from_queryset(ProcessingRunQuerySet)):
    """Default manager exposing ProcessingRunQuerySet helpers."""


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
    RUN_TYPE_CHOICES = [
        ("process", "Process"),
        ("spark_compare", "Spark Compare"),
    ]

    bucket = models.CharField(max_length=255)
    object_key = models.CharField(max_length=1024)
    file_type = models.CharField(max_length=32)
    sheet_name = models.CharField(max_length=255, blank=True)
    run_type = models.CharField(max_length=32, choices=RUN_TYPE_CHOICES, default="process")
    source_run = models.ForeignKey("self", null=True, blank=True, on_delete=models.SET_NULL)
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
    comparison_payload = models.JSONField(default=dict)
    processing_metadata = models.JSONField(default=dict)
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    objects = ProcessingRunManager()

    class Meta:
        """Keep the newest processing runs first in the admin and API lookups."""

        ordering = ["-created_at", "-id"]
        indexes = [
            models.Index(fields=["object_key", "created_at"], name="dproc_object_created_idx"),
            models.Index(fields=["status", "created_at"], name="dproc_status_created_idx"),
            models.Index(fields=["run_type", "created_at"], name="dproc_runtype_created_idx"),
            models.Index(fields=["source_run", "created_at"], name="dproc_source_created_idx"),
        ]

    def __str__(self) -> str:
        """Return a readable identifier for the admin and shell."""

        return f"{self.bucket}/{self.object_key} ({self.created_at:%Y-%m-%d %H:%M:%S})"
