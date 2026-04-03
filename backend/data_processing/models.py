from django.db import models


class ProcessingRun(models.Model):
    STATUS_CHOICES = [
        ("completed", "Completed"),
        ("failed", "Failed"),
    ]

    bucket = models.CharField(max_length=255)
    object_key = models.CharField(max_length=1024)
    file_type = models.CharField(max_length=32)
    sheet_name = models.CharField(max_length=255, blank=True)
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default="completed")
    row_count = models.PositiveBigIntegerField(default=0)
    schema = models.JSONField(default=list)
    warnings = models.JSONField(default=list)
    preview_columns = models.JSONField(default=list)
    processing_metadata = models.JSONField(default=dict)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self) -> str:
        return f"{self.bucket}/{self.object_key} ({self.created_at:%Y-%m-%d %H:%M:%S})"

