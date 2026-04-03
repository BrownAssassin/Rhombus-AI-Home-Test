from django.contrib import admin

from .models import ProcessingRun


@admin.register(ProcessingRun)
class ProcessingRunAdmin(admin.ModelAdmin):
    list_display = ("id", "bucket", "object_key", "file_type", "status", "row_count", "created_at")
    search_fields = ("bucket", "object_key")
    list_filter = ("file_type", "status", "created_at")

