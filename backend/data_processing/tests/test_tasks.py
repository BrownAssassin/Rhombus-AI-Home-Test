"""Task-level regression tests for async processing lifecycle updates."""

from unittest.mock import patch

from django.test import TestCase, override_settings

from data_processing.models import ProcessingRun
from data_processing.services.processing import InvalidCredentialsError
from data_processing.tasks import process_s3_object_async


@override_settings(CELERY_TASK_ALWAYS_EAGER=True, CELERY_TASK_EAGER_PROPAGATES=True)
class ProcessingTaskTests(TestCase):
    """Verify that background tasks keep ProcessingRun state in sync."""

    def setUp(self) -> None:
        """Create a reusable queued run and request payload."""

        self.run = ProcessingRun.objects.create(
            bucket="demo-bucket",
            object_key="incoming/sample.csv",
            file_type="csv",
            status="queued",
            engine="pandas",
            progress_stage="queued",
            progress_percent=0,
        )
        self.request_payload = {
            "access_key_id": "access",
            "secret_access_key": "secret",
            "session_token": "",
            "region": "ap-southeast-2",
            "bucket": "demo-bucket",
            "prefix": "incoming/",
            "object_key": "incoming/sample.csv",
            "sheet_name": "",
            "preview_row_limit": 25,
            "overrides": {},
        }

    def test_process_task_marks_run_completed(self) -> None:
        """Persist the completed preview payload after a successful task run."""

        service_result = {
            "bucket": "demo-bucket",
            "objectKey": "incoming/sample.csv",
            "fileType": "csv",
            "selectedSheet": "",
            "rowCount": 2,
            "schema": [
                {
                    "column": "Score",
                    "inferred_type": "integer",
                    "storage_type": "Int64",
                    "display_type": "Integer",
                    "nullable": False,
                    "confidence": 0.98,
                    "warnings": [],
                    "null_token_count": 0,
                    "sample_values": ["90", "75"],
                    "allowed_overrides": ["text", "integer", "float", "boolean", "date", "datetime", "category", "complex"],
                }
            ],
            "previewColumns": ["Score"],
            "previewRows": [{"Score": 90}, {"Score": 75}],
            "previewPage": {
                "page": 1,
                "pageSize": 25,
                "totalRows": 2,
                "totalPages": 1,
                "hasPreviousPage": False,
                "hasNextPage": False,
            },
            "warnings": [],
            "processingMetadata": {"durationMs": 12.4, "previewRowLimit": 25, "chunkSize": 500},
        }

        with patch("data_processing.tasks.process_s3_object", return_value=service_result):
            process_s3_object_async.apply(kwargs={"run_id": self.run.id, "request_payload": self.request_payload})

        self.run.refresh_from_db()
        self.assertEqual(self.run.status, "completed")
        self.assertEqual(self.run.progress_stage, "completed")
        self.assertEqual(self.run.preview_rows, [{"Score": 90}, {"Score": 75}])

    def test_process_task_marks_run_failed(self) -> None:
        """Persist terminal error details when the background task fails."""

        with patch(
            "data_processing.tasks.process_s3_object",
            side_effect=InvalidCredentialsError("AWS credentials could not be validated."),
        ):
            with self.assertRaises(InvalidCredentialsError):
                process_s3_object_async.apply(kwargs={"run_id": self.run.id, "request_payload": self.request_payload})

        self.run.refresh_from_db()
        self.assertEqual(self.run.status, "failed")
        self.assertEqual(self.run.error_message, "AWS credentials could not be validated.")
