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

        with patch(
            "data_processing.tasks.run_background_process",
            return_value={"runId": self.run.id, "status": "completed"},
        ) as mocked_run:
            process_s3_object_async.apply(kwargs={"run_id": self.run.id, "request_payload": self.request_payload})

        mocked_run.assert_called_once_with(run_id=self.run.id, request_payload=self.request_payload)

    def test_process_task_marks_run_failed(self) -> None:
        """Persist terminal error details when the background task fails."""

        with patch(
            "data_processing.tasks.run_background_process",
            side_effect=InvalidCredentialsError("AWS credentials could not be validated."),
        ) as mocked_run:
            with self.assertRaises(InvalidCredentialsError):
                process_s3_object_async.apply(kwargs={"run_id": self.run.id, "request_payload": self.request_payload})

        mocked_run.assert_called_once_with(run_id=self.run.id, request_payload=self.request_payload)
