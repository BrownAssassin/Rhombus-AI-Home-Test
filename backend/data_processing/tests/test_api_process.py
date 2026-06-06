"""API regression tests for sync and async processing endpoints."""

from types import SimpleNamespace
from unittest.mock import patch

from django.test import TestCase
from rest_framework.test import APIClient

from data_processing.models import ProcessingRun
from data_processing.services.processing import InvalidCredentialsError

from .support import credentials_payload, sample_schema_item


class ProcessApiTests(TestCase):
    """Verify processing endpoints and their error handling."""

    def setUp(self) -> None:
        self.client = APIClient()
        self.credentials_payload = credentials_payload()

    def test_process_endpoint_persists_processing_run_without_exposing_credentials(self) -> None:
        """Persist sanitized run metadata without echoing AWS secrets back."""

        service_result = {
            "bucket": "demo-bucket",
            "objectKey": "incoming/sample.csv",
            "fileType": "csv",
            "selectedSheet": "",
            "rowCount": 2,
            "schema": [sample_schema_item()],
            "previewColumns": ["Score"],
            "previewRows": [{"Score": 90}, {"Score": 75}],
            "previewPage": {
                "page": 1,
                "pageSize": 100,
                "totalRows": 2,
                "totalPages": 1,
                "hasPreviousPage": False,
                "hasNextPage": False,
            },
            "warnings": [],
            "processingMetadata": {"durationMs": 12.4, "previewRowLimit": 100, "chunkSize": 5000},
        }

        payload = {
            **self.credentials_payload,
            "object_key": "incoming/sample.csv",
            "preview_row_limit": 100,
            "overrides": [],
        }

        with patch("data_processing.views.process_sync_run", return_value={**service_result, "runId": 1}):
            response = self.client.post("/api/data/process", payload, format="json")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["previewPage"]["totalRows"], 2)
        self.assertNotIn("access_key_id", response.json())
        self.assertNotIn("secret_access_key", response.json())

    def test_process_async_endpoint_queues_background_job(self) -> None:
        """Create a queued run and return the polling identifiers."""

        payload = {
            **self.credentials_payload,
            "object_key": "incoming/sample.csv",
            "preview_row_limit": 100,
            "overrides": [],
        }

        with patch(
            "data_processing.views.process_s3_object_async.delay",
            return_value=SimpleNamespace(id="task-123"),
        ) as mocked_delay:
            response = self.client.post("/api/data/process-async", payload, format="json")

        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.json()["taskId"], "task-123")
        self.assertEqual(response.json()["runType"], "process")
        run = ProcessingRun.objects.get()
        self.assertEqual(run.status, "queued")
        self.assertEqual(run.engine, "pandas")
        self.assertEqual(run.run_type, "process")
        self.assertEqual(run.progress_stage, "queued")
        self.assertEqual(run.task_id, "task-123")
        self.assertEqual(mocked_delay.call_args.kwargs["request_payload"]["object_key"], "incoming/sample.csv")

    def test_process_endpoint_returns_service_errors(self) -> None:
        """Map service-layer credential errors to API responses."""

        payload = {
            **self.credentials_payload,
            "object_key": "incoming/sample.csv",
            "preview_row_limit": 100,
            "overrides": [],
        }

        with patch(
            "data_processing.views.process_sync_run",
            side_effect=InvalidCredentialsError("AWS credentials could not be validated."),
        ):
            response = self.client.post("/api/data/process", payload, format="json")

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["code"], "invalid_credentials")

    def test_process_endpoint_returns_invalid_override_errors(self) -> None:
        """Surface override validation failures as client errors."""

        payload = {
            **self.credentials_payload,
            "object_key": "incoming/sample.csv",
            "preview_row_limit": 100,
            "overrides": [{"column": "Score", "target_type": "date"}],
        }

        with patch(
            "data_processing.views.process_sync_run",
            side_effect=ValueError("Column 'Score' cannot be safely converted to 'date'."),
        ):
            response = self.client.post("/api/data/process", payload, format="json")

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["code"], "invalid_override")
