from unittest.mock import patch

from django.test import TestCase
from rest_framework.test import APIClient

from data_processing.models import ProcessingRun
from data_processing.services.processing import InvalidCredentialsError


class DataProcessingApiTests(TestCase):
    def setUp(self) -> None:
        self.client = APIClient()
        self.credentials_payload = {
            "access_key_id": "access",
            "secret_access_key": "secret",
            "session_token": "",
            "region": "ap-southeast-2",
            "bucket": "demo-bucket",
            "prefix": "incoming/",
        }

    def test_list_files_endpoint_returns_service_results(self) -> None:
        files = [
            {
                "key": "incoming/sample.csv",
                "size": 147,
                "lastModified": "2026-04-04T00:00:00+00:00",
                "format": "csv",
            }
        ]

        with patch("data_processing.views.list_supported_files", return_value=files) as mocked_list:
            response = self.client.post("/api/s3/files", self.credentials_payload, format="json")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["files"], files)
        credentials = mocked_list.call_args.args[0]
        self.assertEqual(credentials.bucket, "demo-bucket")
        self.assertEqual(credentials.prefix, "incoming/")

    def test_process_endpoint_persists_processing_run_without_exposing_credentials(self) -> None:
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
                    "nullable": True,
                    "confidence": 0.98,
                    "warnings": [],
                    "null_token_count": 0,
                    "sample_values": ["90", "75"],
                    "allowed_overrides": ["text", "integer", "float", "boolean", "date", "datetime", "category", "complex"],
                }
            ],
            "previewColumns": ["Score"],
            "previewRows": [{"Score": 90}, {"Score": 75}],
            "warnings": [],
            "processingMetadata": {"durationMs": 12.4, "previewRowLimit": 100, "chunkSize": 5000},
        }

        payload = {
            **self.credentials_payload,
            "object_key": "incoming/sample.csv",
            "preview_row_limit": 100,
            "overrides": [],
        }

        with patch("data_processing.views.process_s3_object", return_value=service_result):
            response = self.client.post("/api/data/process", payload, format="json")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(ProcessingRun.objects.count(), 1)
        run = ProcessingRun.objects.get()
        self.assertEqual(run.bucket, "demo-bucket")
        self.assertEqual(run.object_key, "incoming/sample.csv")
        self.assertEqual(run.processing_metadata["durationMs"], 12.4)
        self.assertNotIn("access_key_id", response.json())
        self.assertNotIn("secret_access_key", response.json())

    def test_process_endpoint_returns_service_errors(self) -> None:
        payload = {
            **self.credentials_payload,
            "object_key": "incoming/sample.csv",
            "preview_row_limit": 100,
            "overrides": [],
        }

        with patch(
            "data_processing.views.process_s3_object",
            side_effect=InvalidCredentialsError("AWS credentials could not be validated."),
        ):
            response = self.client.post("/api/data/process", payload, format="json")

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["code"], "invalid_credentials")

    def test_process_endpoint_returns_invalid_override_errors(self) -> None:
        payload = {
            **self.credentials_payload,
            "object_key": "incoming/sample.csv",
            "preview_row_limit": 100,
            "overrides": [{"column": "Score", "target_type": "date"}],
        }

        with patch(
            "data_processing.views.process_s3_object",
            side_effect=ValueError("Column 'Score' cannot be safely converted to 'date'."),
        ):
            response = self.client.post("/api/data/process", payload, format="json")

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["code"], "invalid_override")

