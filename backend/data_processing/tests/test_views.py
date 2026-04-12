"""API-level regression tests for the data-processing endpoints."""

from unittest.mock import patch

from django.test import TestCase
from rest_framework.test import APIClient

from data_processing.models import ProcessingRun
from data_processing.services.processing import InvalidCredentialsError


class DataProcessingApiTests(TestCase):
    """Verify the public API contracts and error handling."""

    def setUp(self) -> None:
        """Prepare a reusable API client and credential payload."""

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
        """Return the file list payload from the service layer unchanged."""

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
        """Persist sanitized run metadata without echoing AWS secrets back."""

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

        with patch("data_processing.views.process_s3_object", return_value=service_result):
            response = self.client.post("/api/data/process", payload, format="json")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(ProcessingRun.objects.count(), 1)
        run = ProcessingRun.objects.get()
        self.assertEqual(run.bucket, "demo-bucket")
        self.assertEqual(run.object_key, "incoming/sample.csv")
        self.assertEqual(run.processing_metadata["durationMs"], 12.4)
        self.assertEqual(response.json()["previewPage"]["totalRows"], 2)
        self.assertNotIn("access_key_id", response.json())
        self.assertNotIn("secret_access_key", response.json())

    def test_process_endpoint_returns_service_errors(self) -> None:
        """Map service-layer credential errors to API responses."""

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
        """Surface override validation failures as client errors."""

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

    def test_preview_endpoint_returns_paginated_rows_for_existing_run(self) -> None:
        """Use the saved processing run when it is still available."""

        run = ProcessingRun.objects.create(
            bucket="demo-bucket",
            object_key="incoming/sample.csv",
            file_type="csv",
            sheet_name="",
            status="completed",
            row_count=4,
            schema=[
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
            warnings=[],
            preview_columns=["Score"],
            processing_metadata={"durationMs": 12.4, "previewRowLimit": 100, "chunkSize": 5000},
        )

        payload = {
            **self.credentials_payload,
            "run_id": run.id,
            "page": 2,
            "page_size": 2,
        }

        with patch(
            "data_processing.views.fetch_s3_preview_page",
            return_value={
                "rowCount": 4,
                "previewColumns": ["Score"],
                "previewRows": [{"Score": 85}, {"Score": 80}],
                "previewPage": {
                    "page": 2,
                    "pageSize": 2,
                    "totalRows": 4,
                    "totalPages": 2,
                    "hasPreviousPage": True,
                    "hasNextPage": False,
                },
            },
        ) as mocked_preview:
            response = self.client.post("/api/data/preview", payload, format="json")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["runId"], run.id)
        self.assertEqual(response.json()["previewPage"]["page"], 2)
        self.assertEqual(response.json()["previewRows"], [{"Score": 85}, {"Score": 80}])
        self.assertEqual(mocked_preview.call_args.kwargs["row_count"], 4)

    def test_preview_endpoint_falls_back_to_request_context_when_run_is_missing(self) -> None:
        """Keep paging working in stateless deployments when runs disappear."""

        payload = {
            **self.credentials_payload,
            "run_id": 999,
            "object_key": "incoming/sample.csv",
            "file_type": "csv",
            "selected_sheet": "",
            "row_count": 4,
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
            "preview_columns": ["Score"],
            "page": 2,
            "page_size": 2,
        }

        with patch(
            "data_processing.views.fetch_s3_preview_page",
            return_value={
                "rowCount": 4,
                "previewColumns": ["Score"],
                "previewRows": [{"Score": 85}, {"Score": 80}],
                "previewPage": {
                    "page": 2,
                    "pageSize": 2,
                    "totalRows": 4,
                    "totalPages": 2,
                    "hasPreviousPage": True,
                    "hasNextPage": False,
                },
            },
        ) as mocked_preview:
            response = self.client.post("/api/data/preview", payload, format="json")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["runId"], 999)
        self.assertEqual(mocked_preview.call_args.kwargs["object_key"], "incoming/sample.csv")
        self.assertEqual(mocked_preview.call_args.kwargs["row_count"], 4)

    def test_preview_endpoint_returns_not_found_for_missing_run(self) -> None:
        """Return 404 when neither a saved run nor preview context is available."""

        payload = {
            **self.credentials_payload,
            "run_id": 999,
            "page": 1,
            "page_size": 25,
        }

        response = self.client.post("/api/data/preview", payload, format="json")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["code"], "run_not_found")
