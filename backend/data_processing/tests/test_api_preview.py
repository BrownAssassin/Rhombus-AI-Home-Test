"""API regression tests for later preview paging endpoints."""

from unittest.mock import patch

from django.test import TestCase
from rest_framework.test import APIClient

from data_processing.models import ProcessingRun

from .support import credentials_payload, sample_schema_item


class PreviewApiTests(TestCase):
    """Verify preview paging through saved runs and stateless context."""

    def setUp(self) -> None:
        self.client = APIClient()
        self.credentials_payload = credentials_payload()

    def test_preview_endpoint_returns_paginated_rows_for_existing_run(self) -> None:
        run = ProcessingRun.objects.create(
            bucket="demo-bucket",
            object_key="incoming/sample.csv",
            file_type="csv",
            sheet_name="",
            status="completed",
            row_count=4,
            schema=[sample_schema_item()],
            warnings=[],
            preview_columns=["Score"],
            preview_rows=[{"Score": 90}, {"Score": 75}],
            preview_page={
                "page": 1,
                "pageSize": 2,
                "totalRows": 4,
                "totalPages": 2,
                "hasPreviousPage": False,
                "hasNextPage": True,
            },
            processing_metadata={"durationMs": 12.4, "previewRowLimit": 100, "chunkSize": 5000},
        )

        payload = {
            **self.credentials_payload,
            "run_id": run.id,
            "page": 2,
            "page_size": 2,
        }

        with patch(
            "data_processing.views.load_preview_page",
            return_value={
                "runId": run.id,
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
        self.assertEqual(mocked_preview.call_args.args[0]["run_id"], run.id)

    def test_preview_endpoint_rejects_runs_that_are_still_processing(self) -> None:
        run = ProcessingRun.objects.create(
            bucket="demo-bucket",
            object_key="incoming/sample.csv",
            file_type="csv",
            sheet_name="",
            status="processing",
            engine="pandas",
            progress_stage="profiling_schema",
            progress_percent=45,
        )

        payload = {
            **self.credentials_payload,
            "run_id": run.id,
            "page": 1,
            "page_size": 25,
        }

        response = self.client.post("/api/data/preview", payload, format="json")

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["code"], "run_not_completed")

    def test_preview_endpoint_falls_back_to_request_context_when_run_is_missing(self) -> None:
        payload = {
            **self.credentials_payload,
            "run_id": 999,
            "object_key": "incoming/sample.csv",
            "file_type": "csv",
            "selected_sheet": "",
            "row_count": 4,
            "schema": [sample_schema_item()],
            "preview_columns": ["Score"],
            "page": 2,
            "page_size": 2,
        }

        with patch(
            "data_processing.views.load_preview_page",
            return_value={
                "runId": 999,
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
        self.assertEqual(mocked_preview.call_args.args[0]["object_key"], "incoming/sample.csv")
        self.assertEqual(mocked_preview.call_args.args[0]["row_count"], 4)

    def test_preview_endpoint_returns_not_found_for_missing_run(self) -> None:
        payload = {
            **self.credentials_payload,
            "run_id": 999,
            "page": 1,
            "page_size": 25,
        }

        response = self.client.post("/api/data/preview", payload, format="json")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["code"], "run_not_found")
