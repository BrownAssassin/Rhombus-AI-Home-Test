"""API regression tests for run polling, listing, and Spark comparison."""

from types import SimpleNamespace
from unittest.mock import patch

from django.test import TestCase
from rest_framework.test import APIClient

from data_processing.models import ProcessingRun

from .support import credentials_payload, sample_schema_item


class RunAndSparkApiTests(TestCase):
    """Verify tracked-run endpoints and experimental Spark queueing."""

    def setUp(self) -> None:
        self.client = APIClient()
        self.credentials_payload = credentials_payload()

    def test_run_status_endpoint_returns_completed_payload(self) -> None:
        run = ProcessingRun.objects.create(
            bucket="demo-bucket",
            object_key="incoming/sample.csv",
            file_type="csv",
            sheet_name="",
            run_type="process",
            status="completed",
            engine="pandas",
            task_id="task-123",
            progress_stage="completed",
            progress_percent=100,
            row_count=2,
            schema=[sample_schema_item()],
            warnings=[],
            preview_columns=["Score"],
            preview_rows=[{"Score": 90}, {"Score": 75}],
            preview_page={
                "page": 1,
                "pageSize": 25,
                "totalRows": 2,
                "totalPages": 1,
                "hasPreviousPage": False,
                "hasNextPage": False,
            },
            processing_metadata={"durationMs": 12.4, "previewRowLimit": 25, "chunkSize": 500},
        )

        response = self.client.get(f"/api/data/runs/{run.id}")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "completed")
        self.assertEqual(response.json()["runType"], "process")
        self.assertEqual(response.json()["previewRows"], [{"Score": 90}, {"Score": 75}])

    def test_run_list_endpoint_returns_recent_runs_with_optional_filter(self) -> None:
        other_run = ProcessingRun.objects.create(
            bucket="demo-bucket",
            object_key="incoming/other.csv",
            file_type="csv",
            run_type="process",
            status="completed",
            engine="pandas",
        )
        earliest_matching_run = ProcessingRun.objects.create(
            bucket="demo-bucket",
            object_key="incoming/sample.csv",
            file_type="csv",
            run_type="spark_compare",
            status="queued",
            engine="spark",
        )
        latest_matching_run = ProcessingRun.objects.create(
            bucket="demo-bucket",
            object_key="incoming/sample.csv",
            file_type="csv",
            run_type="process",
            status="completed",
            engine="pandas",
        )

        response = self.client.get("/api/data/runs", {"object_key": "incoming/sample.csv", "limit": 2})

        self.assertEqual(response.status_code, 200)
        payload = response.json()["runs"]
        self.assertEqual([item["runId"] for item in payload], [latest_matching_run.id, earliest_matching_run.id])
        self.assertEqual(payload[0]["runType"], "process")
        self.assertEqual(payload[1]["runType"], "spark_compare")
        self.assertNotIn(other_run.id, [item["runId"] for item in payload])

    def test_spark_compare_endpoint_queues_a_comparison_job_for_completed_csv_runs(self) -> None:
        source_run = ProcessingRun.objects.create(
            bucket="demo-bucket",
            object_key="incoming/sample.csv",
            file_type="csv",
            run_type="process",
            status="completed",
            engine="pandas",
        )
        payload = {
            **self.credentials_payload,
            "source_run_id": source_run.id,
            "page": 1,
            "page_size": 25,
        }

        with patch(
            "data_processing.views.run_spark_comparison.delay",
            return_value=SimpleNamespace(id="spark-task-123"),
        ) as mocked_delay:
            response = self.client.post("/api/data/spark-compare", payload, format="json")

        self.assertEqual(response.status_code, 202)
        queued_run = ProcessingRun.objects.exclude(pk=source_run.id).get()
        self.assertEqual(queued_run.run_type, "spark_compare")
        self.assertEqual(queued_run.source_run_id, source_run.id)
        self.assertEqual(queued_run.engine, "spark")
        self.assertEqual(response.json()["sourceRunId"], source_run.id)
        self.assertEqual(mocked_delay.call_args.kwargs["request_payload"]["object_key"], "incoming/sample.csv")

    def test_spark_compare_endpoint_rejects_incomplete_source_runs(self) -> None:
        source_run = ProcessingRun.objects.create(
            bucket="demo-bucket",
            object_key="incoming/sample.csv",
            file_type="csv",
            run_type="process",
            status="processing",
            engine="pandas",
        )
        payload = {
            **self.credentials_payload,
            "source_run_id": source_run.id,
            "page": 1,
            "page_size": 25,
        }

        response = self.client.post("/api/data/spark-compare", payload, format="json")

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["code"], "source_run_not_completed")

    def test_spark_compare_endpoint_requires_a_completed_source_run_id(self) -> None:
        payload = {
            **self.credentials_payload,
            "page": 1,
            "page_size": 25,
        }

        response = self.client.post("/api/data/spark-compare", payload, format="json")

        self.assertEqual(response.status_code, 400)
        self.assertIn("source_run_id", response.json())

    def test_spark_compare_endpoint_rejects_excel_requests(self) -> None:
        source_run = ProcessingRun.objects.create(
            bucket="demo-bucket",
            object_key="incoming/sample.xlsx",
            file_type="excel",
            run_type="process",
            status="completed",
            engine="pandas",
        )
        payload = {
            **self.credentials_payload,
            "source_run_id": source_run.id,
            "page": 1,
            "page_size": 25,
        }

        response = self.client.post("/api/data/spark-compare", payload, format="json")

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["code"], "unsupported_file_type")

    def test_run_status_endpoint_returns_comparison_payload_for_completed_spark_runs(self) -> None:
        run = ProcessingRun.objects.create(
            bucket="demo-bucket",
            object_key="incoming/sample.csv",
            file_type="csv",
            run_type="spark_compare",
            status="completed",
            engine="spark",
            comparison_payload={
                "engine": "spark",
                "fileType": "csv",
                "objectKey": "incoming/sample.csv",
                "rowCount": 2,
                "sparkSchema": [],
                "previewColumns": ["Score"],
                "previewRows": [{"Score": "90"}],
                "previewPage": {
                    "page": 1,
                    "pageSize": 25,
                    "totalRows": 2,
                    "totalPages": 1,
                    "hasPreviousPage": False,
                    "hasNextPage": False,
                },
                "processingMetadata": {"durationMs": 20.0, "pageSize": 25, "sparkMaster": "local[*]"},
                "notes": ["Experimental comparison mode."],
            },
        )

        response = self.client.get(f"/api/data/runs/{run.id}")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["runType"], "spark_compare")
        self.assertIn("sparkComparison", response.json())
        self.assertNotIn("schema", response.json())

    def test_run_status_endpoint_returns_failure_details(self) -> None:
        run = ProcessingRun.objects.create(
            bucket="demo-bucket",
            object_key="incoming/sample.csv",
            file_type="csv",
            status="failed",
            engine="pandas",
            progress_stage="failed",
            progress_percent=45,
            error_message="AWS credentials could not be validated.",
        )

        response = self.client.get(f"/api/data/runs/{run.id}")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "failed")
        self.assertEqual(response.json()["errorMessage"], "AWS credentials could not be validated.")
