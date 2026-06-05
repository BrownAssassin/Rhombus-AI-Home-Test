"""Focused tests for the application-layer orchestration modules."""

from types import SimpleNamespace
from unittest.mock import patch

from django.test import TestCase

from data_processing.application.errors import RunNotFoundError
from data_processing.application.preview import load_preview_page
from data_processing.application.process_runs import process_sync_run, queue_process_run
from data_processing.application.runs import get_run_status_payload
from data_processing.services.run_tracking import get_active_run, list_runs
from data_processing.models import ProcessingRun


class ApplicationLayerTests(TestCase):
    """Verify orchestration logic without routing through the DRF views."""

    def setUp(self) -> None:
        """Prepare a reusable validated request payload."""

        self.validated_data = {
            "access_key_id": "access",
            "secret_access_key": "secret",
            "session_token": "",
            "region": "ap-southeast-2",
            "bucket": "demo-bucket",
            "prefix": "incoming/",
            "object_key": "incoming/sample.csv",
            "sheet_name": "",
            "preview_row_limit": 25,
            "overrides": [],
        }

    def test_process_sync_run_persists_a_completed_processing_run(self) -> None:
        """Persist the completed sync result through the run-tracking helper."""

        service_result = {
            "bucket": "demo-bucket",
            "objectKey": "incoming/sample.csv",
            "fileType": "csv",
            "selectedSheet": "",
            "rowCount": 2,
            "schema": [],
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

        with patch("data_processing.application.process_runs.process_s3_object", return_value=service_result):
            payload = process_sync_run(self.validated_data)

        run = ProcessingRun.objects.get()
        self.assertEqual(run.status, "completed")
        self.assertEqual(run.preview_rows, [{"Score": 90}, {"Score": 75}])
        self.assertEqual(payload["runId"], run.id)

    def test_queue_process_run_creates_a_queued_run_and_stores_the_task_id(self) -> None:
        """Create the queued row before Celery begins processing it."""

        delay_callable = lambda **kwargs: SimpleNamespace(id="task-123")

        payload = queue_process_run(self.validated_data, delay_callable=delay_callable)

        run = ProcessingRun.objects.get()
        self.assertEqual(run.status, "queued")
        self.assertEqual(run.task_id, "task-123")
        self.assertEqual(payload["taskId"], "task-123")

    def test_get_run_status_payload_raises_for_missing_runs(self) -> None:
        """Surface a stable not-found error when a run disappears."""

        with self.assertRaises(RunNotFoundError):
            get_run_status_payload(999)

    def test_load_preview_page_uses_saved_run_context_when_available(self) -> None:
        """Load preview pages from the stored run instead of raw request context."""

        run = ProcessingRun.objects.create(
            bucket="demo-bucket",
            object_key="incoming/sample.csv",
            file_type="csv",
            status="completed",
            engine="pandas",
            row_count=4,
            schema=[],
            preview_columns=["Score"],
            preview_rows=[{"Score": 90}, {"Score": 75}],
            preview_page={"page": 1, "pageSize": 2, "totalRows": 4, "totalPages": 2},
        )

        with patch(
            "data_processing.application.preview.fetch_s3_preview_page",
            return_value={
                "rowCount": 4,
                "previewColumns": ["Score"],
                "previewRows": [{"Score": 85}, {"Score": 80}],
                "previewPage": {"page": 2, "pageSize": 2, "totalRows": 4, "totalPages": 2},
            },
        ) as mocked_preview:
            payload = load_preview_page(
                {
                    **self.validated_data,
                    "run_id": run.id,
                    "page": 2,
                    "page_size": 2,
                }
            )

        self.assertEqual(payload["runId"], run.id)
        self.assertEqual(payload["previewRows"], [{"Score": 85}, {"Score": 80}])
        self.assertEqual(mocked_preview.call_args.kwargs["row_count"], 4)

    def test_list_runs_defers_heavy_payload_fields_for_recent_run_queries(self) -> None:
        """Return recent runs without eagerly loading the heavyweight JSON payloads."""

        ProcessingRun.objects.create(
            bucket="demo-bucket",
            object_key="incoming/sample.csv",
            file_type="csv",
            status="completed",
            engine="pandas",
            schema=[{"column": "Score"}],
            warnings=["warning"],
            preview_columns=["Score"],
            preview_rows=[{"Score": 90}],
            preview_page={"page": 1},
            comparison_payload={"engine": "spark"},
            processing_metadata={"durationMs": 12.4},
        )

        run = list(list_runs(object_key="incoming/sample.csv", limit=1))[0]

        self.assertTrue(
            {
                "schema",
                "warnings",
                "preview_columns",
                "preview_rows",
                "preview_page",
                "comparison_payload",
                "processing_metadata",
            }.issubset(run.get_deferred_fields())
        )

    def test_get_active_run_only_returns_pending_runs(self) -> None:
        """Expose a focused helper for queued and processing run lookups."""

        completed_run = ProcessingRun.objects.create(
            bucket="demo-bucket",
            object_key="incoming/completed.csv",
            file_type="csv",
            status="completed",
            engine="pandas",
        )
        queued_run = ProcessingRun.objects.create(
            bucket="demo-bucket",
            object_key="incoming/queued.csv",
            file_type="csv",
            status="queued",
            engine="pandas",
        )

        self.assertIsNone(get_active_run(completed_run.id))
        self.assertEqual(get_active_run(queued_run.id).id, queued_run.id)
