"""Focused tests for run query and lifecycle helper seams."""

from django.test import TestCase

from data_processing.models import ProcessingRun
from data_processing.services.run_tracking import get_active_run, mark_run_failed, mark_run_queued


class RunHelperTests(TestCase):
    """Verify focused run-store and lifecycle helper behavior."""

    def test_mark_run_queued_persists_task_metadata(self) -> None:
        run = ProcessingRun.objects.create(
            bucket="demo-bucket",
            object_key="incoming/sample.csv",
            file_type="csv",
            run_type="process",
            status="queued",
            engine="pandas",
        )

        mark_run_queued(run, task_id="task-123")
        run.refresh_from_db()

        self.assertEqual(run.task_id, "task-123")
        self.assertEqual(run.progress_stage, "queued")
        self.assertEqual(run.progress_percent, 0)

    def test_mark_run_failed_sets_terminal_state(self) -> None:
        run = ProcessingRun.objects.create(
            bucket="demo-bucket",
            object_key="incoming/sample.csv",
            file_type="csv",
            run_type="process",
            status="processing",
            engine="pandas",
        )

        mark_run_failed(run, "boom")
        run.refresh_from_db()

        self.assertEqual(run.status, "failed")
        self.assertEqual(run.error_message, "boom")
        self.assertEqual(run.progress_stage, "failed")
        self.assertEqual(run.progress_percent, 100)

    def test_get_active_run_filters_out_terminal_runs(self) -> None:
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
