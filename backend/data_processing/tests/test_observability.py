"""Focused tests for backend observability helpers."""

import logging

from django.test import SimpleTestCase

from data_processing.services.observability import elapsed_ms, log_stage_event


class _CaptureHandler(logging.Handler):
    """Capture emitted log records for assertions about structured extras."""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


class ObservabilityTests(SimpleTestCase):
    """Verify the shared timing/logging helper behavior."""

    def test_log_stage_event_includes_duration_and_omits_none_fields(self) -> None:
        logger = logging.getLogger("data_processing.tests.observability")
        logger.setLevel(logging.INFO)
        logger.propagate = False
        handler = _CaptureHandler()
        logger.addHandler(handler)
        self.addCleanup(logger.removeHandler, handler)

        log_stage_event(
            logger,
            "processing.test.completed",
            duration_ms=12.5,
            run_id=7,
            task_id=None,
            object_key="incoming/sample.csv",
        )

        record = handler.records[0]
        self.assertEqual(record.msg, "processing.test.completed")
        self.assertEqual(record.duration_ms, 12.5)
        self.assertEqual(record.run_id, 7)
        self.assertEqual(record.object_key, "incoming/sample.csv")
        self.assertFalse(hasattr(record, "task_id"))

    def test_elapsed_ms_returns_non_negative_duration(self) -> None:
        duration_ms = elapsed_ms(0.0)
        self.assertGreaterEqual(duration_ms, 0.0)
