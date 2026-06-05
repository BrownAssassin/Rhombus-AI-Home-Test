"""Focused tests for background task payload serialization helpers."""

from django.test import SimpleTestCase

from data_processing.application.process_run_payloads import (
    build_credentials_from_task_payload,
    build_processing_task_request,
    build_spark_task_request,
)
from data_processing.services.processing import S3Credentials

from .support import credentials_payload


class TaskPayloadTests(SimpleTestCase):
    """Verify typed background-task payload helpers."""

    def setUp(self) -> None:
        payload = credentials_payload()
        self.credentials = S3Credentials(
            access_key_id=payload["access_key_id"],
            secret_access_key=payload["secret_access_key"],
            session_token=payload["session_token"],
            region=payload["region"],
            bucket=payload["bucket"],
            prefix=payload["prefix"],
        )

    def test_build_processing_task_request_flattens_overrides(self) -> None:
        request_payload = build_processing_task_request(
            self.credentials,
            {
                **credentials_payload(),
                "object_key": "incoming/sample.csv",
                "sheet_name": "",
                "preview_row_limit": 25,
                "overrides": [{"column": "Score", "target_type": "float"}],
            },
        )

        self.assertEqual(request_payload["object_key"], "incoming/sample.csv")
        self.assertEqual(request_payload["overrides"], {"Score": "float"})

    def test_build_spark_task_request_and_credential_rehydration_round_trip(self) -> None:
        task_payload = build_spark_task_request(
            self.credentials,
            object_key="incoming/sample.csv",
            page=2,
            page_size=50,
        )

        rehydrated = build_credentials_from_task_payload(task_payload)

        self.assertEqual(task_payload["page"], 2)
        self.assertEqual(task_payload["page_size"], 50)
        self.assertEqual(rehydrated.bucket, self.credentials.bucket)
        self.assertEqual(rehydrated.region, self.credentials.region)
