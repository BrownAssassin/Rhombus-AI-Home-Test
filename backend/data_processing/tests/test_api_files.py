"""API regression tests for supported-file browsing endpoints."""

from unittest.mock import patch

from django.test import TestCase
from rest_framework.test import APIClient

from .support import credentials_payload


class FileBrowsingApiTests(TestCase):
    """Verify the public file-browsing API contract."""

    def setUp(self) -> None:
        self.client = APIClient()
        self.credentials_payload = credentials_payload()

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

        with patch("data_processing.views.list_files", return_value=files) as mocked_list:
            response = self.client.post("/api/s3/files", self.credentials_payload, format="json")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["files"], files)
        self.assertEqual(mocked_list.call_args.args[0]["bucket"], "demo-bucket")
        self.assertEqual(mocked_list.call_args.args[0]["prefix"], "incoming/")
