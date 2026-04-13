"""Service-level regression tests for the processing pipeline."""

from datetime import datetime, timezone
from pathlib import Path
import tempfile
from unittest.mock import patch

from django.test import SimpleTestCase

import data_processing.services.processing as processing_service
from data_processing.services.processing import (
    CSV_CHUNK_SIZE,
    S3Credentials,
    clear_staged_file_cache,
    fetch_s3_preview_page,
    list_supported_files,
    process_local_file,
    process_s3_object,
)


class FakePaginator:
    """Minimal paginator stub for list_objects_v2 tests."""

    def __init__(self, pages):
        """Store the pages returned by paginate()."""

        self.pages = pages

    def paginate(self, **kwargs):
        """Return the configured paginator pages."""

        return self.pages


class FakeS3Client:
    """Small S3 client stub for staging, listing, and preview tests."""

    def __init__(self, *, objects=None, pages=None):
        """Configure fake S3 objects and paginator pages."""

        self.objects = objects or {}
        self.pages = pages or []
        self.head_calls = 0
        self.download_calls = 0

    def get_paginator(self, name: str):
        """Return the fake list_objects paginator."""

        if name != "list_objects_v2":
            raise AssertionError(f"Unexpected paginator requested: {name}")
        return FakePaginator(self.pages)

    def head_object(self, Bucket: str, Key: str):
        """Return stable object metadata for the requested fake object."""

        self.head_calls += 1
        return {"ContentLength": len(self.objects[Key]), "ETag": '"demo-etag"'}

    def download_fileobj(self, Bucket: str, Key: str, fileobj):
        """Write the requested fake object into the supplied file object."""

        self.download_calls += 1
        fileobj.write(self.objects[Key])


class ProcessingServiceTests(SimpleTestCase):
    """Verify local and S3-backed processing behavior."""

    def setUp(self) -> None:
        """Prepare clean staged-file state and shared fake credentials."""

        clear_staged_file_cache()
        self.addCleanup(clear_staged_file_cache)
        self.credentials = S3Credentials(
            access_key_id="access",
            secret_access_key="secret",
            region="ap-southeast-2",
            bucket="demo-bucket",
            prefix="incoming/",
        )

    def test_list_supported_files_filters_and_sorts_objects(self) -> None:
        """Return only supported files and keep them sorted by key."""

        fake_client = FakeS3Client(
            pages=[
                {
                    "Contents": [
                        {
                            "Key": "incoming/notes.txt",
                            "Size": 42,
                            "LastModified": datetime(2026, 4, 4, tzinfo=timezone.utc),
                        },
                        {
                            "Key": "incoming/z-last.xlsx",
                            "Size": 512,
                            "LastModified": datetime(2026, 4, 4, tzinfo=timezone.utc),
                        },
                        {
                            "Key": "incoming/a-first.csv",
                            "Size": 128,
                            "LastModified": datetime(2026, 4, 4, tzinfo=timezone.utc),
                        },
                    ]
                }
            ]
        )

        with patch("data_processing.services.processing.build_s3_client", return_value=fake_client):
            files = list_supported_files(self.credentials)

        self.assertEqual([item["key"] for item in files], ["incoming/a-first.csv", "incoming/z-last.xlsx"])
        self.assertEqual(files[0]["format"], "csv")
        self.assertEqual(files[1]["format"], "excel")

    def test_process_s3_object_returns_preview_and_processing_metadata(self) -> None:
        """Process S3 CSVs and include preview plus processing metadata."""

        fake_client = FakeS3Client(
            objects={
                "incoming/sample.csv": (
                    b"Name,Birthdate,Score,Grade\n"
                    b"Alice,1/01/1990,90,A\n"
                    b"Bob,2/02/1991,Not Available,B\n"
                )
            }
        )

        with patch("data_processing.services.processing.build_s3_client", return_value=fake_client):
            result = process_s3_object(self.credentials, "incoming/sample.csv", preview_row_limit=1)

        schema = {item["column"]: item for item in result["schema"]}
        self.assertEqual(result["fileType"], "csv")
        self.assertEqual(result["objectKey"], "incoming/sample.csv")
        self.assertEqual(result["rowCount"], 2)
        self.assertEqual(len(result["previewRows"]), 1)
        self.assertEqual(result["previewPage"]["totalPages"], 2)
        self.assertEqual(result["processingMetadata"]["chunkSize"], CSV_CHUNK_SIZE)
        self.assertEqual(schema["Score"]["inferred_type"], "integer")

    def test_process_local_file_supports_preview_limit(self) -> None:
        """Respect the requested preview-row limit for local CSVs."""

        with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, encoding="utf-8", newline="") as handle:
            handle.write("Name,Score\nAlice,90\nBob,85\nCharlie,80\n")
            csv_path = Path(handle.name)

        self.addCleanup(csv_path.unlink, missing_ok=True)

        result = process_local_file(csv_path, preview_row_limit=2)

        self.assertEqual(result["fileType"], "csv")
        self.assertEqual(result["rowCount"], 3)
        self.assertEqual(len(result["previewRows"]), 2)
        self.assertEqual(result["previewColumns"], ["Name", "Score"])

    def test_process_local_file_formats_date_and_datetime_previews_distinctly(self) -> None:
        """Render date-only overrides differently from full datetimes in previews."""

        with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, encoding="utf-8", newline="") as handle:
            handle.write("MeetingDay,OccurredAt\n1990-01-01 15:30:00,1990-01-01 15:30:00\n")
            csv_path = Path(handle.name)

        self.addCleanup(csv_path.unlink, missing_ok=True)

        result = process_local_file(
            csv_path,
            overrides={
                "MeetingDay": "date",
                "OccurredAt": "datetime",
            },
            preview_row_limit=1,
        )

        self.assertEqual(result["previewRows"][0]["MeetingDay"], "1990-01-01")
        self.assertEqual(result["previewRows"][0]["OccurredAt"], "1990-01-01T15:30:00")

    def test_fetch_s3_preview_page_uses_stored_schema_for_requested_page(self) -> None:
        """Page through processed S3 data using the provided schema context."""

        fake_client = FakeS3Client(
            objects={
                "incoming/sample.csv": (
                    b"Name,Score\n"
                    b"Alice,90\n"
                    b"Bob,85\n"
                    b"Charlie,80\n"
                    b"David,75\n"
                )
            }
        )
        schema = [
            {
                "column": "Name",
                "inferred_type": "text",
                "storage_type": "string",
                "display_type": "Text",
                "nullable": False,
                "confidence": 0.45,
                "warnings": [],
                "null_token_count": 0,
                "sample_values": ["Alice", "Bob"],
                "allowed_overrides": ["text", "integer", "float", "boolean", "date", "datetime", "category", "complex"],
            },
            {
                "column": "Score",
                "inferred_type": "integer",
                "storage_type": "Int64",
                "display_type": "Integer",
                "nullable": False,
                "confidence": 0.98,
                "warnings": [],
                "null_token_count": 0,
                "sample_values": ["90", "85"],
                "allowed_overrides": ["text", "integer", "float", "boolean", "date", "datetime", "category", "complex"],
            },
        ]

        with patch("data_processing.services.processing.build_s3_client", return_value=fake_client):
            preview = fetch_s3_preview_page(
                credentials=self.credentials,
                object_key="incoming/sample.csv",
                file_type="csv",
                selected_sheet="",
                schema=schema,
                row_count=4,
                page=2,
                page_size=2,
                preview_columns=["Name", "Score"],
            )

        self.assertEqual(preview["previewPage"]["page"], 2)
        self.assertEqual(preview["previewPage"]["totalPages"], 2)
        self.assertEqual(preview["previewRows"], [{"Name": "Charlie", "Score": 80}, {"Name": "David", "Score": 75}])

    def test_csv_staging_cache_reuses_downloaded_object_for_follow_up_preview(self) -> None:
        """Reuse one staged CSV across initial processing and later paging."""

        fake_client = FakeS3Client(
            objects={
                "incoming/sample.csv": (
                    b"Name,Score\n"
                    b"Alice,90\n"
                    b"Bob,85\n"
                    b"Charlie,80\n"
                    b"David,75\n"
                )
            }
        )

        with patch("data_processing.services.processing.build_s3_client", return_value=fake_client):
            result = process_s3_object(self.credentials, "incoming/sample.csv", preview_row_limit=2)
            preview = fetch_s3_preview_page(
                credentials=self.credentials,
                object_key="incoming/sample.csv",
                file_type="csv",
                selected_sheet="",
                schema=result["schema"],
                row_count=result["rowCount"],
                page=2,
                page_size=2,
                preview_columns=result["previewColumns"],
            )

        self.assertEqual(fake_client.download_calls, 1)
        self.assertEqual(preview["previewRows"], [{"Name": "Charlie", "Score": 80}, {"Name": "David", "Score": 75}])

    def test_csv_staging_cache_can_be_disabled_without_breaking_follow_up_preview(self) -> None:
        """Support disabling staged-file reuse without breaking preview paging."""

        fake_client = FakeS3Client(
            objects={
                "incoming/sample.csv": (
                    b"Name,Score\n"
                    b"Alice,90\n"
                    b"Bob,85\n"
                    b"Charlie,80\n"
                    b"David,75\n"
                )
            }
        )

        with (
            patch("data_processing.services.processing.build_s3_client", return_value=fake_client),
            patch.object(processing_service.STAGED_FILE_CACHE, "max_items", 0),
        ):
            result = process_s3_object(self.credentials, "incoming/sample.csv", preview_row_limit=2)
            preview = fetch_s3_preview_page(
                credentials=self.credentials,
                object_key="incoming/sample.csv",
                file_type="csv",
                selected_sheet="",
                schema=result["schema"],
                row_count=result["rowCount"],
                page=2,
                page_size=2,
                preview_columns=result["previewColumns"],
            )

        self.assertEqual(fake_client.download_calls, 2)
        self.assertEqual(preview["previewRows"], [{"Name": "Charlie", "Score": 80}, {"Name": "David", "Score": 75}])
