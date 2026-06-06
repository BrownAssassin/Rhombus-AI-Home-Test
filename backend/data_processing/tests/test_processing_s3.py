"""Processing regression tests for S3-backed file listing and preview paging."""

from datetime import timezone
from unittest.mock import patch

from django.test import SimpleTestCase

from data_processing.services.processing import (
    CSV_CHUNK_SIZE,
    S3Credentials,
    clear_staged_file_cache,
    fetch_s3_preview_page,
    list_supported_files,
    process_s3_object,
)

from .support import FakeS3Client, SAMPLE_LAST_MODIFIED, sample_schema_item


class S3ProcessingTests(SimpleTestCase):
    """Verify S3-backed processing and preview behavior."""

    def setUp(self) -> None:
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
        fake_client = FakeS3Client(
            pages=[
                {
                    "Contents": [
                        {"Key": "incoming/notes.txt", "Size": 42, "LastModified": SAMPLE_LAST_MODIFIED},
                        {"Key": "incoming/z-last.xlsx", "Size": 512, "LastModified": SAMPLE_LAST_MODIFIED},
                        {"Key": "incoming/a-first.csv", "Size": 128, "LastModified": SAMPLE_LAST_MODIFIED},
                    ]
                }
            ]
        )

        with patch("data_processing.services.processing.s3.build_s3_client", return_value=fake_client):
            files = list_supported_files(self.credentials)

        self.assertEqual([item["key"] for item in files], ["incoming/a-first.csv", "incoming/z-last.xlsx"])
        self.assertEqual(files[0]["format"], "csv")
        self.assertEqual(files[1]["format"], "excel")

    def test_process_s3_object_returns_preview_and_processing_metadata(self) -> None:
        fake_client = FakeS3Client(
            objects={
                "incoming/sample.csv": (
                    b"Name,Birthdate,Score,Grade\n"
                    b"Alice,1/01/1990,90,A\n"
                    b"Bob,2/02/1991,Not Available,B\n"
                )
            }
        )

        with patch("data_processing.services.processing.pipeline.build_s3_client", return_value=fake_client):
            result = process_s3_object(self.credentials, "incoming/sample.csv", preview_row_limit=1)

        schema = {item["column"]: item for item in result["schema"]}
        self.assertEqual(result["fileType"], "csv")
        self.assertEqual(result["objectKey"], "incoming/sample.csv")
        self.assertEqual(result["rowCount"], 2)
        self.assertEqual(len(result["previewRows"]), 1)
        self.assertEqual(result["previewPage"]["totalPages"], 2)
        self.assertEqual(result["processingMetadata"]["chunkSize"], CSV_CHUNK_SIZE)
        self.assertEqual(schema["Score"]["inferred_type"], "integer")

    def test_fetch_s3_preview_page_uses_stored_schema_for_requested_page(self) -> None:
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
                **sample_schema_item(column="Name"),
                "inferred_type": "text",
                "storage_type": "string",
                "display_type": "Text",
                "sample_values": ["Alice", "Bob"],
            },
            sample_schema_item(column="Score"),
        ]

        with patch("data_processing.services.processing.pipeline.build_s3_client", return_value=fake_client):
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

        with patch("data_processing.services.processing.pipeline.build_s3_client", return_value=fake_client):
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
