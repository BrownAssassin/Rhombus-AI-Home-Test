"""Processing regression tests for staged-file cache and lease behavior."""

from pathlib import Path
from unittest.mock import patch

from django.test import SimpleTestCase

import data_processing.services.processing as processing_service
from data_processing.services.processing import S3Credentials, clear_staged_file_cache, fetch_s3_preview_page, process_s3_object
from data_processing.services.processing.staging import StagedFileLease, lease_staged_s3_object

from .support import FakeS3Client


class StagingProcessingTests(SimpleTestCase):
    """Verify staged-file cache behavior and cleanup guarantees."""

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

    def test_csv_staging_cache_can_be_disabled_without_breaking_follow_up_preview(self) -> None:
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
            patch("data_processing.services.processing.pipeline.build_s3_client", return_value=fake_client),
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

    def test_lease_staged_s3_object_releases_the_file_even_after_errors(self) -> None:
        lease = StagedFileLease(path=Path("demo.csv"), content_length=12, release_when_done=True)

        with (
            patch("data_processing.services.processing.staging.get_staged_s3_object_path", return_value=lease),
            patch("data_processing.services.processing.staging.release_staged_file") as mocked_release,
        ):
            with self.assertRaises(RuntimeError):
                with lease_staged_s3_object(object(), "demo-bucket", "incoming/demo.csv"):
                    raise RuntimeError("boom")

        mocked_release.assert_called_once_with(lease)
