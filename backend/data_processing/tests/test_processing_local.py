"""Processing regression tests for local-file and preview-metadata behavior."""

import tempfile
from pathlib import Path

from django.test import SimpleTestCase

from data_processing.services.processing import InvalidPreviewPageError, clear_staged_file_cache, process_local_file
from data_processing.services.processing.preview import build_preview_page_metadata


class LocalProcessingTests(SimpleTestCase):
    """Verify local CSV processing and preview metadata behavior."""

    def setUp(self) -> None:
        clear_staged_file_cache()
        self.addCleanup(clear_staged_file_cache)

    def test_process_local_file_supports_preview_limit(self) -> None:
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

    def test_preview_page_metadata_rejects_out_of_range_pages(self) -> None:
        with self.assertRaises(InvalidPreviewPageError):
            build_preview_page_metadata(4, page=3, page_size=2)
