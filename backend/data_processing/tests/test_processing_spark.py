"""Processing regression tests for the experimental Spark comparison path."""

from unittest.mock import patch

from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

from django.test import SimpleTestCase

from data_processing.services.processing import S3Credentials, clear_staged_file_cache
from data_processing.services.spark_processing import run_spark_csv_comparison

from .support import spark_dataframe_fixture


class SparkProcessingTests(SimpleTestCase):
    """Verify the optimized Spark comparison behavior."""

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

    def test_run_spark_csv_comparison_reads_the_csv_once_and_serializes_preview_values(self) -> None:
        _, spark, staged_context = spark_dataframe_fixture()

        with (
            patch("data_processing.services.spark_processing.build_s3_client", return_value=object()),
            patch("data_processing.services.spark_processing.lease_staged_s3_object", return_value=staged_context),
            patch("data_processing.services.spark_processing._create_spark_session", return_value=spark),
        ):
            result = run_spark_csv_comparison(
                credentials=self.credentials,
                object_key="incoming/sample.csv",
                page=1,
                page_size=25,
            )

        self.assertEqual(spark.read.csv_calls, 1)
        self.assertTrue(spark.stopped)
        self.assertEqual(result["previewRows"][0]["Date"], "2026-06-05")
        self.assertEqual(result["previewRows"][0]["OccurredAt"], "2026-06-05T12:30:00")
        self.assertEqual(result["previewRows"][0]["Amount"], "12.50")
        self.assertEqual(result["sparkSchema"][0]["sparkType"], "date")
