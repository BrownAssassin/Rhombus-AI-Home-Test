"""Focused tests for staged-file cache configuration helpers."""

from unittest.mock import patch

from django.test import SimpleTestCase

from data_processing.services.processing.staging_config import load_staged_file_cache_config


class StagingConfigTests(SimpleTestCase):
    """Verify staged-file cache policy loading from environment variables."""

    def test_load_staged_file_cache_config_clamps_negative_values(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "STAGED_FILE_CACHE_MAX_ITEMS": "-4",
                "STAGED_FILE_CACHE_TTL_SECONDS": "-10",
            },
            clear=False,
        ):
            config = load_staged_file_cache_config()

        self.assertEqual(config.max_items, 0)
        self.assertEqual(config.ttl_seconds, 0)
