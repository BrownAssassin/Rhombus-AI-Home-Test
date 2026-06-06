"""Configuration helpers for the staged-file cache policy."""

from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class StagedFileCacheConfig:
    """Configured bounds for the staged-file disk cache."""

    max_items: int
    ttl_seconds: int


def load_staged_file_cache_config() -> StagedFileCacheConfig:
    """Load the staged-file cache policy from environment variables."""

    return StagedFileCacheConfig(
        max_items=max(0, int(os.getenv("STAGED_FILE_CACHE_MAX_ITEMS", "2"))),
        ttl_seconds=max(0, int(os.getenv("STAGED_FILE_CACHE_TTL_SECONDS", "900"))),
    )
