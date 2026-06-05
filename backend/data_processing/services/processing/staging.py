"""Public staged-file lease helpers layered over the staged-file cache store."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
import logging
from pathlib import Path

from data_processing.services.observability import log_stage_event

from .s3 import download_object_to_temp_file, head_object_metadata
from .staging_config import StagedFileCacheConfig, load_staged_file_cache_config
from .staging_store import StagedFileCache, StagedFileCacheEntry


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StagedFileLease:
    """Local staged-file handle that knows whether it must be cleaned up."""

    path: Path
    content_length: int
    release_when_done: bool = False


STAGED_FILE_CACHE_CONFIG = load_staged_file_cache_config()
STAGED_FILE_CACHE = StagedFileCache(config=STAGED_FILE_CACHE_CONFIG)


def clear_staged_file_cache() -> None:
    """Remove any staged S3 files kept on disk for reuse."""

    STAGED_FILE_CACHE.clear()


def get_staged_s3_object_path(
    client,
    bucket: str,
    object_key: str,
    *,
    max_size_bytes: int | None = None,
) -> StagedFileLease:
    """Return a staged-file lease, reusing the cache only when enabled."""

    metadata = head_object_metadata(
        client,
        bucket,
        object_key,
        max_size_bytes=max_size_bytes,
    )
    if STAGED_FILE_CACHE.max_items <= 0:
        temp_path, _ = download_object_to_temp_file(
            client,
            bucket,
            object_key,
            metadata=metadata,
        )
        log_stage_event(
            logger,
            "processing.staging.downloaded_ephemeral",
            bucket=bucket,
            object_key=object_key,
            content_length=metadata.content_length,
        )
        return StagedFileLease(
            path=temp_path,
            content_length=metadata.content_length,
            release_when_done=True,
        )

    cached_path = STAGED_FILE_CACHE.get(bucket, object_key, metadata=metadata)
    if cached_path is not None:
        log_stage_event(
            logger,
            "processing.staging.cache_hit",
            bucket=bucket,
            object_key=object_key,
            content_length=metadata.content_length,
        )
        return StagedFileLease(path=cached_path, content_length=metadata.content_length)

    temp_path, _ = download_object_to_temp_file(
        client,
        bucket,
        object_key,
        metadata=metadata,
    )
    cached_path = STAGED_FILE_CACHE.put(bucket, object_key, metadata=metadata, path=temp_path)
    log_stage_event(
        logger,
        "processing.staging.cache_populated",
        bucket=bucket,
        object_key=object_key,
        content_length=metadata.content_length,
    )
    return StagedFileLease(path=cached_path, content_length=metadata.content_length)


def release_staged_file(lease: StagedFileLease) -> None:
    """Clean up request-scoped staged files when cache reuse is disabled."""

    if lease.release_when_done:
        lease.path.unlink(missing_ok=True)


@contextmanager
def lease_staged_s3_object(
    client,
    bucket: str,
    object_key: str,
    *,
    max_size_bytes: int | None = None,
) -> Iterator[StagedFileLease]:
    """Yield a staged-file lease and always release it with the public contract."""

    lease = get_staged_s3_object_path(
        client,
        bucket,
        object_key,
        max_size_bytes=max_size_bytes,
    )
    try:
        yield lease
    finally:
        release_staged_file(lease)
