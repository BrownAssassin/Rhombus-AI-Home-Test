"""Disk-backed staged-file caching for S3 objects reused across requests."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
import os
from pathlib import Path
import threading
import time

from .s3 import S3ObjectMetadata, download_object_to_temp_file, head_object_metadata


STAGED_FILE_CACHE_MAX_ITEMS = max(0, int(os.getenv("STAGED_FILE_CACHE_MAX_ITEMS", "2")))
STAGED_FILE_CACHE_TTL_SECONDS = max(0, int(os.getenv("STAGED_FILE_CACHE_TTL_SECONDS", "900")))


@dataclass
class StagedFileCacheEntry:
    """Cached local copy of an S3 object plus the metadata that validated it."""

    path: Path
    content_length: int
    etag: str
    cached_at: float


@dataclass(frozen=True)
class StagedFileLease:
    """Local staged-file handle that knows whether it must be cleaned up."""

    path: Path
    content_length: int
    release_when_done: bool = False


class StagedFileCache:
    """Small disk cache for recently staged S3 objects."""

    def __init__(self, *, max_items: int, ttl_seconds: int) -> None:
        """Create a bounded cache for staged S3 files."""

        self.max_items = max_items
        self.ttl_seconds = ttl_seconds
        self._entries: OrderedDict[tuple[str, str], StagedFileCacheEntry] = OrderedDict()
        self._lock = threading.Lock()

    def clear(self) -> None:
        """Remove every staged file currently tracked by the cache."""

        with self._lock:
            for entry in self._entries.values():
                entry.path.unlink(missing_ok=True)
            self._entries.clear()

    def get(self, bucket: str, object_key: str, *, metadata: S3ObjectMetadata) -> Path | None:
        """Return a still-valid staged file for the object, if one exists."""

        cache_key = (bucket, object_key)
        now = time.time()
        with self._lock:
            self._purge_expired_locked(now)
            entry = self._entries.get(cache_key)
            if entry is None:
                return None
            if (
                not entry.path.exists()
                or entry.content_length != metadata.content_length
                or entry.etag != metadata.etag
            ):
                self._remove_entry_locked(cache_key)
                return None
            self._entries.move_to_end(cache_key)
            return entry.path

    def put(self, bucket: str, object_key: str, *, metadata: S3ObjectMetadata, path: Path) -> Path:
        """Store a freshly staged file and evict older entries if needed."""

        cache_key = (bucket, object_key)
        entry = StagedFileCacheEntry(
            path=path,
            content_length=metadata.content_length,
            etag=metadata.etag,
            cached_at=time.time(),
        )
        with self._lock:
            self._purge_expired_locked(entry.cached_at)
            existing = self._entries.pop(cache_key, None)
            if existing is not None and existing.path != path:
                existing.path.unlink(missing_ok=True)
            self._entries[cache_key] = entry
            self._entries.move_to_end(cache_key)
            self._evict_overflow_locked()
        return path

    def _purge_expired_locked(self, now: float) -> None:
        """Drop expired or missing cache entries while the lock is held."""

        if self.ttl_seconds <= 0:
            while self._entries:
                self._remove_entry_locked(next(iter(self._entries)))
            return

        for cache_key in list(self._entries):
            entry = self._entries[cache_key]
            if not entry.path.exists() or now - entry.cached_at > self.ttl_seconds:
                self._remove_entry_locked(cache_key)

    def _evict_overflow_locked(self) -> None:
        """Enforce the configured max entry count while the lock is held."""

        while len(self._entries) > self.max_items:
            self._remove_entry_locked(next(iter(self._entries)))

    def _remove_entry_locked(self, cache_key: tuple[str, str]) -> None:
        """Remove one cached entry and delete its staged file if present."""

        entry = self._entries.pop(cache_key, None)
        if entry is not None:
            entry.path.unlink(missing_ok=True)


STAGED_FILE_CACHE = StagedFileCache(
    max_items=STAGED_FILE_CACHE_MAX_ITEMS,
    ttl_seconds=STAGED_FILE_CACHE_TTL_SECONDS,
)


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
        return StagedFileLease(
            path=temp_path,
            content_length=metadata.content_length,
            release_when_done=True,
        )

    cached_path = STAGED_FILE_CACHE.get(bucket, object_key, metadata=metadata)
    if cached_path is not None:
        return StagedFileLease(path=cached_path, content_length=metadata.content_length)

    temp_path, _ = download_object_to_temp_file(
        client,
        bucket,
        object_key,
        metadata=metadata,
    )
    cached_path = STAGED_FILE_CACHE.put(bucket, object_key, metadata=metadata, path=temp_path)
    return StagedFileLease(path=cached_path, content_length=metadata.content_length)


def release_staged_file(lease: StagedFileLease) -> None:
    """Clean up request-scoped staged files when cache reuse is disabled."""

    if lease.release_when_done:
        lease.path.unlink(missing_ok=True)
