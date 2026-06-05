"""Cache store implementation for staged S3 object files."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
import threading
import time

from .s3 import S3ObjectMetadata
from .staging_config import StagedFileCacheConfig


@dataclass
class StagedFileCacheEntry:
    """Cached local copy of an S3 object plus the metadata that validated it."""

    path: Path
    content_length: int
    etag: str
    cached_at: float


class StagedFileCache:
    """Small disk cache for recently staged S3 objects."""

    def __init__(self, *, config: StagedFileCacheConfig) -> None:
        """Create a bounded cache for staged S3 files."""

        self.config = config
        self.max_items = config.max_items
        self.ttl_seconds = config.ttl_seconds
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
