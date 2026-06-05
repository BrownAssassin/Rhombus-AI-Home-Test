"""Shared observability helpers for backend timing and stage logs."""

from __future__ import annotations

import logging
from time import perf_counter


def stage_started() -> float:
    """Capture a monotonic timestamp for a backend processing stage."""

    return perf_counter()


def elapsed_ms(started_at: float) -> float:
    """Convert a stage start timestamp into elapsed milliseconds."""

    return round((perf_counter() - started_at) * 1000, 2)


def log_stage_event(
    logger: logging.Logger,
    event: str,
    *,
    duration_ms: float | None = None,
    **fields,
) -> None:
    """Emit one standardized backend stage log with safe structured fields."""

    extra = {key: value for key, value in fields.items() if value is not None}
    if duration_ms is not None:
        extra["duration_ms"] = duration_ms
    logger.info(event, extra=extra)
