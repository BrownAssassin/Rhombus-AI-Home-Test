"""Application-level errors for API orchestration and background run flows."""

from __future__ import annotations


class ApplicationError(Exception):
    """Base class for orchestration errors that map to stable API responses."""

    status_code = 400
    code = "application_error"


class RunNotFoundError(ApplicationError):
    """Raised when the requested tracked run no longer exists."""

    status_code = 404
    code = "run_not_found"


class RunNotCompletedError(ApplicationError):
    """Raised when a tracked run is referenced before completion."""

    status_code = 409
    code = "run_not_completed"


class SourceRunNotCompletedError(ApplicationError):
    """Raised when a Spark comparison references a non-terminal process run."""

    status_code = 409
    code = "source_run_not_completed"


class InvalidSourceRunError(ApplicationError):
    """Raised when a source run is incompatible with Spark comparison."""

    status_code = 400
    code = "invalid_source_run"


class TaskQueueError(ApplicationError):
    """Raised when Celery is unavailable for a queued background workflow."""

    status_code = 503
    code = "task_queue_error"
