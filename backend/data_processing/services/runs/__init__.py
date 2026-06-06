"""Run persistence helpers grouped by query, creation, and lifecycle roles."""

from .creation import (
    create_completed_process_run,
    create_queued_process_run,
    create_queued_spark_comparison_run,
)
from .lifecycle import (
    mark_run_completed,
    mark_run_comparison_completed,
    mark_run_failed,
    mark_run_processing,
    mark_run_queued,
)
from .queries import get_active_run, get_run, list_runs

__all__ = [
    "create_completed_process_run",
    "create_queued_process_run",
    "create_queued_spark_comparison_run",
    "get_active_run",
    "get_run",
    "list_runs",
    "mark_run_completed",
    "mark_run_comparison_completed",
    "mark_run_failed",
    "mark_run_processing",
    "mark_run_queued",
]
