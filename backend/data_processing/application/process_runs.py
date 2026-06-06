"""Stable application facade for sync, async, and Spark process runs."""

from __future__ import annotations

from .process_run_execution import run_background_process, run_background_spark_comparison
from .process_run_queueing import queue_process_run, queue_spark_comparison
from .process_run_sync import process_sync_run

__all__ = [
    "process_sync_run",
    "queue_process_run",
    "queue_spark_comparison",
    "run_background_process",
    "run_background_spark_comparison",
]
