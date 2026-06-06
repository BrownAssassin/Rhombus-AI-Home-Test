"""Query helpers for tracked processing runs."""

from __future__ import annotations

from data_processing.models import ProcessingRun


def get_run(run_id: int) -> ProcessingRun | None:
    """Return one tracked run by id or None when it no longer exists."""

    return ProcessingRun.objects.detail_fields().filter(pk=run_id).first()


def get_active_run(run_id: int) -> ProcessingRun | None:
    """Return one active tracked run or None when it is no longer pending."""

    return ProcessingRun.objects.active().detail_fields().filter(pk=run_id).first()


def list_runs(*, object_key: str | None = None, limit: int = 10):
    """Return recent runs ordered newest-first with an optional file filter."""

    runs = ProcessingRun.objects.summary_fields()
    if object_key is not None:
        runs = runs.for_object(object_key)
    return runs.recent(limit)
