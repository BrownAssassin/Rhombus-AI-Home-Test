"""Celery tasks for background dataset processing."""

from __future__ import annotations

from celery import shared_task

from data_processing.application.process_runs import run_background_process, run_background_spark_comparison


@shared_task(bind=True)
def process_s3_object_async(self, *, run_id: int, request_payload: dict[str, object]) -> dict[str, object]:
    """Process a dataset in the background and persist run progress as it changes."""

    return run_background_process(run_id=run_id, request_payload=request_payload)


@shared_task(bind=True)
def run_spark_comparison(self, *, run_id: int, request_payload: dict[str, object]) -> dict[str, object]:
    """Run the experimental Spark comparison in the background for future use."""

    return run_background_spark_comparison(run_id=run_id, request_payload=request_payload)
