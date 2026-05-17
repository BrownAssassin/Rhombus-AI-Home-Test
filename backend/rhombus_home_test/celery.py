"""Celery application bootstrap for async processing tasks."""

from __future__ import annotations

import os

from celery import Celery


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "rhombus_home_test.settings")

app = Celery("rhombus_home_test")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()
