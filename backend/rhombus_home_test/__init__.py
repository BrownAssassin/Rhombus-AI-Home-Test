"""Django project package for the Rhombus AI home test."""

from .celery import app as celery_app

__all__ = ("celery_app",)
