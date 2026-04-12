"""WSGI entrypoint for the Django project."""

import os

from django.core.wsgi import get_wsgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "rhombus_home_test.settings")

application = get_wsgi_application()
