#!/usr/bin/env python
"""Project management entrypoint for Django commands."""

from __future__ import annotations

import os
import sys


def main() -> None:
    """Execute Django management commands for the backend project."""

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "rhombus_home_test.settings")
    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
