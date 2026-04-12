#!/usr/bin/env python
"""Project management entrypoint for Django commands."""

import os
from pathlib import Path
import sys


def main() -> None:
    """Execute Django management commands with the backend package on sys.path."""

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "rhombus_home_test.settings")
    sys.path.insert(0, str(Path(__file__).resolve().parent / "backend"))
    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
