#!/usr/bin/env python
import os
from pathlib import Path
import sys


def main() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "rhombus_home_test.settings")
    sys.path.insert(0, str(Path(__file__).resolve().parent / "backend"))
    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
