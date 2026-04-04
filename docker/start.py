from __future__ import annotations

import os
import subprocess
import sys


def main() -> int:
    subprocess.run([sys.executable, "manage.py", "migrate", "--noinput"], check=True)
    port = os.getenv("PORT", "8000")
    os.execvp(
        "gunicorn",
        [
            "gunicorn",
            "rhombus_home_test.wsgi:application",
            "--bind",
            f"0.0.0.0:{port}",
        ],
    )


if __name__ == "__main__":
    raise SystemExit(main())
