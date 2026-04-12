"""Container startup entrypoint for migrations plus Gunicorn launch."""

from __future__ import annotations

import os
import subprocess
import sys


def main() -> int:
    """Run migrations once, then hand off the process to Gunicorn."""

    subprocess.run([sys.executable, "manage.py", "migrate", "--noinput"], check=True)
    # Render injects the listening port at runtime, so Gunicorn must bind to
    # that value after the one-time startup tasks complete.
    port = os.getenv("PORT", "8000")
    workers = os.getenv("WEB_CONCURRENCY", "1")
    threads = os.getenv("GUNICORN_THREADS", "1")
    timeout = os.getenv("GUNICORN_TIMEOUT", "180")
    os.execvp(
        "gunicorn",
        [
            "gunicorn",
            "rhombus_home_test.wsgi:application",
            "--bind",
            f"0.0.0.0:{port}",
            "--workers",
            workers,
            "--threads",
            threads,
            "--timeout",
            timeout,
        ],
    )


if __name__ == "__main__":
    raise SystemExit(main())
