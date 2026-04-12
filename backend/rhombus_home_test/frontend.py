"""Serve the built React frontend from Django."""

from pathlib import Path

from django.conf import settings
from django.http import FileResponse, HttpResponse


def frontend_app(request):
    """Return the built frontend index or a helpful build-missing response."""

    build_dir = Path(settings.FRONTEND_BUILD_DIR)
    index_path = build_dir / "index.html"
    if not index_path.exists():
        return HttpResponse(
            "Frontend build not found. Run `npm install` and `npm run build` in ./frontend.",
            content_type="text/plain",
            status=503,
        )
    return FileResponse(index_path.open("rb"))
