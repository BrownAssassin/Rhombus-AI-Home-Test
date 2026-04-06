import os
from pathlib import Path


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().casefold() in {"1", "true", "yes", "on"}


def env_list(name: str, default: list[str]) -> list[str]:
    value = os.getenv(name)
    if value is None:
        return default
    return [item.strip() for item in value.split(",") if item.strip()]


def merge_unique(base: list[str], extras: list[str]) -> list[str]:
    merged = list(base)
    for item in extras:
        if item and item not in merged:
            merged.append(item)
    return merged


BASE_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = BASE_DIR.parent
FRONTEND_DIR = REPO_ROOT / "frontend"
FRONTEND_BUILD_DIR = Path(os.getenv("DJANGO_FRONTEND_BUILD_DIR", FRONTEND_DIR / "dist"))
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME", "").strip()
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL", "").strip()

SECRET_KEY = os.getenv("DJANGO_SECRET_KEY", "django-insecure-rhombus-home-test")
DEBUG = env_bool("DJANGO_DEBUG", True)

# Render publishes the canonical onrender.com host and origin at runtime, so
# fold them into Django's trust lists instead of making every deploy restate them.
ALLOWED_HOSTS = merge_unique(
    env_list("DJANGO_ALLOWED_HOSTS", ["localhost", "127.0.0.1"]),
    [RENDER_EXTERNAL_HOSTNAME],
)
CSRF_TRUSTED_ORIGINS = merge_unique(
    env_list("DJANGO_CSRF_TRUSTED_ORIGINS", []),
    [RENDER_EXTERNAL_URL],
)

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "rest_framework",
    "data_processing",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "rhombus_home_test.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [FRONTEND_BUILD_DIR],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    }
]

WSGI_APPLICATION = "rhombus_home_test.wsgi.application"
ASGI_APPLICATION = "rhombus_home_test.asgi.application"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": Path(os.getenv("DJANGO_SQLITE_PATH", REPO_ROOT / "db.sqlite3")),
    }
}

AUTH_PASSWORD_VALIDATORS = []

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

STATIC_URL = "/static/"
STATIC_ROOT = REPO_ROOT / "backend" / "static"
STATICFILES_DIRS = [FRONTEND_BUILD_DIR] if FRONTEND_BUILD_DIR.exists() else []
STATICFILES_STORAGE = "whitenoise.storage.CompressedManifestStaticFilesStorage"
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

REST_FRAMEWORK = {
    "DEFAULT_RENDERER_CLASSES": ["rest_framework.renderers.JSONRenderer"],
}
