FROM node:22-bookworm-slim AS frontend-builder

WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build


FROM python:3.12-slim-bookworm AS python-base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DJANGO_DEBUG=False \
    PORT=8000 \
    PYTHONPATH=/app/backend

WORKDIR /app

FROM python-base AS worker-runtime

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

COPY requirements.base.txt requirements.worker.txt ./
RUN apt-get update \
    && apt-get install --no-install-recommends -y openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r requirements.worker.txt

COPY backend ./backend
CMD ["celery", "-A", "rhombus_home_test", "worker", "--loglevel=info", "--concurrency=2"]


FROM python-base AS web-runtime

COPY requirements.base.txt requirements.web.txt ./
RUN pip install --no-cache-dir -r requirements.web.txt

COPY backend ./backend
COPY docker/start.py ./docker/start.py
COPY --from=frontend-builder /app/frontend/dist ./frontend/dist

RUN python backend/manage.py collectstatic --noinput

EXPOSE 8000

CMD ["python", "docker/start.py"]
