FROM node:22-bookworm-slim AS frontend-builder

WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build


FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DJANGO_DEBUG=False \
    PORT=8000 \
    PYTHONPATH=/app/backend

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY manage.py ./
COPY backend ./backend
COPY infer_data_types.py ./
COPY docker/start.py ./docker/start.py
COPY --from=frontend-builder /app/frontend/dist ./frontend/dist

RUN python manage.py collectstatic --noinput

EXPOSE 8000

CMD ["python", "docker/start.py"]
