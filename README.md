# Rhombus-AI-Home-Test

[![Python](https://img.shields.io/badge/Python-3.12-blue)](https://www.python.org/)
[![Django](https://img.shields.io/badge/Django-5.2-0C4B33)](https://www.djangoproject.com/)
[![React](https://img.shields.io/badge/React-19-61DAFB)](https://react.dev/)
[![TypeScript](https://img.shields.io/badge/TypeScript-5-3178C6)](https://www.typescriptlang.org/)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED)](https://www.docker.com/)
[![Live App](https://img.shields.io/badge/Live%20App-Render-46E3B7)](https://rhombus-ai-home-test.onrender.com/)
[![License: Unlicense](https://img.shields.io/badge/License-Unlicense-lightgrey)](LICENSE)
[![Repo Size](https://img.shields.io/github/repo-size/BrownAssassin/Rhombus-AI-Home-Test)](https://github.com/BrownAssassin/Rhombus-AI-Home-Test)

Single-host Django + React application for browsing CSV and Excel files in Amazon S3, inferring Pandas data types, previewing the processed result with optional column overrides, and experimenting with Redis/Celery background processing plus PySpark CSV comparison.

Public deployment: `https://rhombus-ai-home-test.onrender.com/`

## What it does

- Connects to S3 using runtime AWS credentials supplied by the user.
- Lists supported `.csv`, `.xls`, and `.xlsx` objects from a bucket or prefix.
- Profiles columns with conservative inference rules for integers, floats, booleans, dates, datetimes, categories, and complex numbers.
- Lets the user override inferred types before reprocessing.
- Pages through the processed dataset from the backend so reviewers can inspect full files instead of a capped in-memory sample.
- Queues background processing jobs through Celery and Redis while the frontend polls run status updates.
- Offers an experimental PySpark comparison mode for CSV row counting, schema mapping, and preview generation.
- Stores sanitized processing metadata in Django without persisting AWS secrets.
- Exposes a local CLI via `infer_data_types.py` for quick local-file smoke testing.

## Inference and performance approach

- CSV files are staged from S3 onto local disk, then profiled in chunks so larger datasets do not depend on a single long-lived streaming response.
- Repeated preview-page requests can reuse a small bounded cache of staged S3 files to avoid re-downloading the same CSV on every page change or override.
- Excel files are supported, but they still load the selected sheet into memory and are capped at 20 MB in this MVP.
- Type inference is intentionally conservative: ambiguous short dates stay as text unless overridden, and manual overrides are validated to avoid lossy coercion.
- Date and DateTime are both backed by pandas datetime storage internally, but the preview renders them differently so date-only columns stay calendar-shaped.
- Category inference is strict for larger datasets and slightly softer for very small repeated-label samples such as grade-like columns.
- Redis and Celery now provide an optional background-processing path, but the original synchronous Pandas flow remains the authoritative default path.
- PySpark is scoped intentionally as an experimental CSV comparison mode. It compares row counts, Spark-native schema mapping, and preview slices without replacing the current Pandas inference engine.

## Stack

- Backend: Django 5, Django REST Framework, Pandas, boto3, Celery, Redis, PySpark (experimental)
- Frontend: React 19, TypeScript, Vite, Vitest
- Local orchestration: Docker Compose for web, Celery worker, and Redis
- Deployment shape: single host serving the built frontend from Django

## Project structure

- `backend/`: Django project and the `data_processing` app
- `frontend/`: React + TypeScript frontend
- `docs/brief/`: assignment brief and supporting project notes
- `examples/`: sample datasets for local smoke testing
- `infer_data_types.py`: local CLI wrapper around the shared processing service
- `Dockerfile`: production-oriented single-container deployment
- `docker-compose.yml`: local async development stack for Django, Celery, and Redis

## Requirements

- Python 3.12 recommended for local work to match the Docker runtime
- Node.js 22 or newer recommended
- npm 11 or newer
- Docker Desktop for container verification and deployment builds
- Java 17 or newer for the experimental local PySpark comparison path
- Redis for the optional local async-processing stack, unless you use Docker Compose

The current dependency set also installs and runs on Python 3.14, but keeping local development on Python 3.12 reduces drift from the containerized runtime. If you only want the stable synchronous flow, Redis and Java are optional. They are needed only for the experimental async and Spark comparison features in this branch.

## Local setup

### 1. Create the backend environment

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

### 2. Install the frontend dependencies

```powershell
cd frontend
npm install
cd ..
```

### 3. Apply migrations

```powershell
python manage.py migrate
```

### 4. Optional: start Redis and a Celery worker for the async path

The new async endpoints require Redis plus a running Celery worker. The quickest local path is Docker Compose:

```powershell
docker compose up --build
```

If you prefer to run the pieces manually, start Redis first, then launch the worker in a second terminal:

```powershell
python -m celery -A rhombus_home_test worker --loglevel=info
```

## Running the app locally

### Backend plus built frontend

Build the frontend once, then let Django serve it:

```powershell
cd frontend
npm run build
cd ..
python manage.py runserver
```

Open `http://127.0.0.1:8000`.

### Async and experimental stack via Docker Compose

This branch adds a local multi-service setup for the new Celery and Redis path:

```powershell
docker compose up --build
```

That starts:

- `web`: Django + the built frontend
- `worker`: Celery background worker
- `redis`: broker/result backend for queued jobs

### Split development mode

Run Django for the API:

```powershell
python manage.py runserver
```

In a second terminal, run Vite for the frontend:

```powershell
cd frontend
npm run dev
```

Open `http://127.0.0.1:5173`.

## Environment variables

The app works locally without extra configuration, but these variables are supported for deployment:

- `DJANGO_SECRET_KEY`: Django secret key
- `DJANGO_DEBUG`: `True` or `False`
- `DJANGO_ALLOWED_HOSTS`: comma-separated hostnames
- `DJANGO_CSRF_TRUSTED_ORIGINS`: comma-separated trusted origins
- `DJANGO_SQLITE_PATH`: optional SQLite database path
- `DJANGO_FRONTEND_BUILD_DIR`: optional override for the built frontend location
- `WEB_CONCURRENCY`: Gunicorn worker count for the container runtime
- `GUNICORN_TIMEOUT`: Gunicorn request timeout in seconds
- `CSV_CHUNK_SIZE`: chunk size used for CSV profiling and preview paging
- `STAGED_FILE_CACHE_MAX_ITEMS`: max number of staged S3 files kept on local disk for reuse (`0` disables the cache)
- `STAGED_FILE_CACHE_TTL_SECONDS`: cache lifetime for staged S3 files
- `REDIS_URL`: default Redis connection string
- `CELERY_BROKER_URL`: Celery broker URL, typically Redis
- `CELERY_RESULT_BACKEND`: Celery result backend URL, typically Redis
- `CELERY_TASK_TIME_LIMIT`: hard time limit for background tasks
- `CELERY_TASK_SOFT_TIME_LIMIT`: soft time limit for background tasks
- `CELERY_TASK_ALWAYS_EAGER`: optional local debugging switch to execute async tasks inline
- `PORT`: port used by the container startup command

On Render, `RENDER_EXTERNAL_HOSTNAME` and `RENDER_EXTERNAL_URL` are injected automatically and merged into Django's trusted host and origin lists. You only need to set `DJANGO_ALLOWED_HOSTS` or `DJANGO_CSRF_TRUSTED_ORIGINS` manually if you later add a custom domain.

See `.env.example` for a starter local or container configuration.

## CLI usage

The local CLI uses the same inference service as the web application:

```powershell
python infer_data_types.py examples/sample_data.csv --preview-rows 5
```

Optional Excel sheet selection:

```powershell
python infer_data_types.py path\to\workbook.xlsx --sheet-name Sheet1
```

## API summary

### `POST /api/s3/files`

Lists supported S3 objects for a bucket or prefix.

Request body:

```json
{
  "access_key_id": "AKIA...",
  "secret_access_key": "secret",
  "session_token": "",
  "region": "ap-southeast-2",
  "bucket": "demo-bucket",
  "prefix": "incoming/"
}
```

## Project brief alignment

- **Pandas inference and conversion**: the shared backend service profiles and converts CSV/XLS/XLSX data loaded into pandas DataFrames, with explicit handling for object-like mixed columns, dates, numerics, categories, booleans, and complex values.
- **Large-file handling and tuning**: S3 CSVs are staged locally, profiled in chunks, and paged from the backend. Render tuning knobs such as `WEB_CONCURRENCY`, `GUNICORN_TIMEOUT`, `CSV_CHUNK_SIZE`, and staged-file cache settings are documented below.
- **Django backend**: the API exposes S3 browsing, file processing, preview pagination, health checks, and persisted sanitized run metadata.
- **React frontend**: the UI supports S3 connection, file browsing, file processing, schema review, manual overrides, reset/reprocess actions, and processed preview pagination.
- **Documentation and testing**: the repo includes setup/deploy instructions, backend tests, frontend tests, and a local CLI for smoke testing with sample datasets.

### `POST /api/data/process`

Processes the selected S3 object, stores the inferred schema, and returns the first preview page.

Request body:

```json
{
  "access_key_id": "AKIA...",
  "secret_access_key": "secret",
  "session_token": "",
  "region": "ap-southeast-2",
  "bucket": "demo-bucket",
  "prefix": "incoming/",
  "object_key": "incoming/sample.csv",
  "sheet_name": "",
  "preview_row_limit": 100,
  "overrides": [
    { "column": "Score", "target_type": "float" }
  ]
}
```

Response highlights:

- `runId`: stored processing run identifier used for later preview-page requests and display
- `schema`: inferred or overridden schema metadata
- `previewRows`: first processed page of rows
- `previewPage`: page metadata including `page`, `pageSize`, `totalRows`, and `totalPages`

### `POST /api/data/process-async`

Queues the existing Pandas processing flow as a Celery background task. This is the production-style enhancement path for longer-running files.

Response highlights:

- `runId`: processing run identifier used for polling
- `taskId`: Celery task identifier
- `status`: initial queued state
- `engine`: currently `pandas`

### `GET /api/data/runs/<id>`

Polls the current run lifecycle state.

While queued or processing, the response returns:

- `status`
- `engine`
- `progressStage`
- `progressPercent`
- `errorMessage`

Once completed, it also returns the same processed payload shape used by `POST /api/data/process`, including:

- `rowCount`
- `schema`
- `previewColumns`
- `previewRows`
- `previewPage`
- `warnings`
- `processingMetadata`

### `POST /api/data/preview`

Returns a specific processed page using the current schema and file context. The API will reuse the stored `ProcessingRun`
when it is available, but the request can also supply the preview context directly so paging still works in stateless or
ephemeral deployment environments.

Request body:

```json
{
  "access_key_id": "AKIA...",
  "secret_access_key": "secret",
  "session_token": "",
  "region": "ap-southeast-2",
  "bucket": "demo-bucket",
  "prefix": "incoming/",
  "run_id": 12,
  "object_key": "incoming/sample.csv",
  "file_type": "csv",
  "selected_sheet": "",
  "row_count": 14941,
  "schema": [
    {
      "column": "Score",
      "inferred_type": "float",
      "storage_type": "Float64",
      "display_type": "Float",
      "nullable": true,
      "confidence": 0.97,
      "warnings": [],
      "null_token_count": 0,
      "sample_values": ["90", "75"],
      "allowed_overrides": ["text", "integer", "float", "boolean", "date", "datetime", "category", "complex"]
    }
  ],
  "preview_columns": ["Score"],
  "page": 3,
  "page_size": 50
}
```

### `POST /api/data/spark-compare`

Runs an experimental CSV-only PySpark comparison against the selected staged S3 file. This mode is intentionally educational and does not replace the main Pandas inference pipeline.

Response highlights:

- `sparkSchema`: Spark-native type details plus mapped user-facing labels
- `rowCount`: Spark row count
- `previewColumns` and `previewRows`: requested Spark preview slice
- `processingMetadata.durationMs`: Spark comparison timing
- `notes`: reminder that the Pandas path remains authoritative

## Async architecture flow

The enhancement path introduced on this branch follows this lifecycle:

```text
Django request -> Celery task -> Redis broker/result backend -> Celery worker
-> ProcessingRun updates in Django -> frontend polling via GET /api/data/runs/<id>
```

That keeps Redis focused on transient queueing and task state, while Django remains the durable source of truth for user-visible processing runs.

## Testing

Backend:

```powershell
python manage.py test
python manage.py check
```

Frontend:

```powershell
cd frontend
npm test
npm run build
```

CLI smoke test:

```powershell
python infer_data_types.py examples/sample_data.csv --preview-rows 5
```

## Docker verification

Build the production image:

```powershell
docker build -t rhombus-home-test .
```

Run the container locally:

```powershell
docker run --rm -p 8000:8000 `
  -e DJANGO_SECRET_KEY=replace-me `
  -e DJANGO_DEBUG=False `
  -e DJANGO_ALLOWED_HOSTS=localhost,127.0.0.1 `
  rhombus-home-test
```

Then open `http://127.0.0.1:8000`.

Run the local async stack:

```powershell
docker compose up --build
```

## Render deployment

Render is the recommended public host for this project because it matches the app's single-container architecture and gives you one public URL for both the Django API and the React frontend.

This branch keeps the current synchronous Pandas flow as the stable deployment default. The new Redis/Celery and PySpark additions are intended to be validated locally or in a separate enhancement/demo environment first rather than forcing them into the existing submission deployment immediately.

### Create the service

1. Sign in to Render and create a new Web Service.
2. Connect the GitHub repository `BrownAssassin/Rhombus-AI-Home-Test`.
3. Select the `main` branch.
4. Choose `Docker` as the runtime so Render builds from the repository `Dockerfile`.
5. Set the health check path to `/api/health/`.

### Configure environment variables

Add these values in the Render dashboard before the first deploy:

- `DJANGO_SECRET_KEY`: required secret value for Django
- `DJANGO_DEBUG=False`

Optional settings:

- `DJANGO_ALLOWED_HOSTS`: only needed if you later add a custom domain
- `DJANGO_CSRF_TRUSTED_ORIGINS`: only needed if you later add a custom domain
- `DJANGO_SQLITE_PATH=/app/data/db.sqlite3`: only needed if you later attach a persistent disk and want the SQLite file to survive redeploys
- `WEB_CONCURRENCY=1`: recommended for the starter Render instance size
- `GUNICORN_TIMEOUT=180`: gives longer CSV processing requests room to finish
- `CSV_CHUNK_SIZE=500`: keeps CSV memory use conservative during profiling and paging
- `STAGED_FILE_CACHE_MAX_ITEMS=2`: lets repeated paging and reprocessing reuse the same staged S3 file
- `STAGED_FILE_CACHE_TTL_SECONDS=900`: expires staged files after 15 minutes to keep disk use bounded

Render automatically provides `PORT`, `RENDER_EXTERNAL_HOSTNAME`, and `RENDER_EXTERNAL_URL`, and the app is configured to trust those values without any extra setup.

### Deploy and verify

1. Trigger the initial deploy from Render.
2. Open the generated `onrender.com` URL after the health check passes.
3. Confirm `GET /api/health/` returns a healthy response.
4. Run one end-to-end flow through the UI with a real or demo S3 bucket.

## Notes and limitations

- AWS credentials are accepted at runtime and are intentionally not stored in the database.
- CSV handling is chunked after staging the S3 object to a temp file, and repeat requests can reuse a small bounded local cache of staged files to avoid unnecessary re-downloads. If you set `STAGED_FILE_CACHE_MAX_ITEMS=0`, staged files are cleaned up after each request instead of being reused.
- Excel handling is capped at 20 MB in this MVP, and each preview-page request reloads the selected sheet because pandas does not offer the same chunked read path as CSV.
- Type inference is intentionally conservative. Ambiguous date columns stay as text unless the user overrides them.
- Small repeated-label columns can infer as `Category`, but high-cardinality or mostly-unique string columns intentionally stay as `Text`.
- The app supports paginated preview browsing across the processed dataset, but it does not export a full transformed file in this MVP.
- A Render deployment that keeps SQLite in the container filesystem is suitable for demos, but `ProcessingRun` history resets whenever the service is rebuilt or restarted.
- Redis and Celery are the real production-style enhancement in this branch. They move longer processing jobs out of the request-response cycle while keeping Django as the source of truth for run status.
- The PySpark feature in this branch is experimental and CSV-only. It is intentionally framed as a comparison/learning tool, not as a drop-in replacement for the main Pandas inference engine.
- The polished `main` branch remains the stable submission snapshot. This branch is a post-submission enhancement that demonstrates how Redis/Celery and PySpark could be applied to the app thoughtfully.
