# Rhombus-AI-Home-Test

Single-host Django + React application for browsing CSV and Excel files in Amazon S3, inferring Pandas data types, and previewing the processed result with optional column overrides.

## What it does

- Connects to S3 using runtime AWS credentials supplied by the user.
- Lists supported `.csv`, `.xls`, and `.xlsx` objects from a bucket/prefix.
- Profiles columns with stricter inference rules for integers, floats, booleans, dates, datetimes, categories, and complex numbers.
- Lets the user override inferred types before reprocessing.
- Stores sanitized processing metadata in Django without persisting AWS secrets.
- Exposes a local CLI via `infer_data_types.py` for quick local-file smoke testing.

## Stack

- Backend: Django 5, Django REST Framework, Pandas, boto3
- Frontend: React 19, TypeScript, Vite, Vitest
- Deployment shape: single host serving the built frontend from Django

## Project structure

- `backend/`: Django project and the `data_processing` app
- `frontend/`: React + TypeScript frontend
- `infer_data_types.py`: local CLI wrapper around the shared processing service
- `sample_data.csv`: simple local sample dataset
- `Dockerfile`: production-oriented single-container deployment

## Requirements

- Python 3.12 recommended for local work to match the Docker runtime
- Node.js 22+ recommended
- npm 11+

The current dependency set also installs and runs on Python 3.14, but keeping local development on Python 3.12 reduces drift from the containerized runtime.

## Local setup

### 1. Create the backend environment

```powershell
python -m venv .venv
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

## Running the app locally

### Backend + built frontend

Build the frontend once, then let Django serve it:

```powershell
cd frontend
npm run build
cd ..
python manage.py runserver
```

Open `http://127.0.0.1:8000`.

### Split development mode

Run Django for the API and Vite for the frontend:

```powershell
python manage.py runserver
```

In a second terminal:

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
- `PORT`: port used by the container startup command

See `.env.example` for a starter set of values.

## CLI usage

The local CLI uses the same inference service as the web application:

```powershell
python infer_data_types.py sample_data.csv --preview-rows 5
```

Optional Excel sheet selection:

```powershell
python infer_data_types.py path\\to\\workbook.xlsx --sheet-name Sheet1
```

## API summary

### `POST /api/s3/files`

Lists supported S3 objects for a bucket/prefix.

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

### `POST /api/data/process`

Processes the selected S3 object and returns schema metadata plus a preview.

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

## Deployment

This repository includes a production-oriented `Dockerfile` that:

- installs Python dependencies
- builds the React frontend
- collects static files
- runs migrations on container start
- serves the app with Gunicorn

### Build the image

```powershell
docker build -t rhombus-home-test .
```

### Run the container

```powershell
docker run --rm -p 8000:8000 `
  -e DJANGO_SECRET_KEY=replace-me `
  -e DJANGO_DEBUG=False `
  -e DJANGO_ALLOWED_HOSTS=localhost,127.0.0.1 `
  rhombus-home-test
```

Then open `http://127.0.0.1:8000`.

## Notes and limitations

- AWS credentials are accepted at runtime and are intentionally not stored in the database.
- CSV handling is chunked for profiling and preview generation; Excel handling is capped at 20 MB in this MVP.
- Type inference is intentionally conservative. Ambiguous date columns stay as text unless the user overrides them.
- The app currently returns a processed preview rather than exporting a full transformed file.
- A public deployment URL is not included in this repository because deployment credentials and hosting configuration are environment-specific.
