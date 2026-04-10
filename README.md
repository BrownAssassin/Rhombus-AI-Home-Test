# Rhombus-AI-Home-Test

Single-host Django + React application for browsing CSV and Excel files in Amazon S3, inferring Pandas data types, and previewing the processed result with optional column overrides.

## What it does

- Connects to S3 using runtime AWS credentials supplied by the user.
- Lists supported `.csv`, `.xls`, and `.xlsx` objects from a bucket or prefix.
- Profiles columns with conservative inference rules for integers, floats, booleans, dates, datetimes, categories, and complex numbers.
- Lets the user override inferred types before reprocessing.
- Pages through the processed dataset from the backend so reviewers can inspect full files instead of a capped in-memory sample.
- Stores sanitized processing metadata in Django without persisting AWS secrets.
- Exposes a local CLI via `infer_data_types.py` for quick local-file smoke testing.

## Stack

- Backend: Django 5, Django REST Framework, Pandas, boto3
- Frontend: React 19, TypeScript, Vite, Vitest
- Deployment shape: single host serving the built frontend from Django

## Project structure

- `backend/`: Django project and the `data_processing` app
- `frontend/`: React + TypeScript frontend
- `docs/brief/`: assignment brief and supporting project notes
- `examples/`: sample datasets for local smoke testing
- `infer_data_types.py`: local CLI wrapper around the shared processing service
- `Dockerfile`: production-oriented single-container deployment

## Requirements

- Python 3.12 recommended for local work to match the Docker runtime
- Node.js 22 or newer recommended
- npm 11 or newer
- Docker Desktop for container verification and deployment builds

The current dependency set also installs and runs on Python 3.14, but keeping local development on Python 3.12 reduces drift from the containerized runtime.

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

## Render deployment

Render is the recommended public host for this project because it matches the app's single-container architecture and gives you one public URL for both the Django API and the React frontend.

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

Render automatically provides `PORT`, `RENDER_EXTERNAL_HOSTNAME`, and `RENDER_EXTERNAL_URL`, and the app is configured to trust those values without any extra setup.

### Deploy and verify

1. Trigger the initial deploy from Render.
2. Open the generated `onrender.com` URL after the health check passes.
3. Confirm `GET /api/health/` returns a healthy response.
4. Run one end-to-end flow through the UI with a real or demo S3 bucket.

## Notes and limitations

- AWS credentials are accepted at runtime and are intentionally not stored in the database.
- CSV handling is chunked for profiling and preview generation; Excel handling is capped at 20 MB in this MVP.
- Type inference is intentionally conservative. Ambiguous date columns stay as text unless the user overrides them.
- The app supports paginated preview browsing across the processed dataset, but it does not export a full transformed file in this MVP.
- A Render deployment that keeps SQLite in the container filesystem is suitable for demos, but `ProcessingRun` history resets whenever the service is rebuilt or restarted.
