# Rhombus-AI-Home-Test

This repository contains the scaffold for a Django + React application focused on S3-backed dataset ingestion and data type inference.

Current structure:
- `backend/` contains the Django project and the `data_processing` app.
- `frontend/` contains the React + TypeScript + Vite client shell.
- `requirements.txt` captures the Python backend/runtime dependencies.
- `manage.py` is configured to run the Django project from the repo root.

Local setup:
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
cd frontend
npm install
```

Development:
```powershell
python manage.py migrate
python manage.py runserver
cd frontend
npm run dev
```
