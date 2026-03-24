#!/usr/bin/env bash
# Start the FastAPI backend
cd "$(dirname "$0")"
mkdir -p data celery_data/in celery_data/out celery_data/processed
./venv/bin/uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
