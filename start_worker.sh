#!/usr/bin/env bash
# Start the Celery worker (filesystem broker)
cd "$(dirname "$0")"
mkdir -p data celery_data/in celery_data/out celery_data/processed
./venv/bin/celery -A app.tasks.scraper.celery_app worker --loglevel=info --pool=solo
