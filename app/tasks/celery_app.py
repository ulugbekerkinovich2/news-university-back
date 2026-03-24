import os
from celery import Celery

# Filesystem broker — no external server required
BROKER_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../celery_data"))
os.makedirs(f"{BROKER_DIR}/in", exist_ok=True)
os.makedirs(f"{BROKER_DIR}/out", exist_ok=True)
os.makedirs(f"{BROKER_DIR}/processed", exist_ok=True)

celery_app = Celery(
    "university_scraper",
    broker=f"filesystem://",
    backend="db+sqlite:///data/celery_results.db",
)

celery_app.conf.update(
    broker_transport_options={
        "data_folder_in": f"{BROKER_DIR}/in",
        "data_folder_out": f"{BROKER_DIR}/out",
        "processed_folder": f"{BROKER_DIR}/processed",
    },
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Tashkent",
    enable_utc=True,
    task_always_eager=False,
)
