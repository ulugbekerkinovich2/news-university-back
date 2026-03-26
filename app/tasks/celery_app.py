import os
from celery import Celery
from celery.schedules import crontab

# Filesystem broker — no external server required
BROKER_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../celery_data"))
os.makedirs(f"{BROKER_DIR}/in", exist_ok=True)
os.makedirs(f"{BROKER_DIR}/out", exist_ok=True)
os.makedirs(f"{BROKER_DIR}/processed", exist_ok=True)

celery_app = Celery(
    "university_scraper",
    broker=f"filesystem://",
    backend=os.getenv("CELERY_RESULT_BACKEND", "db+postgresql+psycopg2://m3:postgres@localhost:5432/university_hub") if os.getenv("DATABASE_URL") else "db+sqlite:///data/celery_results.db",
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
    # ── Celery Beat: auto-scrape all universities every 6 hours ──
    beat_schedule={
        "scrape-all-universities-every-6h": {
            "task": "tasks.scrape_all_universities",
            "schedule": crontab(minute=0, hour="*/6"),
        },
        "cleanup-old-jobs-daily": {
            "task": "tasks.cleanup_old_jobs",
            "schedule": crontab(minute=30, hour=3),
        },
    },
    beat_scheduler="celery.beat:PersistentScheduler",
    beat_schedule_filename="celery_data/celerybeat-schedule",
)

@celery_app.task(name="tasks.cleanup_old_jobs")
def cleanup_old_jobs():
    """Delete scrape jobs and events older than 7 days to save space."""
    from sqlalchemy import create_engine, delete
    from sqlalchemy.orm import Session
    from datetime import datetime, timedelta
    import os
    
    db_url = os.getenv("DATABASE_URL", "sqlite:///./data/app.db")
    if "+asyncpg" in db_url:
        db_url = db_url.replace("+asyncpg", "")
        
    engine = create_engine(db_url)
    from app.models import ScrapeJob, ScrapeJobEvent
    
    threshold = datetime.utcnow() - timedelta(days=7)
    
    with Session(engine) as session:
        # Delete events first (foreign key)
        session.execute(delete(ScrapeJobEvent).where(ScrapeJobEvent.created_at < threshold))
        # Delete jobs
        session.execute(delete(ScrapeJob).where(ScrapeJob.created_at < threshold))
        session.commit()
