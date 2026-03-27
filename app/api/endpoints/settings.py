"""
Settings API — GET/PATCH system configuration.
Key settings:
  scrape_interval_hours  (1, 2, 4, 6, 12, 24)  default: 6
  scrape_enabled         (true/false)           default: true
"""
from datetime import datetime
from typing import Dict, Optional
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from pydantic import BaseModel

from app.api.endpoints.auth import require_permission
from app.core.database import get_db
from app.models import SystemSetting

router = APIRouter(
    prefix="/settings",
    tags=["Settings"],
    dependencies=[Depends(require_permission("manage_settings"))],
)

# Default values — returned when setting not in DB yet
DEFAULTS: Dict[str, str] = {
    "scrape_interval_hours": "6",
    "scrape_enabled": "true",
    "scrape_max_links": "40",
}


async def _get_all(db: AsyncSession) -> Dict[str, str]:
    result = await db.execute(select(SystemSetting))
    rows = {r.key: r.value for r in result.scalars().all()}
    # Merge with defaults (defaults fill missing keys)
    merged = {**DEFAULTS, **rows}
    return merged


@router.get("")
async def get_settings(db: AsyncSession = Depends(get_db)):
    return await _get_all(db)


class SettingsPatch(BaseModel):
    scrape_interval_hours: Optional[str] = None
    scrape_enabled: Optional[str] = None
    scrape_max_links: Optional[str] = None


@router.patch("")
async def update_settings(data: SettingsPatch, db: AsyncSession = Depends(get_db)):
    updates = data.dict(exclude_none=True)

    for key, value in updates.items():
        result = await db.execute(select(SystemSetting).where(SystemSetting.key == key))
        existing = result.scalar_one_or_none()
        if existing:
            existing.value = str(value)
            existing.updated_at = datetime.utcnow()
        else:
            db.add(SystemSetting(key=key, value=str(value)))

    await db.commit()

    # Write Celery Beat config file so worker picks it up on next restart
    _write_celery_config(updates.get("scrape_interval_hours"))

    return await _get_all(db)


def _write_celery_config(interval_hours: Optional[str] = None):
    """Write a simple JSON config that celery_app.py reads at startup."""
    import json, os
    config_path = "celery_data/schedule_config.json"
    os.makedirs("celery_data", exist_ok=True)
    current = {}
    if os.path.exists(config_path):
        try:
            with open(config_path) as f:
                current = json.load(f)
        except Exception:
            pass
    if interval_hours is not None:
        current["scrape_interval_hours"] = int(interval_hours)
    with open(config_path, "w") as f:
        json.dump(current, f, indent=2)
