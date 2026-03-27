import uuid
from typing import Optional, List
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from pydantic import BaseModel

from app.api.endpoints.auth import require_permission
from app.core.database import get_db
from app.models import ApiKey, User

router = APIRouter(
    prefix="/api-keys",
    tags=["API Keys"],
    dependencies=[Depends(require_permission("manage_api_keys"))],
)


class ApiKeyCreate(BaseModel):
    name: str
    key_hash: str
    key_prefix: str
    rate_limit_per_minute: Optional[int] = 60
    rate_limit_per_day: Optional[int] = 10000
    expires_at: Optional[str] = None


class ApiKeyUpdate(BaseModel):
    name: Optional[str] = None
    rate_limit_per_minute: Optional[int] = None
    rate_limit_per_day: Optional[int] = None
    is_active: Optional[bool] = None
    expires_at: Optional[str] = None


class ApiKeyOut(BaseModel):
    id: str
    name: str
    key_prefix: str
    rate_limit_per_minute: int
    rate_limit_per_day: int
    is_active: bool
    last_used_at: Optional[datetime]
    request_count: int
    created_at: datetime
    expires_at: Optional[datetime]

    class Config:
        orm_mode = True


@router.get("", response_model=List[ApiKeyOut])
async def list_api_keys(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(ApiKey).order_by(ApiKey.created_at.desc()))
    return result.scalars().all()


@router.post("", response_model=ApiKeyOut, status_code=201)
async def create_api_key(data: ApiKeyCreate, db: AsyncSession = Depends(get_db)):
    key = ApiKey(
        id=str(uuid.uuid4()),
        name=data.name,
        key_hash=data.key_hash,
        key_prefix=data.key_prefix,
        rate_limit_per_minute=data.rate_limit_per_minute,
        rate_limit_per_day=data.rate_limit_per_day,
        expires_at=datetime.fromisoformat(data.expires_at) if data.expires_at else None,
    )
    db.add(key)
    await db.commit()
    await db.refresh(key)
    return key


@router.put("/{key_id}", response_model=ApiKeyOut)
async def update_api_key(key_id: str, data: ApiKeyUpdate, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(ApiKey).where(ApiKey.id == key_id))
    key = result.scalar_one_or_none()
    if not key:
        raise HTTPException(status_code=404, detail="API key not found")
    for field, val in data.dict(exclude_none=True).items():
        setattr(key, field, val)
    key.updated_at = datetime.utcnow()
    await db.commit()
    await db.refresh(key)
    return key


@router.delete("/{key_id}", status_code=204)
async def delete_api_key(key_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(ApiKey).where(ApiKey.id == key_id))
    key = result.scalar_one_or_none()
    if not key:
        raise HTTPException(status_code=404, detail="API key not found")
    await db.delete(key)
    await db.commit()
