from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, or_
from sqlalchemy.orm import selectinload
from typing import Optional, List
from pydantic import BaseModel
from datetime import datetime

from app.core.database import get_db
from app.models import University, ScrapeStatus

router = APIRouter(prefix="/universities", tags=["Universities"])


class UniversityOut(BaseModel):
    id: str
    region_id: Optional[str]
    name_uz: str
    name_en: Optional[str]
    name_ru: Optional[str]
    website: Optional[str]
    logo_url: Optional[str]
    scrape_status: str
    last_scraped_at: Optional[datetime]
    last_error_message: Optional[str]
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True


class UniversityCreate(BaseModel):
    id: str
    region_id: Optional[str] = None
    name_uz: str
    name_en: Optional[str] = None
    name_ru: Optional[str] = None
    website: Optional[str] = None


class UniversityUpdate(BaseModel):
    region_id: Optional[str] = None
    name_uz: Optional[str] = None
    name_en: Optional[str] = None
    name_ru: Optional[str] = None
    website: Optional[str] = None
    logo_url: Optional[str] = None
    scrape_status: Optional[str] = None


class PaginatedUniversities(BaseModel):
    data: List[UniversityOut]
    count: int


@router.get("", response_model=PaginatedUniversities)
async def list_universities(
    search: Optional[str] = Query(None),
    region_id: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    q = select(University)
    if search:
        q = q.where(
            or_(
                University.name_uz.ilike(f"%{search}%"),
                University.name_en.ilike(f"%{search}%"),
                University.name_ru.ilike(f"%{search}%"),
            )
        )
    if region_id:
        q = q.where(University.region_id == region_id)
    if status:
        q = q.where(University.scrape_status == status)

    total_q = select(func.count()).select_from(q.subquery())
    count = (await db.execute(total_q)).scalar()

    offset = (page - 1) * limit
    q = q.order_by(University.name_uz).offset(offset).limit(limit)
    result = await db.execute(q)
    items = result.scalars().all()
    return {"data": items, "count": count}


@router.get("/{university_id}", response_model=UniversityOut)
async def get_university(university_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(University).where(University.id == university_id))
    uni = result.scalar_one_or_none()
    if not uni:
        raise HTTPException(status_code=404, detail="University not found")
    return uni


@router.post("", response_model=UniversityOut, status_code=201)
async def create_university(data: UniversityCreate, db: AsyncSession = Depends(get_db)):
    uni = University(
        id=data.id,
        region_id=data.region_id,
        name_uz=data.name_uz,
        name_en=data.name_en,
        name_ru=data.name_ru,
        website=data.website,
        scrape_status=ScrapeStatus.NO_SOURCE if not data.website else ScrapeStatus.IDLE,
    )
    db.add(uni)
    await db.commit()
    await db.refresh(uni)
    return uni


@router.put("/{university_id}", response_model=UniversityOut)
async def update_university(university_id: str, data: UniversityUpdate, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(University).where(University.id == university_id))
    uni = result.scalar_one_or_none()
    if not uni:
        raise HTTPException(status_code=404, detail="University not found")
    for field, val in data.dict(exclude_none=True).items():
        setattr(uni, field, val)
    uni.updated_at = datetime.utcnow()
    await db.commit()
    await db.refresh(uni)
    return uni


@router.delete("/{university_id}", status_code=204)
async def delete_university(university_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(University).where(University.id == university_id))
    uni = result.scalar_one_or_none()
    if not uni:
        raise HTTPException(status_code=404, detail="University not found")
    await db.delete(uni)
    await db.commit()


@router.get("/regions/list")
async def get_regions(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(University.region_id)
        .where(University.region_id.isnot(None))
        .distinct()
    )
    return sorted([r for (r,) in result.all() if r])
