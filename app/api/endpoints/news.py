from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, or_
from sqlalchemy.orm import selectinload
from typing import Optional, List
from pydantic import BaseModel
from datetime import datetime

from app.core.database import get_db
from app.models import NewsPost, MediaAsset, University

router = APIRouter(prefix="/news", tags=["News"])


class MediaAssetOut(BaseModel):
    id: str
    type: str
    original_url: str
    stored_url: Optional[str]
    provider: Optional[str]

    class Config:
        orm_mode = True


class UniversitySmall(BaseModel):
    id: str
    name_uz: str
    name_en: Optional[str]
    name_ru: Optional[str]
    region_id: Optional[str]
    website: Optional[str]
    logo_url: Optional[str]

    class Config:
        orm_mode = True


class NewsPostOut(BaseModel):
    id: str
    university_id: str
    title: str
    slug: str
    summary: Optional[str]
    content_html: Optional[str]
    content_text: Optional[str]
    published_at: Optional[datetime]
    source_url: str
    canonical_url: Optional[str]
    language: Optional[str]
    cover_image_id: Optional[str]
    created_at: datetime
    updated_at: datetime
    university: Optional[UniversitySmall]
    cover_image: Optional[MediaAssetOut]
    media_assets: List[MediaAssetOut] = []

    class Config:
        orm_mode = True


class PaginatedNews(BaseModel):
    data: List[NewsPostOut]
    count: int


@router.get("", response_model=PaginatedNews)
async def list_news(
    university_id: Optional[str] = Query(None),
    region_id: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    language: Optional[str] = Query(None),
    from_date: Optional[str] = Query(None),
    to_date: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    q = select(NewsPost).options(
        selectinload(NewsPost.university),
        selectinload(NewsPost.cover_image),
        selectinload(NewsPost.media_assets),
    )
    if university_id:
        q = q.where(NewsPost.university_id == university_id)
    if region_id:
        uni_q = select(University.id).where(University.region_id == region_id)
        uni_ids = (await db.execute(uni_q)).scalars().all()
        q = q.where(NewsPost.university_id.in_(uni_ids))
    if search:
        q = q.where(NewsPost.title.ilike(f"%{search}%"))
    if language and language != "all":
        q = q.where(NewsPost.language == language)
    if from_date:
        q = q.where(NewsPost.published_at >= from_date)
    if to_date:
        q = q.where(NewsPost.published_at <= to_date)

    total_q = select(func.count()).select_from(q.subquery())
    count = (await db.execute(total_q)).scalar()

    offset = (page - 1) * limit
    q = q.order_by(NewsPost.published_at.desc().nullsfirst()).offset(offset).limit(limit)
    result = await db.execute(q)
    return {"data": result.scalars().all(), "count": count}


@router.get("/{post_id}", response_model=NewsPostOut)
async def get_news(post_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(NewsPost)
        .options(
            selectinload(NewsPost.university),
            selectinload(NewsPost.cover_image),
            selectinload(NewsPost.media_assets),
        )
        .where(NewsPost.id == post_id)
    )
    post = result.scalar_one_or_none()
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    return post


@router.delete("/{post_id}", status_code=204)
async def delete_news(post_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(NewsPost).where(NewsPost.id == post_id))
    post = result.scalar_one_or_none()
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    await db.delete(post)
    await db.commit()
