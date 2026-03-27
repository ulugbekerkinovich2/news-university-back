from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload
from typing import Optional, List
from pydantic import BaseModel
from datetime import datetime
import re

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
    mt_id: Optional[int]
    mt_slug: Optional[str]

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


def _slugify(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    normalized = value.lower()
    normalized = normalized.replace("o‘", "o").replace("o'", "o")
    normalized = normalized.replace("g‘", "g").replace("g'", "g")
    normalized = normalized.replace("ʼ", "").replace("`", "")
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized)
    normalized = re.sub(r"-{2,}", "-", normalized).strip("-")
    return normalized or None


async def _resolve_university_ids(
    db: AsyncSession,
    *,
    slug: Optional[str] = None,
    university_mt_id: Optional[int] = None,
) -> List[str]:
    if university_mt_id is not None:
        return (await db.execute(
            select(University.id).where(University.mt_id == university_mt_id)
        )).scalars().all()

    if not slug:
        return []

    exact_ids = (await db.execute(
        select(University.id).where(University.mt_slug == slug)
    )).scalars().all()
    if exact_ids:
        return exact_ids

    universities = (await db.execute(
        select(University.id, University.name_uz, University.name_en, University.name_ru)
    )).all()

    matched_ids: List[str] = []
    for university_id, name_uz, name_en, name_ru in universities:
        candidates = {
            _slugify(name_uz),
            _slugify(name_en),
            _slugify(name_ru),
        }
        if slug in candidates:
            matched_ids.append(university_id)

    return matched_ids


@router.get("", response_model=PaginatedNews)
async def list_news(
    university_id: Optional[str] = Query(None),
    university_mt_id: Optional[int] = Query(None),
    slug: Optional[str] = Query(None),
    mt_id: Optional[int] = Query(None),
    mt_slug: Optional[str] = Query(None),
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
    
    resolved_mt_id = university_mt_id if university_mt_id is not None else mt_id
    resolved_slug = slug if slug is not None else mt_slug

    if resolved_mt_id or resolved_slug:
        uni_ids = await _resolve_university_ids(
            db,
            slug=resolved_slug,
            university_mt_id=resolved_mt_id,
        )
        q = q.where(NewsPost.university_id.in_(uni_ids))

    if region_id:
        uni_q = select(University.id).where(University.region_id == region_id)
        uni_ids = (await db.execute(uni_q)).scalars().all()
        q = q.where(NewsPost.university_id.in_(uni_ids))
    if search:
        # PostgreSQL Full-Text Search with ranking
        ts_vector = func.to_tsvector('simple', NewsPost.title + ' ' + func.coalesce(NewsPost.content_text, ''))
        ts_query = func.plainto_tsquery('simple', search)
        q = q.where(ts_vector.op('@@')(ts_query))
        # Order by rank primarily, then by date
        q = q.order_by(func.ts_rank(ts_vector, ts_query).desc())
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
