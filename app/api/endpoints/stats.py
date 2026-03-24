from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from typing import List

from app.core.database import get_db
from app.models import University, NewsPost, MediaAsset, ScrapeJob, ScrapeStatus

router = APIRouter(prefix="/stats", tags=["Statistics"])


@router.get("")
async def get_stats(db: AsyncSession = Depends(get_db)):
    total_universities = (await db.execute(select(func.count(University.id)))).scalar()
    total_posts = (await db.execute(select(func.count(NewsPost.id)))).scalar()

    status_result = await db.execute(select(University.scrape_status, func.count()).group_by(University.scrape_status))
    by_status = {row[0].value if hasattr(row[0], 'value') else str(row[0]): row[1] for row in status_result.all()}

    return {
        "totalUniversities": total_universities,
        "totalPosts": total_posts,
        "byStatus": by_status,
    }


@router.get("/language-distribution")
async def language_distribution(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(NewsPost.language, func.count()).group_by(NewsPost.language)
    )
    rows = result.all()
    total = sum(r[1] for r in rows)
    label_map = {"uz": "O'zbek", "ru": "Русский", "en": "English", "unknown": "Noma'lum"}
    return [
        {
            "language": label_map.get(lang or "unknown", lang or "unknown"),
            "count": cnt,
            "percentage": round(cnt / total * 100) if total else 0,
        }
        for lang, cnt in rows
    ]


@router.get("/scraping-status-distribution")
async def scraping_status_distribution(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(University.scrape_status, func.count()).group_by(University.scrape_status)
    )
    rows = result.all()
    total = sum(r[1] for r in rows)
    return [
        {
            "status": status.value if hasattr(status, 'value') else str(status),
            "count": cnt,
            "percentage": round(cnt / total * 100) if total else 0,
        }
        for status, cnt in rows
    ]


@router.get("/top-universities")
async def top_universities(limit: int = 10, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(NewsPost.university_id, func.count().label("cnt"))
        .group_by(NewsPost.university_id)
        .order_by(func.count().desc())
        .limit(limit)
    )
    rows = result.all()
    out = []
    for uni_id, cnt in rows:
        uni_res = await db.execute(select(University).where(University.id == uni_id))
        uni = uni_res.scalar_one_or_none()
        out.append({
            "university_id": uni_id,
            "name": uni.name_uz if uni else "Unknown",
            "count": cnt,
        })
    return out


@router.get("/media")
async def media_stats(db: AsyncSession = Depends(get_db)):
    total_images = (await db.execute(select(func.count(MediaAsset.id)).where(MediaAsset.type == "image"))).scalar()
    total_videos = (await db.execute(select(func.count(MediaAsset.id)).where(MediaAsset.type == "video"))).scalar()
    posts_with_media = (await db.execute(
        select(func.count(NewsPost.id)).where(NewsPost.cover_image_id.isnot(None))
    )).scalar()
    return {
        "totalImages": total_images,
        "totalVideos": total_videos,
        "postsWithMedia": posts_with_media,
        "avgImagesPerPost": round(total_images / posts_with_media, 1) if posts_with_media else 0,
    }
