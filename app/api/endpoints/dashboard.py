from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from typing import List, Optional
from pydantic import BaseModel

from app.core.database import get_db
from app.models import University, NewsPost
from .universities import UniversityOut, PaginatedUniversities

router = APIRouter(prefix="/dashboard", tags=["Dashboard"])

class DashboardInitResponse(BaseModel):
    stats: dict
    regions: List[str]
    universities: PaginatedUniversities

@router.get("/init", response_model=DashboardInitResponse)
async def dashboard_init(db: AsyncSession = Depends(get_db)):
    """Aggregate stats, regions and initial universities for faster dashboard load."""
    
    # 1. Stats
    total_universities = (await db.execute(select(func.count(University.id)))).scalar()
    total_posts = (await db.execute(select(func.count(NewsPost.id)))).scalar()
    status_result = await db.execute(select(University.scrape_status, func.count()).group_by(University.scrape_status))
    by_status = {row[0].value if hasattr(row[0], 'value') else str(row[0]): row[1] for row in status_result.all()}
    
    stats = {
        "totalUniversities": total_universities,
        "totalPosts": total_posts,
        "byStatus": by_status,
    }

    # 2. Regions
    regions_res = await db.execute(
        select(University.region_id).where(University.region_id.isnot(None)).distinct()
    )
    regions = sorted([r for (r,) in regions_res.all() if r])

    # 3. First 12 Universities (Limit matched with frontend Index.tsx LIMIT=12)
    uni_q = select(University).order_by(University.name_uz).limit(12)
    uni_res = await db.execute(uni_q)
    universities_list = uni_res.scalars().all()
    
    return {
        "stats": stats,
        "regions": regions,
        "universities": {
            "data": universities_list,
            "count": total_universities
        }
    }
