import uuid
from datetime import datetime
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from pydantic import BaseModel

from app.core.database import get_db
from app.models import ScrapeJob, ScrapeJobEvent, University, JobStatus, JobScope, ScrapeStage

router = APIRouter(prefix="/jobs", tags=["Scrape Jobs"])


class ScrapeJobCreate(BaseModel):
    scope: str = "SINGLE_UNIVERSITY"
    university_id: Optional[str] = None
    status_filters: Optional[List[str]] = None


class ScrapeJobOut(BaseModel):
    id: str
    scope: str
    university_id: Optional[str]
    university_name: Optional[str] = None
    status: str
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    created_at: datetime
    totals_json: Optional[dict]

    class Config:
        orm_mode = True


class ScrapeJobEventOut(BaseModel):
    id: str
    job_id: str
    university_id: Optional[str]
    stage: str
    message: Optional[str]
    timestamp: datetime
    counters_json: Optional[dict]

    class Config:
        orm_mode = True


class LiveJobOut(BaseModel):
    id: str
    scope: str
    university_id: Optional[str]
    university_name: Optional[str]
    status: str
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    created_at: datetime
    totals_json: Optional[dict]
    events: List[ScrapeJobEventOut] = []

    class Config:
        orm_mode = True


class PaginatedJobs(BaseModel):
    data: List[ScrapeJobOut]
    count: int


@router.get("", response_model=PaginatedJobs)
async def list_jobs(
    status: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    q = select(ScrapeJob)
    if status:
        q = q.where(ScrapeJob.status == status)
    total = (await db.execute(select(func.count()).select_from(q.subquery()))).scalar()
    offset = (page - 1) * limit
    result = await db.execute(q.order_by(ScrapeJob.created_at.desc()).offset(offset).limit(limit))
    return {"data": result.scalars().all(), "count": total}


@router.get("/active", response_model=List[ScrapeJobOut])
async def active_jobs(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(ScrapeJob)
        .where(ScrapeJob.status.in_([JobStatus.QUEUED, JobStatus.RUNNING]))
        .order_by(ScrapeJob.created_at.desc())
    )
    return result.scalars().all()


@router.get("/live", response_model=List[LiveJobOut])
async def live_jobs(
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    """Return recent jobs (active + last N finished) with university name + events.
    Designed for real-time dashboard polling — single request for everything."""
    jobs_result = await db.execute(
        select(ScrapeJob)
        .order_by(ScrapeJob.created_at.desc())
        .limit(limit)
    )
    jobs = jobs_result.scalars().all()

    # Batch load universities
    uni_ids = {j.university_id for j in jobs if j.university_id}
    unis = {}
    if uni_ids:
        u_result = await db.execute(
            select(University).where(University.id.in_(uni_ids))
        )
        for u in u_result.scalars().all():
            unis[u.id] = u.name_uz

    # Batch load events (last 20 per job)
    job_ids = [j.id for j in jobs]
    events_result = await db.execute(
        select(ScrapeJobEvent)
        .where(ScrapeJobEvent.job_id.in_(job_ids))
        .order_by(ScrapeJobEvent.timestamp.asc())
    )
    all_events = events_result.scalars().all()

    events_by_job: dict[str, list] = {}
    for ev in all_events:
        events_by_job.setdefault(ev.job_id, []).append(ev)

    result = []
    for job in jobs:
        job_events = events_by_job.get(job.id, [])[-20:]  # last 20
        result.append(LiveJobOut(
            id=job.id,
            scope=str(job.scope),
            university_id=job.university_id,
            university_name=unis.get(job.university_id) if job.university_id else None,
            status=str(job.status),
            started_at=job.started_at,
            finished_at=job.finished_at,
            created_at=job.created_at,
            totals_json=job.totals_json,
            events=[ScrapeJobEventOut(
                id=e.id,
                job_id=e.job_id,
                university_id=e.university_id,
                stage=str(e.stage),
                message=e.message,
                timestamp=e.timestamp,
                counters_json=e.counters_json,
            ) for e in job_events],
        ))

    return result


@router.post("", response_model=ScrapeJobOut, status_code=201)
async def create_job(
    data: ScrapeJobCreate,
    db: AsyncSession = Depends(get_db),
):
    from app.tasks.scraper import launch_scrape_job

    scope = JobScope.ALL_UNIVERSITIES if data.scope == "ALL_UNIVERSITIES" else JobScope.SINGLE_UNIVERSITY

    if scope == JobScope.SINGLE_UNIVERSITY and not data.university_id:
        raise HTTPException(status_code=400, detail="university_id required for SINGLE_UNIVERSITY scope")

    job = ScrapeJob(
        id=str(uuid.uuid4()),
        scope=scope,
        university_id=data.university_id,
        status=JobStatus.QUEUED,
    )
    db.add(job)
    await db.commit()
    await db.refresh(job)

    if scope == JobScope.SINGLE_UNIVERSITY:
        launch_scrape_job(data.university_id, job.id)
    else:
        # Queue child jobs for all universities with websites, then dispatch in parallel.
        q = select(University).where(University.website.isnot(None))
        if data.status_filters:
            q = q.where(University.scrape_status.in_(data.status_filters))
        result = await db.execute(q)
        universities = result.scalars().all()
        queued_count = 0
        for uni in universities:
            child_job = ScrapeJob(
                id=str(uuid.uuid4()),
                scope=JobScope.SINGLE_UNIVERSITY,
                university_id=uni.id,
                status=JobStatus.QUEUED,
            )
            db.add(child_job)
            await db.commit()
            await db.refresh(child_job)
            launch_scrape_job(uni.id, child_job.id)
            queued_count += 1

        job.status = JobStatus.DONE
        job.finished_at = datetime.utcnow()
        job.totals_json = {"queued": queued_count}
        await db.commit()
        await db.refresh(job)

    return job


@router.put("/{job_id}/cancel", response_model=ScrapeJobOut)
async def cancel_job(job_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(ScrapeJob).where(ScrapeJob.id == job_id))
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    job.status = JobStatus.CANCELLED
    job.finished_at = datetime.utcnow()
    await db.commit()
    await db.refresh(job)
    return job


@router.get("/{job_id}/events", response_model=List[ScrapeJobEventOut])
async def get_job_events(job_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(ScrapeJobEvent)
        .where(ScrapeJobEvent.job_id == job_id)
        .order_by(ScrapeJobEvent.timestamp.desc())
    )
    return result.scalars().all()
