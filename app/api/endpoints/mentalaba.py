import asyncio
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.endpoints.auth import require_permission
from app.api.endpoints.news import NewsPostOut
from app.core.database import AsyncSessionLocal, get_db
from app.models import NewsPost
from app.services.mentalaba import (
    deactivate_exported_post,
    export_post_to_mentalaba,
    get_cached_tags,
    get_overview,
    get_export_mode,
    has_mentalaba_token,
    load_exportable_posts,
    prepare_syndication,
    refresh_post_syndication_state,
    set_export_mode,
    sync_tags,
    sync_universities,
)

router = APIRouter(
    prefix="/mentalaba",
    tags=["Mentalaba Export"],
    dependencies=[Depends(require_permission("manage_news"))],
)


class MentalabaSettingsPatch(BaseModel):
    export_mode: str


class MentalabaPostStatusPatch(BaseModel):
    syndication_status: str


class MentalabaSelectedSendPayload(BaseModel):
    post_ids: List[str]


class MentalabaSendResult(BaseModel):
    processed: int
    exported: int
    failed: int


class MentalabaQueueItem(BaseModel):
    post: NewsPostOut
    is_exportable: bool
    export_reason: str
    suggested_tag_ids: List[int]
    suggested_tags: List[str]
    cover_image_url: Optional[str]
    mentalaba_university_id: Optional[int]


class PaginatedMentalabaQueue(BaseModel):
    data: List[MentalabaQueueItem]
    count: int


@router.get("/overview")
async def overview(db: AsyncSession = Depends(get_db)):
    return await get_overview(db)


@router.patch("/settings")
async def patch_settings(payload: MentalabaSettingsPatch, db: AsyncSession = Depends(get_db)):
    try:
        mode = await set_export_mode(db, payload.export_mode)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"export_mode": mode}


@router.post("/sync/tags")
async def sync_remote_tags(db: AsyncSession = Depends(get_db)):
    if not has_mentalaba_token():
        raise HTTPException(status_code=400, detail="Mentalaba token configured emas")
    try:
        return await sync_tags(db)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Tag sync xatosi: {exc}") from exc


@router.post("/sync/universities")
async def sync_remote_universities(db: AsyncSession = Depends(get_db)):
    if not has_mentalaba_token():
        raise HTTPException(status_code=400, detail="Mentalaba token configured emas")
    try:
        return await sync_universities(db)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"University sync xatosi: {exc}") from exc


@router.get("/news-queue", response_model=PaginatedMentalabaQueue)
async def news_queue(
    syndication_status: str = Query("PENDING"),
    syndication_statuses: Optional[str] = Query(None),
    eligible_only: bool = Query(False),
    university_id: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    parsed_statuses = [item.strip() for item in (syndication_statuses or "").split(",") if item.strip()]
    posts, count = await load_exportable_posts(
        db,
        syndication_status=syndication_status,
        syndication_statuses=parsed_statuses,
        university_id=university_id,
        search=search,
        page=page,
        limit=limit,
    )
    tags = await get_cached_tags(db)
    data: List[Dict[str, Any]] = []
    for post in posts:
        prepared = prepare_syndication(post, tags)
        if eligible_only and not prepared["is_exportable"]:
            continue
        data.append(
            {
                "post": post,
                **prepared,
            }
        )
    return {"data": data, "count": len(data) if eligible_only else count}


@router.put("/news/{post_id}/status")
async def update_queue_status(post_id: str, payload: MentalabaPostStatusPatch, db: AsyncSession = Depends(get_db)):
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

    normalized = (payload.syndication_status or "").upper()
    if normalized not in {"DRAFT", "PENDING", "REJECTED", "FAILED"}:
        raise HTTPException(status_code=400, detail="Invalid syndication status")

    if normalized == "REJECTED" and post.syndication_remote_id:
        post = await deactivate_exported_post(db, post)
        return post

    post.syndication_status = normalized
    if normalized != "FAILED":
        post.syndication_last_error = None
    await db.commit()
    await db.refresh(post)
    return post


@router.post("/news/{post_id}/send")
async def send_post(post_id: str, db: AsyncSession = Depends(get_db)):
    if not has_mentalaba_token():
        raise HTTPException(status_code=400, detail="Mentalaba token configured emas")
    post = await export_post_to_mentalaba(db, post_id)
    return {
        "id": post.id,
        "syndication_status": post.syndication_status,
        "syndication_remote_id": post.syndication_remote_id,
        "syndication_last_error": post.syndication_last_error,
    }


@router.post("/news/send-bulk", response_model=MentalabaSendResult)
async def send_bulk(
    limit: int = Query(20, ge=1, le=100),
    university_id: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    if not has_mentalaba_token():
        raise HTTPException(status_code=400, detail="Mentalaba token configured emas")

    result = await db.execute(
        select(NewsPost.id)
        .where(NewsPost.syndication_status == "PENDING")
        .where(NewsPost.university_id == university_id if university_id else True)
        .order_by(NewsPost.published_at.desc().nullsfirst(), NewsPost.created_at.desc())
        .limit(limit)
    )
    post_ids = result.scalars().all()
    exported = 0
    failed = 0
    for post_id in post_ids:
        post = await export_post_to_mentalaba(db, post_id)
        if post.syndication_status == "EXPORTED":
            exported += 1
        else:
            failed += 1
    return {"processed": len(post_ids), "exported": exported, "failed": failed}


@router.post("/news/send-selected", response_model=MentalabaSendResult)
async def send_selected(
    payload: MentalabaSelectedSendPayload,
    db: AsyncSession = Depends(get_db),
):
    if not has_mentalaba_token():
        raise HTTPException(status_code=400, detail="Mentalaba token configured emas")

    post_ids = [post_id for post_id in payload.post_ids if post_id]
    exported = 0
    failed = 0
    for post_id in post_ids:
        post = await export_post_to_mentalaba(db, post_id)
        if post.syndication_status == "EXPORTED":
            exported += 1
        else:
            failed += 1
    return {"processed": len(post_ids), "exported": exported, "failed": failed}


async def trigger_auto_send(post_id: str) -> None:
    async with AsyncSessionLocal() as db:
        await export_post_to_mentalaba(db, post_id)


async def update_post_after_moderation(post_id: str) -> None:
    async with AsyncSessionLocal() as db:
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
            return
        if post.moderation_status in {"REJECTED", "TRASH"} and post.syndication_remote_id:
            await deactivate_exported_post(db, post)
            return
        await refresh_post_syndication_state(db, post)
        mode = await get_export_mode(db)
        await db.commit()
        if mode == "auto" and post.syndication_status == "PENDING" and has_mentalaba_token():
            asyncio.create_task(trigger_auto_send(post_id))
