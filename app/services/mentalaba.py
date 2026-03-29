import json
import mimetypes
import os
import re
from datetime import datetime
from html import escape
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import httpx
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import MediaAsset, NewsPost, SystemSetting, University


MENTALABA_BASE_URL = os.getenv("MENTALABA_API_BASE", "https://api.mentalaba.uz/v1").rstrip("/")
MENTALABA_TOKEN = os.getenv("MENTALABA_API_TOKEN", "")
MENTALABA_UPLOAD_ASSOCIATED_WITH = os.getenv("MENTALABA_UPLOAD_ASSOCIATED_WITH", "test")
MENTALABA_UPLOAD_USAGE = os.getenv("MENTALABA_UPLOAD_USAGE", "test_gallery")

EXPORT_MODE_KEY = "mentalaba_export_mode"
TAGS_CACHE_KEY = "mentalaba_tags_cache"
UNIVERSITIES_CACHE_KEY = "mentalaba_universities_cache"
LAST_TAGS_SYNC_KEY = "mentalaba_tags_last_synced_at"
LAST_UNIVERSITIES_SYNC_KEY = "mentalaba_universities_last_synced_at"


def _normalize_text(value: Optional[str]) -> str:
    text = (value or "").lower().strip()
    text = text.replace("o‘", "o").replace("o'", "o")
    text = text.replace("g‘", "g").replace("g'", "g")
    text = text.replace("ʼ", "").replace("`", "")
    text = re.sub(r"\s+", " ", text)
    return text


def _slugify(value: Optional[str]) -> str:
    normalized = _normalize_text(value)
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized)
    normalized = re.sub(r"-{2,}", "-", normalized).strip("-")
    return normalized


def _strip_html(value: Optional[str]) -> str:
    raw = re.sub(r"<[^>]+>", " ", value or "")
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw


def _html_from_text(value: Optional[str]) -> str:
    text = _strip_html(value)
    if not text:
        return ""
    paragraphs = [segment.strip() for segment in re.split(r"\n{2,}", text) if segment.strip()]
    if not paragraphs:
        paragraphs = [text]
    return "".join(f"<p>{escape(paragraph)}</p>" for paragraph in paragraphs)


def _dedupe_ints(values: List[int]) -> List[int]:
    seen = set()
    result: List[int] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


async def _get_setting(db: AsyncSession, key: str, default: Optional[str] = None) -> Optional[str]:
    result = await db.execute(select(SystemSetting).where(SystemSetting.key == key))
    row = result.scalar_one_or_none()
    return row.value if row else default


async def _set_setting(db: AsyncSession, key: str, value: str, description: Optional[str] = None) -> None:
    result = await db.execute(select(SystemSetting).where(SystemSetting.key == key))
    row = result.scalar_one_or_none()
    if row:
        row.value = value
        row.description = description or row.description
        row.updated_at = datetime.utcnow()
    else:
        db.add(SystemSetting(key=key, value=value, description=description))


async def get_export_mode(db: AsyncSession) -> str:
    return (await _get_setting(db, EXPORT_MODE_KEY, "manual") or "manual").lower()


async def set_export_mode(db: AsyncSession, mode: str) -> str:
    normalized = (mode or "manual").lower()
    if normalized not in {"manual", "auto"}:
        raise ValueError("Invalid export mode")
    await _set_setting(db, EXPORT_MODE_KEY, normalized, "Mentalaba export mode")
    await db.commit()
    return normalized


def has_mentalaba_token() -> bool:
    return bool(MENTALABA_TOKEN.strip())


def _auth_headers() -> Dict[str, str]:
    if not has_mentalaba_token():
        raise RuntimeError("Mentalaba API token is not configured")
    return {"Authorization": f"Bearer {MENTALABA_TOKEN}"}


async def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=45.0, headers=_auth_headers())


async def sync_tags(db: AsyncSession) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=45.0, headers=_auth_headers()) as client:
        response = await client.get(f"{MENTALABA_BASE_URL}/tags", params={"offset": 0, "limit": 100})
        response.raise_for_status()
        payload = response.json()
    tags = payload.get("tags", [])
    await _set_setting(db, TAGS_CACHE_KEY, json.dumps(tags), "Mentalaba tags cache")
    await _set_setting(db, LAST_TAGS_SYNC_KEY, datetime.utcnow().isoformat(), "Mentalaba tags last synced at")
    await db.commit()
    return {"count": len(tags), "pageInfo": payload.get("pageInfo", {})}


async def get_cached_tags(db: AsyncSession) -> List[Dict[str, Any]]:
    raw = await _get_setting(db, TAGS_CACHE_KEY, "[]")
    try:
        parsed = json.loads(raw or "[]")
    except json.JSONDecodeError:
        parsed = []
    return parsed if isinstance(parsed, list) else []


async def sync_universities(db: AsyncSession) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=45.0, headers=_auth_headers()) as client:
        response = await client.get(f"{MENTALABA_BASE_URL}/universities/getAll", params={"offset": 0, "limit": 1000})
        response.raise_for_status()
        payload = response.json()
    universities = payload if isinstance(payload, list) else []
    await _set_setting(db, UNIVERSITIES_CACHE_KEY, json.dumps(universities), "Mentalaba universities cache")
    await _set_setting(db, LAST_UNIVERSITIES_SYNC_KEY, datetime.utcnow().isoformat(), "Mentalaba universities last synced at")

    local_result = await db.execute(select(University))
    local_universities = local_result.scalars().all()
    index: Dict[str, int] = {}
    for item in universities:
        for key in ("full_name_uz", "full_name_ru", "full_name_en"):
            normalized = _normalize_text(item.get(key))
            if normalized and normalized not in index:
                index[normalized] = int(item["id"])

    matched = 0
    unmatched = 0
    for university in local_universities:
        target_id = None
        for candidate in (university.name_uz, university.name_ru, university.name_en):
            normalized = _normalize_text(candidate)
            if normalized in index:
                target_id = index[normalized]
                break
        university.mentalaba_id = target_id
        if target_id:
            matched += 1
        else:
            unmatched += 1

    await db.commit()
    return {"count": len(universities), "matched": matched, "unmatched": unmatched}


async def get_cached_remote_universities(db: AsyncSession) -> List[Dict[str, Any]]:
    raw = await _get_setting(db, UNIVERSITIES_CACHE_KEY, "[]")
    try:
        parsed = json.loads(raw or "[]")
    except json.JSONDecodeError:
        parsed = []
    return parsed if isinstance(parsed, list) else []


def _pick_cover_image(post: NewsPost) -> Optional[str]:
    candidates: List[Optional[str]] = []
    if post.cover_image:
        candidates.extend([post.cover_image.stored_url, post.cover_image.original_url])
    for asset in post.media_assets or []:
        if getattr(asset, "type", None) == "image":
            candidates.extend([asset.stored_url, asset.original_url])
    for candidate in candidates:
        if candidate and candidate.strip():
            return candidate.strip()
    return None


def _resolve_local_static_path(image_url: str) -> Optional[Path]:
    if image_url.startswith("/static/"):
        return Path("static") / image_url.removeprefix("/static/")
    if image_url.startswith("static/"):
        return Path(image_url)
    return None


async def _upload_remote_image(image_url: str) -> str:
    local_path = _resolve_local_static_path(image_url)
    if local_path and local_path.exists():
        file_bytes = local_path.read_bytes()
        filename = local_path.name
    else:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(image_url)
            response.raise_for_status()
            file_bytes = response.content
            filename = Path(image_url.split("?")[0]).name or "image.jpg"

    mime_type, _ = mimetypes.guess_type(filename)
    files = {"file": (filename, file_bytes, mime_type or "image/jpeg")}
    data = {
        "associated_with": MENTALABA_UPLOAD_ASSOCIATED_WITH,
        "usage": MENTALABA_UPLOAD_USAGE,
    }
    async with httpx.AsyncClient(timeout=60.0, headers=_auth_headers()) as client:
        response = await client.post(f"{MENTALABA_BASE_URL}/images/upload", data=data, files=files)
        if response.status_code >= 400:
            raise RuntimeError(
                f"Image upload failed ({response.status_code}): {response.text}"
            )
        payload = response.json()
    path = payload.get("path")
    if not path:
        raise RuntimeError("Mentalaba image upload returned no path")
    return str(path)


async def update_remote_news_status(remote_id: str, status_value: str) -> Dict[str, Any]:
    payload = {"status": status_value}
    last_error: Optional[Exception] = None
    async with httpx.AsyncClient(timeout=45.0, headers=_auth_headers()) as client:
        for method in (client.patch, client.put):
            try:
                response = await method(f"{MENTALABA_BASE_URL}/news/{remote_id}", json=payload)
                if response.status_code >= 400:
                    raise RuntimeError(f"{response.status_code}: {response.text}")
                return response.json()
            except Exception as exc:
                last_error = exc
    raise RuntimeError(f"Remote status update failed: {last_error}")


def _build_meta(title: str, text: str) -> Dict[str, str]:
    clean_title = (title or "").strip()[:255]
    clean_text = (text or "").strip()
    description = clean_text[:160] if clean_text else clean_title
    keywords = ", ".join([part for part in [clean_title, description[:60]] if part])
    return {
        "title": clean_title,
        "description": description,
        "keywords": keywords[:255],
    }


def match_tag_ids(post: NewsPost, tags: List[Dict[str, Any]]) -> List[int]:
    corpus = _normalize_text(" ".join(filter(None, [post.title, post.summary, post.content_text])))
    matched: List[int] = []
    for tag in tags:
        if tag.get("status") != "active":
            continue
        values = [
            _normalize_text(tag.get("tag_uz")),
            _normalize_text(tag.get("tag_ru")),
            _normalize_text(tag.get("tag_en")),
        ]
        for value in values:
            if value and value in corpus:
                matched.append(int(tag["id"]))
                break

    fallbacks: List[int] = []
    for tag in tags:
        value_uz = _normalize_text(tag.get("tag_uz"))
        value_en = _normalize_text(tag.get("tag_en"))
        if value_uz == "ta'lim" or value_en == "education":
            fallbacks.append(int(tag["id"]))
        if value_uz.strip() == "universitet" or value_en == "university":
            fallbacks.append(int(tag["id"]))

    if "qabul" in corpus or "admission" in corpus or "abituriyent" in corpus:
        for tag in tags:
            tag_blob = " ".join(
                _normalize_text(tag.get(key)) for key in ("tag_uz", "tag_ru", "tag_en")
            )
            if "qabul" in tag_blob or "admission" in tag_blob or "abituriyent" in tag_blob:
                matched.append(int(tag["id"]))

    if "grant" in corpus or "stipend" in corpus or "scholarship" in corpus:
        for tag in tags:
            tag_blob = " ".join(
                _normalize_text(tag.get(key)) for key in ("tag_uz", "tag_ru", "tag_en")
            )
            if "grant" in tag_blob or "stipend" in tag_blob or "scholarship" in tag_blob:
                matched.append(int(tag["id"]))

    return _dedupe_ints(matched or fallbacks)[:6]


def _tag_labels(tag_ids: List[int], tags: List[Dict[str, Any]]) -> List[str]:
    labels: List[str] = []
    for tag in tags:
        if int(tag["id"]) in tag_ids:
            labels.append(tag.get("tag_uz") or tag.get("tag_en") or tag.get("tag_ru") or str(tag["id"]))
    return labels


def is_exportable(post: NewsPost) -> Tuple[bool, str]:
    if post.moderation_status != "APPROVED":
        return False, "Post hali approve qilinmagan"
    if post.syndication_status == "REJECTED":
        return False, "Eksport uchun reject qilingan"
    if not _pick_cover_image(post):
        return False, "Sifatli rasm topilmadi"
    if not post.university or not post.university.mentalaba_id:
        return False, "Mentalaba universitet mapping topilmadi"
    return True, "Tayyor"


def prepare_syndication(post: NewsPost, tags: List[Dict[str, Any]]) -> Dict[str, Any]:
    exportable, reason = is_exportable(post)
    suggested_tag_ids = match_tag_ids(post, tags)
    if exportable and not suggested_tag_ids:
        exportable = False
        reason = "Tag mapping topilmadi, avval tag sync qiling"
    return {
        "is_exportable": exportable,
        "export_reason": reason,
        "suggested_tag_ids": suggested_tag_ids,
        "suggested_tags": _tag_labels(suggested_tag_ids, tags),
        "cover_image_url": _pick_cover_image(post),
        "mentalaba_university_id": post.university.mentalaba_id if post.university else None,
    }


async def load_exportable_posts(
    db: AsyncSession,
    *,
    syndication_status: Optional[str] = None,
    page: int = 1,
    limit: int = 20,
) -> Tuple[List[NewsPost], int]:
    q = select(NewsPost).options(
        selectinload(NewsPost.university),
        selectinload(NewsPost.cover_image),
        selectinload(NewsPost.media_assets),
    )
    if syndication_status and syndication_status != "ALL":
        q = q.where(NewsPost.syndication_status == syndication_status.upper())
    base_query = q
    total = (await db.execute(select(func.count()).select_from(base_query.subquery()))).scalar() or 0
    q = base_query.order_by(NewsPost.published_at.desc().nullsfirst(), NewsPost.created_at.desc()).offset((page - 1) * limit).limit(limit)
    posts = (await db.execute(q)).scalars().all()
    return posts, total


def _build_payload(post: NewsPost, header_image: str, tag_ids: List[int]) -> Dict[str, Any]:
    title = (post.title or "").strip()
    html = post.content_html or _html_from_text(post.content_text or post.summary or title)
    plain = _strip_html(post.content_text or post.summary or html or title)
    meta = _build_meta(title, plain)
    return {
        "header_image": header_image,
        "title_uz": title,
        "title_ru": title,
        "title_en": title,
        "description_uz": html,
        "description_ru": html,
        "description_en": html,
        "mtdt_title_uz": meta["title"],
        "mtdt_title_ru": meta["title"],
        "mtdt_title_en": meta["title"],
        "mtdt_description_uz": meta["description"],
        "mtdt_description_ru": meta["description"],
        "mtdt_description_en": meta["description"],
        "mtdt_keywords_uz": meta["keywords"],
        "mtdt_keywords_ru": meta["keywords"],
        "mtdt_keywords_en": meta["keywords"],
        "relation_id": post.university.mentalaba_id if post.university else None,
        "status": "active",
        "tag_ids": tag_ids,
        "views_count": 0,
    }


async def export_post_to_mentalaba(db: AsyncSession, post_id: str) -> NewsPost:
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
        raise ValueError("Post not found")

    tags = await get_cached_tags(db)
    prepared = prepare_syndication(post, tags)
    if not prepared["is_exportable"]:
        post.syndication_status = "FAILED"
        post.syndication_last_error = prepared["export_reason"]
        await db.commit()
        return post

    try:
        header_image = await _upload_remote_image(prepared["cover_image_url"])
        payload = _build_payload(post, header_image, prepared["suggested_tag_ids"])
        async with httpx.AsyncClient(timeout=60.0, headers=_auth_headers()) as client:
            response = await client.post(f"{MENTALABA_BASE_URL}/news", json=payload)
            if response.status_code >= 400:
                raise RuntimeError(f"News create failed ({response.status_code}): {response.text}")
            remote = response.json()
        post.syndication_status = "EXPORTED"
        post.syndication_remote_id = str(remote.get("id") or "")
        post.syndication_last_error = None
        post.syndication_pushed_at = datetime.utcnow()
        await db.commit()
        await db.refresh(post)
        return post
    except Exception as exc:
        post.syndication_status = "FAILED"
        post.syndication_last_error = str(exc)
        await db.commit()
        await db.refresh(post)
        return post


async def deactivate_exported_post(db: AsyncSession, post: NewsPost) -> NewsPost:
    if not post.syndication_remote_id:
        post.syndication_status = "REJECTED"
        post.syndication_last_error = None
        await db.commit()
        await db.refresh(post)
        return post

    try:
        await update_remote_news_status(post.syndication_remote_id, "non-active")
        post.syndication_status = "REJECTED"
        post.syndication_last_error = None
        await db.commit()
        await db.refresh(post)
        return post
    except Exception as exc:
        post.syndication_status = "FAILED"
        post.syndication_last_error = str(exc)
        await db.commit()
        await db.refresh(post)
        return post


async def refresh_post_syndication_state(db: AsyncSession, post: NewsPost) -> None:
    exportable, _ = is_exportable(post)
    if post.moderation_status in {"REJECTED", "TRASH"}:
        post.syndication_status = "REJECTED"
    elif post.syndication_status == "EXPORTED":
        return
    elif exportable:
        post.syndication_status = "PENDING"
    else:
        post.syndication_status = "DRAFT"


async def get_overview(db: AsyncSession) -> Dict[str, Any]:
    tags = await get_cached_tags(db)
    remote_universities = await get_cached_remote_universities(db)
    local_universities = (await db.execute(select(University))).scalars().all()
    posts = (await db.execute(select(NewsPost))).scalars().all()
    by_status: Dict[str, int] = {}
    for post in posts:
        key = post.syndication_status or "DRAFT"
        by_status[key] = by_status.get(key, 0) + 1
    mapped_universities = len([item for item in local_universities if item.mentalaba_id])
    return {
        "export_mode": await get_export_mode(db),
        "token_configured": has_mentalaba_token(),
        "tags_count": len(tags),
        "remote_universities_count": len(remote_universities),
        "mapped_universities_count": mapped_universities,
        "unmapped_universities_count": max(0, len(local_universities) - mapped_universities),
        "news_by_status": by_status,
        "last_tags_sync_at": await _get_setting(db, LAST_TAGS_SYNC_KEY),
        "last_universities_sync_at": await _get_setting(db, LAST_UNIVERSITIES_SYNC_KEY),
    }
