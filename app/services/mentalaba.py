import json
import mimetypes
import os
import re
from datetime import datetime
from html import escape
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
import httpx
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import MediaAsset, NewsPost, SystemSetting, University


MENTALABA_BASE_URL = os.getenv("MENTALABA_API_BASE", "https://api.mentalaba.uz/v1").rstrip("/")
MENTALABA_TOKEN = os.getenv("MENTALABA_API_TOKEN", "")
MENTALABA_UPLOAD_ASSOCIATED_WITH = os.getenv("MENTALABA_UPLOAD_ASSOCIATED_WITH", "test")
MENTALABA_UPLOAD_USAGE = os.getenv("MENTALABA_UPLOAD_USAGE", "test_gallery")
MENTALABA_DEFAULT_NEWS_STATUS = os.getenv("MENTALABA_DEFAULT_NEWS_STATUS", "non-active")

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


def _tag_stem(value: Optional[str]) -> str:
    normalized = _normalize_text(value)
    normalized = re.sub(r"\b20\d{2}\b", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


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


def _safe_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, indent=2, default=str)
    except Exception:
        return str(value)


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


async def _upload_remote_image(image_url: str) -> Tuple[str, Dict[str, Any]]:
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
    request_meta = {
        "method": "POST",
        "url": f"{MENTALABA_BASE_URL}/images/upload",
        "associated_with": MENTALABA_UPLOAD_ASSOCIATED_WITH,
        "usage": MENTALABA_UPLOAD_USAGE,
        "filename": filename,
        "mime_type": mime_type or "image/jpeg",
        "source_image_url": image_url,
        "size_bytes": len(file_bytes),
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
    return str(path), {
        "request": request_meta,
        "response": payload,
        "status_code": response.status_code,
    }


async def update_remote_news_status(remote_id: str, status_value: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    payload = {"status": status_value}
    last_error: Optional[Exception] = None
    async with httpx.AsyncClient(timeout=45.0, headers=_auth_headers()) as client:
        for method in (client.patch, client.put):
            try:
                response = await method(f"{MENTALABA_BASE_URL}/news/{remote_id}", json=payload)
                if response.status_code >= 400:
                    raise RuntimeError(f"{response.status_code}: {response.text}")
                body = response.json()
                return body, {
                    "request": {
                        "method": method.__name__.upper(),
                        "url": f"{MENTALABA_BASE_URL}/news/{remote_id}",
                        "json": payload,
                    },
                    "response": body,
                    "status_code": response.status_code,
                }
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


def _normalize_language(value: Optional[str]) -> str:
    raw = _normalize_text(value)
    if raw in {"uz", "uzbek", "oz", "o'zbek", "ozbek"}:
        return "uz"
    if raw in {"ru", "rus", "russian", "рус", "ru-ru"}:
        return "ru"
    if raw in {"en", "eng", "english", "en-us", "en-gb"}:
        return "en"
    return "unknown"


def _post_content_bundle(post: NewsPost) -> Dict[str, str]:
    title = (post.title or "").strip()
    html = post.content_html or _html_from_text(post.content_text or post.summary or title)
    plain = _strip_html(post.content_text or post.summary or html or title)
    meta = _build_meta(title, plain)
    return {
        "title": title,
        "html": html,
        "meta_title": meta["title"],
        "meta_description": meta["description"],
        "meta_keywords": meta["keywords"],
    }


async def _load_translation_candidates(db: AsyncSession, post: NewsPost) -> List[NewsPost]:
    identifiers: Set[str] = {
        value.strip()
        for value in [post.source_url, post.canonical_url]
        if value and value.strip()
    }
    if not identifiers:
        return [post]

    result = await db.execute(
        select(NewsPost)
        .where(NewsPost.university_id == post.university_id)
        .where(
            or_(
                NewsPost.source_url.in_(identifiers),
                NewsPost.canonical_url.in_(identifiers),
            )
        )
    )
    posts = result.scalars().all()
    if not posts:
        return [post]

    by_id = {item.id: item for item in posts}
    by_id[post.id] = post
    return list(by_id.values())


def match_tag_ids(post: NewsPost, tags: List[Dict[str, Any]]) -> List[int]:
    corpus = _normalize_text(" ".join(filter(None, [post.title, post.summary, post.content_text])))
    years_in_content = set(re.findall(r"\b20\d{2}\b", " ".join(filter(None, [post.title or "", post.summary or "", post.content_text or ""]))))
    matched: List[int] = []
    semantic_groups = {
        "admission": ["qabul", "admission", "abituriyent", "abiturent", "abiturient", "прием", "абитуриент"],
        "grant": ["grant", "stipend", "scholarship", "stipendiya", "грант", "стипенд"],
        "transfer": ["kochirish", "ko'chirish", "transfer", "перевод"],
        "mandate": ["mandat", "mandate"],
        "certificate": ["sertifikat", "certificate"],
        "score_quota": ["kvota", "ball", "quota", "score", "kirish ballari"],
        "student": ["talaba", "student", "студент"],
        "internship": ["amaliyot", "internship", "практика"],
        "olympiad": ["olimpiada", "olympics", "олимпиада"],
        "cooperation": ["hamkorlik", "cooperation", "сотрудничество"],
        "technology": ["technology", "texnolog", "технолог"],
        "education": ["talim", "ta'lim", "education", "образование"],
        "university": ["universitet", "university", "университет"],
    }

    active_tags = [tag for tag in tags if tag.get("status") == "active"]

    for tag in active_tags:
        values = [
            _normalize_text(tag.get("tag_uz")),
            _normalize_text(tag.get("tag_ru")),
            _normalize_text(tag.get("tag_en")),
        ]
        year_tokens = set()
        for value in values:
            year_tokens.update(re.findall(r"\b20\d{2}\b", value))

        # Don't bind year-specific tags unless the same year is present in the content.
        if year_tokens and not (year_tokens & years_in_content):
            continue

        for value in values:
            if value and value in corpus:
                matched.append(int(tag["id"]))
                break

    for group_keywords in semantic_groups.values():
        if not any(keyword in corpus for keyword in group_keywords):
            continue
        for tag in active_tags:
            values = [
                _normalize_text(tag.get("tag_uz")),
                _normalize_text(tag.get("tag_ru")),
                _normalize_text(tag.get("tag_en")),
                _tag_stem(tag.get("tag_uz")),
                _tag_stem(tag.get("tag_ru")),
                _tag_stem(tag.get("tag_en")),
            ]
            if any(value and any(keyword in value for keyword in group_keywords) for value in values):
                year_tokens = set()
                for value in values[:3]:
                    year_tokens.update(re.findall(r"\b20\d{2}\b", value))
                if year_tokens and not (year_tokens & years_in_content):
                    continue
                matched.append(int(tag["id"]))

    fallbacks: List[int] = []
    for tag in active_tags:
        value_uz = _tag_stem(tag.get("tag_uz"))
        value_en = _tag_stem(tag.get("tag_en"))
        if value_uz == "ta'lim" or value_en == "education":
            fallbacks.append(int(tag["id"]))
        if value_uz.strip() == "universitet" or value_en == "university":
            fallbacks.append(int(tag["id"]))

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
    syndication_statuses: Optional[List[str]] = None,
    university_id: Optional[str] = None,
    search: Optional[str] = None,
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
    if syndication_statuses:
        normalized_statuses = [status.upper() for status in syndication_statuses if status and status.upper() != "ALL"]
        if normalized_statuses:
            q = q.where(NewsPost.syndication_status.in_(normalized_statuses))
    if university_id:
        q = q.where(NewsPost.university_id == university_id)
    if search:
        pattern = f"%{search.strip()}%"
        q = q.join(University, University.id == NewsPost.university_id).where(
            or_(
                NewsPost.title.ilike(pattern),
                func.coalesce(NewsPost.summary, "").ilike(pattern),
                func.coalesce(NewsPost.content_text, "").ilike(pattern),
                func.coalesce(University.name_uz, "").ilike(pattern),
                func.coalesce(University.name_ru, "").ilike(pattern),
                func.coalesce(University.name_en, "").ilike(pattern),
            )
        )
    base_query = q
    total = (await db.execute(select(func.count()).select_from(base_query.subquery()))).scalar() or 0
    q = base_query.order_by(NewsPost.published_at.desc().nullsfirst(), NewsPost.created_at.desc()).offset((page - 1) * limit).limit(limit)
    posts = (await db.execute(q)).scalars().all()
    return posts, total


def _build_payload(post: NewsPost, header_image: str, tag_ids: List[int], localized_posts: List[NewsPost]) -> Dict[str, Any]:
    localized: Dict[str, Dict[str, str]] = {}
    for candidate in localized_posts:
        lang = _normalize_language(candidate.language)
        if lang in {"uz", "ru", "en"} and lang not in localized:
            localized[lang] = _post_content_bundle(candidate)

    primary = _post_content_bundle(post)
    for lang in ("uz", "ru", "en"):
        localized.setdefault(lang, primary)

    return {
        "header_image": header_image,
        "title_uz": localized["uz"]["title"],
        "title_ru": localized["ru"]["title"],
        "title_en": localized["en"]["title"],
        "description_uz": localized["uz"]["html"],
        "description_ru": localized["ru"]["html"],
        "description_en": localized["en"]["html"],
        "mtdt_title_uz": localized["uz"]["meta_title"],
        "mtdt_title_ru": localized["ru"]["meta_title"],
        "mtdt_title_en": localized["en"]["meta_title"],
        "mtdt_description_uz": localized["uz"]["meta_description"],
        "mtdt_description_ru": localized["ru"]["meta_description"],
        "mtdt_description_en": localized["en"]["meta_description"],
        "mtdt_keywords_uz": localized["uz"]["meta_keywords"],
        "mtdt_keywords_ru": localized["ru"]["meta_keywords"],
        "mtdt_keywords_en": localized["en"]["meta_keywords"],
        "relation_id": post.university.mentalaba_id if post.university else None,
        "status": MENTALABA_DEFAULT_NEWS_STATUS,
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
        post.syndication_last_action = "eligibility_check_failed"
        await db.commit()
        return post

    try:
        header_image, image_trace = await _upload_remote_image(prepared["cover_image_url"])
        localized_posts = await _load_translation_candidates(db, post)
        payload = _build_payload(post, header_image, prepared["suggested_tag_ids"], localized_posts)
        post.syndication_image_payload = _safe_json(image_trace["request"])
        post.syndication_image_response = _safe_json(image_trace["response"])
        async with httpx.AsyncClient(timeout=60.0, headers=_auth_headers()) as client:
            response = await client.post(f"{MENTALABA_BASE_URL}/news", json=payload)
            if response.status_code >= 400:
                raise RuntimeError(f"News create failed ({response.status_code}): {response.text}")
            remote = response.json()
        post.syndication_status = "EXPORTED"
        post.syndication_remote_id = str(remote.get("id") or "")
        post.syndication_last_error = None
        post.syndication_pushed_at = datetime.utcnow()
        post.syndication_request_payload = _safe_json({
            "method": "POST",
            "url": f"{MENTALABA_BASE_URL}/news",
            "json": payload,
        })
        post.syndication_response_payload = _safe_json(remote)
        post.syndication_last_action = "create_remote_news"
        post.syndication_last_status_code = response.status_code
        await db.commit()
        await db.refresh(post)
        return post
    except Exception as exc:
        post.syndication_status = "FAILED"
        post.syndication_last_error = str(exc)
        post.syndication_last_action = "create_remote_news_failed"
        await db.commit()
        await db.refresh(post)
        return post


async def deactivate_exported_post(db: AsyncSession, post: NewsPost) -> NewsPost:
    if not post.syndication_remote_id:
        post.syndication_status = "REJECTED"
        post.syndication_last_error = None
        post.syndication_last_action = "deactivate_skip_remote"
        await db.commit()
        await db.refresh(post)
        return post

    try:
        _, trace = await update_remote_news_status(post.syndication_remote_id, "non-active")
        post.syndication_status = "REJECTED"
        post.syndication_last_error = None
        post.syndication_request_payload = _safe_json(trace["request"])
        post.syndication_response_payload = _safe_json(trace["response"])
        post.syndication_last_action = "deactivate_remote"
        post.syndication_last_status_code = trace["status_code"]
        await db.commit()
        await db.refresh(post)
        return post
    except Exception as exc:
        post.syndication_status = "FAILED"
        post.syndication_last_error = str(exc)
        post.syndication_last_action = "deactivate_remote_failed"
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
