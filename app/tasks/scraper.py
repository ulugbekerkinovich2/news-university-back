"""
Advanced University News Scraper
=================================
Features:
  - RSS / Atom / sitemap.xml discovery (fastest, most structured)
  - Heuristic link discovery with 25+ URL patterns
  - Pagination following (up to 5 pages)
  - trafilatura-based content extraction (newspaper-grade)
  - langdetect auto language detection
  - OpenGraph / meta date extraction
  - og:image + article <img> extraction → MediaAsset
  - SHA-256 content fingerprint deduplication
  - Per-URL retry with exponential backoff
  - Per-domain random delay (politeness)
  - Granular job events per stage
"""

import asyncio
import hashlib
import random
import re
import uuid
from datetime import datetime
from typing import Optional, Set, List
from urllib.parse import urljoin, urlparse

import feedparser
import httpx
import trafilatura
from bs4 import BeautifulSoup
from langdetect import detect, LangDetectException
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select, update

from app.tasks.celery_app import celery_app

# ── Config ─────────────────────────────────────────────────────────────────
DATABASE_URL = "sqlite+aiosqlite:///./data/app.db"
MAX_LINKS_PER_SITE = 40       # max article URLs to try per university
MAX_PAGES = 5                   # max pagination depth
REQUEST_TIMEOUT = 20            # seconds
RETRY_COUNT = 3
RETRY_BACKOFF = 2.0            # seconds (doubled each retry)

# Patterns that signal a news/article URL on Uzbek university sites
# Covers: /news/, /yangilik/, /xabar/, /maqola/, /press/, /blog/
# Also: /<lang>/<section> style  like /oz/news/, /en/news/
# And: date-based paths like /2023/01/, /2024/07/12/
NEWS_URL_PATTERNS = re.compile(
    r"/(news|yangilik|xabar|article|post|blog|press|media|maqola|"
    r"announcement|event|publication|material|barchasi|all|"
    r"latest|recent|novosti|stat|pressa|matbuot|axborot|"
    r"mediamarkaz|media-markaz|nashrlar|tahrir)/"
    r"|/(oz|uz|en|ru)/(news|yangilik|xabar|media|matbuot|article|blog)/"
    r"|/\d{4}/\d{2}/",  # date-based paths e.g. /2024/03/
    re.IGNORECASE,
)

# More permissive: any internal link with slug-like structure longer than /x/y
ARTICLE_PATH_MIN_DEPTH = 2   # at least /section/slug

PAGINATION_PATTERNS = re.compile(
    r"[?&](page|p|offset|start)=\d+|/page/\d+",
    re.IGNORECASE,
)

RSS_PATHS = [
    "/feed", "/rss", "/rss.xml", "/atom.xml", "/feed.xml",
    "/news/rss", "/news/feed", "/yangiliklar/rss",
    "/feed/rss", "/feed/atom",
]

SITEMAP_PATHS = [
    "/sitemap.xml", "/sitemap_news.xml", "/sitemap-news.xml",
    "/news-sitemap.xml", "/sitemap-index.xml",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; UniversityNewsBot/2.0; "
        "+https://university-hub.uz/bot)"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "uz,en;q=0.9,ru;q=0.8",
}


# ── Engine helpers ──────────────────────────────────────────────────────────

def _make_engine():
    return create_async_engine(DATABASE_URL, echo=False, future=True)


def _make_session(engine):
    return sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


# ── HTTP helpers ────────────────────────────────────────────────────────────

async def _fetch(client: httpx.AsyncClient, url: str, retries: int = RETRY_COUNT) -> Optional[httpx.Response]:
    """Fetch URL with exponential backoff retries."""
    delay = RETRY_BACKOFF
    for attempt in range(retries):
        try:
            resp = await client.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, follow_redirects=True)
            if resp.status_code < 400:
                return resp
        except (httpx.RequestError, httpx.TimeoutException):
            pass
        if attempt < retries - 1:
            await asyncio.sleep(delay + random.uniform(0, 1))
            delay *= 2
    return None


async def _polite_delay():
    """Random delay between 0.5s and 2s to be polite to servers."""
    await asyncio.sleep(random.uniform(0.5, 2.0))


# ── Link discovery ──────────────────────────────────────────────────────────

async def _discover_via_rss(client: httpx.AsyncClient, base_url: str) -> Set[str]:
    """Try RSS/Atom feeds — highest quality, structured links."""
    links = set()
    for path in RSS_PATHS:
        url = base_url.rstrip("/") + path
        resp = await _fetch(client, url, retries=1)
        if resp and resp.status_code == 200:
            feed = feedparser.parse(resp.text)
            if feed.entries:
                for entry in feed.entries[:MAX_LINKS_PER_SITE]:
                    link = entry.get("link", "")
                    if link:
                        links.add(link)
                if links:
                    break  # found good feed, stop trying
    return links


async def _discover_via_sitemap(client: httpx.AsyncClient, base_url: str) -> Set[str]:
    """Parse sitemap.xml for news URLs."""
    links = set()
    for path in SITEMAP_PATHS:
        url = base_url.rstrip("/") + path
        resp = await _fetch(client, url, retries=1)
        if resp and resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "xml")
            for loc in soup.find_all("loc"):
                href = loc.get_text(strip=True)
                if NEWS_URL_PATTERNS.search(href):
                    links.add(href)
            if links:
                break
    return links


def _extract_links_from_page(html: str, base_url: str) -> Set[str]:
    """Heuristic link extraction from HTML page.
    First tries news-pattern matching, then falls back to depth-based heuristic."""
    soup = BeautifulSoup(html, "html.parser")
    base = urlparse(base_url)
    links: Set[str] = set()
    fallback: Set[str] = set()

    # Skip these common non-article paths
    SKIP_PATHS = re.compile(
        r"/(about|contact|login|register|search|tag|category|author|"
        r"privacy|terms|cookies|sitemap|feed|rss|xml|pdf|wp-admin|"
        r"static|assets|media/images|style|css|js|font|cdn)/",
        re.IGNORECASE,
    )

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue

        full = urljoin(base_url, href)
        parsed = urlparse(full)

        # Same domain only
        if parsed.netloc and parsed.netloc != base.netloc:
            continue

        clean = full.split("?")[0].split("#")[0]
        path_parts = [p for p in parsed.path.split("/") if p]

        # Must have news-like path — primary match
        if NEWS_URL_PATTERNS.search(parsed.path) and len(path_parts) >= 2:
            links.add(clean)

        # Fallback: any deep internal link (depth >= 3) that isn't admin/static
        elif len(path_parts) >= 3 and not SKIP_PATHS.search(parsed.path):
            # Last segment looks like a slug (has letters, not just a number)
            last = path_parts[-1]
            if re.search(r"[a-zA-Z]", last) or len(last) > 5:
                fallback.add(clean)

    # Use fallback pool if primary found nothing
    if not links and fallback:
        links = set(list(fallback)[:MAX_LINKS_PER_SITE])

    return links


async def _discover_paginated(
    client: httpx.AsyncClient, base_url: str, first_html: str
) -> Set[str]:
    """Follow pagination and collect links from listing pages."""
    soup = BeautifulSoup(first_html, "html.parser")
    base = urlparse(base_url)
    visited = {base_url}
    queue = []

    # Find pagination links
    for a in soup.find_all("a", href=True):
        href = a["href"]
        full = urljoin(base_url, href)
        if PAGINATION_PATTERNS.search(full) and full not in visited:
            queue.append(full)

    all_links = _extract_links_from_page(first_html, base_url)

    for page_url in queue[:MAX_PAGES - 1]:
        await _polite_delay()
        resp = await _fetch(client, page_url, retries=1)
        if resp:
            all_links |= _extract_links_from_page(resp.text, base_url)
            visited.add(page_url)

    return all_links


# ── Article extraction ──────────────────────────────────────────────────────

def _extract_date(soup: BeautifulSoup) -> Optional[datetime]:
    """Try to extract publication date from various meta tags."""
    selectors = [
        ("meta", {"property": "article:published_time"}),
        ("meta", {"name": "pubdate"}),
        ("meta", {"name": "DC.date"}),
        ("meta", {"property": "og:updated_time"}),
        ("meta", {"name": "date"}),
    ]
    for tag, attrs in selectors:
        el = soup.find(tag, attrs)
        if el:
            val = el.get("content", "")
            if val:
                try:
                    return datetime.fromisoformat(val[:19].replace("Z", ""))
                except (ValueError, AttributeError):
                    pass

    # Try <time> element
    time_el = soup.find("time", datetime=True)
    if time_el:
        try:
            return datetime.fromisoformat(time_el["datetime"][:19].replace("Z", ""))
        except (ValueError, TypeError):
            pass
    return None


def _extract_og_image(soup: BeautifulSoup) -> Optional[str]:
    """Extract og:image or first article image."""
    og = soup.find("meta", property="og:image")
    if og:
        return og.get("content")
    # first article/main img
    for container in ["article", "main", "section"]:
        el = soup.find(container)
        if el:
            img = el.find("img", src=True)
            if img:
                return img["src"]
    return None


def _detect_language(text: str) -> str:
    try:
        lang = detect(text[:1000])
        return lang if lang else "unknown"
    except LangDetectException:
        return "unknown"


def _content_fingerprint(url: str, title: str) -> str:
    return hashlib.sha256(f"{url}||{title}".encode()).hexdigest()


def _extract_article(html: str, url: str) -> dict:
    """Use trafilatura for high-quality extraction, fallback to BS4."""
    # Try trafilatura first
    extracted = trafilatura.extract(
        html,
        url=url,
        include_comments=False,
        include_tables=True,
        no_fallback=False,
        favor_recall=True,
    )
    meta = trafilatura.extract_metadata(html, default_url=url)

    soup = BeautifulSoup(html, "html.parser")

    title = ""
    if meta and meta.title:
        title = meta.title
    else:
        h1 = soup.find("h1")
        title = h1.get_text(strip=True) if h1 else (soup.title.string if soup.title else url)

    content_text = extracted or ""
    if not content_text:
        # BS4 fallback
        for tag in ["article", "main", '[class*="content"]', '[class*="news"]']:
            el = soup.select_one(tag)
            if el:
                content_text = el.get_text("\n", strip=True)[:10000]
                break

    published_at = None
    if meta and meta.date:
        try:
            published_at = datetime.fromisoformat(str(meta.date)[:19])
        except (ValueError, TypeError):
            pass
    if not published_at:
        published_at = _extract_date(soup)

    # Summary: first 300 chars of content
    summary = content_text[:300].strip() if content_text else None

    og_image = _extract_og_image(soup)
    language = _detect_language(content_text or title)
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower())[:120].strip("-")

    return {
        "title": title[:500],
        "slug": slug or "article",
        "content_text": content_text[:20000],
        "summary": summary,
        "published_at": published_at,
        "language": language,
        "og_image": og_image,
    }


# ── Main async scraper ──────────────────────────────────────────────────────

async def _add_event(db, job_id: str, uni_id: str, stage, message: str, counters: dict = None):
    from app.models import ScrapeJobEvent
    ev = ScrapeJobEvent(
        job_id=job_id,
        university_id=uni_id,
        stage=stage,
        message=message,
        counters_json=counters or {},
    )
    db.add(ev)
    await db.commit()


async def _scrape_university_async(university_id: str, job_id: str):
    from app.models import (
        University, NewsPost, MediaAsset, ScrapeJob, ScrapeJobEvent,
        ScrapeStage, JobStatus, ScrapeStatus, MediaType,
    )

    engine = _make_engine()
    Session = _make_session(engine)

    async with Session() as db:
        # ── INIT ────────────────────────────────────────────────────────
        await db.execute(
            update(ScrapeJob).where(ScrapeJob.id == job_id)
            .values(status=JobStatus.RUNNING, started_at=datetime.utcnow())
        )
        await db.execute(
            update(University).where(University.id == university_id)
            .values(scrape_status=ScrapeStatus.IN_PROGRESS)
        )
        await db.commit()

        result = await db.execute(select(University).where(University.id == university_id))
        uni = result.scalar_one_or_none()

        if not uni or not uni.website:
            await db.execute(
                update(University).where(University.id == university_id)
                .values(scrape_status=ScrapeStatus.NO_SOURCE)
            )
            await db.execute(
                update(ScrapeJob).where(ScrapeJob.id == job_id)
                .values(status=JobStatus.DONE, finished_at=datetime.utcnow())
            )
            await db.commit()
            await engine.dispose()
            return

        base_url = uni.website.rstrip("/")
        parsed_base = urlparse(base_url)
        if not parsed_base.scheme:
            base_url = "https://" + base_url

        saved_count = 0
        skipped_count = 0
        error_count = 0

        try:
            async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT, follow_redirects=True) as client:

                # ── DISCOVER ─────────────────────────────────────────────
                await _add_event(
                    db, job_id, university_id, ScrapeStage.DISCOVER,
                    f"Starting discovery on {base_url}"
                )

                article_links: Set[str] = set()

                # 1. RSS feeds (best quality)
                rss_links = await _discover_via_rss(client, base_url)
                if rss_links:
                    article_links |= rss_links
                    await _add_event(
                        db, job_id, university_id, ScrapeStage.DISCOVER,
                        f"RSS: found {len(rss_links)} articles"
                    )

                # 2. Sitemap.xml
                if len(article_links) < MAX_LINKS_PER_SITE:
                    sitemap_links = await _discover_via_sitemap(client, base_url)
                    article_links |= sitemap_links
                    if sitemap_links:
                        await _add_event(
                            db, job_id, university_id, ScrapeStage.DISCOVER,
                            f"Sitemap: found {len(sitemap_links)} articles"
                        )

                # 3. Heuristic homepage crawl + pagination
                if len(article_links) < MAX_LINKS_PER_SITE:
                    homepage_resp = await _fetch(client, base_url)
                    if homepage_resp:
                        html = homepage_resp.text
                        page_links = await _discover_paginated(client, base_url, html)
                        article_links |= page_links
                        await _add_event(
                            db, job_id, university_id, ScrapeStage.DISCOVER,
                            f"Heuristic crawl: found {len(page_links)} article links"
                        )

                # Limit total
                article_links_list = list(article_links)[:MAX_LINKS_PER_SITE]

                await _add_event(
                    db, job_id, university_id, ScrapeStage.CRAWL,
                    f"Total unique candidate URLs: {len(article_links_list)}",
                    {"total_candidates": len(article_links_list)}
                )

                # ── PARSE ────────────────────────────────────────────────
                for idx, url in enumerate(article_links_list):
                    await _polite_delay()

                    resp = await _fetch(client, url)
                    if not resp:
                        error_count += 1
                        continue

                    try:
                        article = _extract_article(resp.text, url)
                        title = article["title"]
                        if not title or len(title) < 5:
                            skipped_count += 1
                            continue

                        fingerprint = _content_fingerprint(url, title)

                        # Dedup by URL or fingerprint
                        dup_url = await db.execute(
                            select(NewsPost).where(NewsPost.source_url == url)
                        )
                        dup_fp = await db.execute(
                            select(NewsPost).where(NewsPost.hash_fingerprint == fingerprint)
                        )
                        if dup_url.scalar_one_or_none() or dup_fp.scalar_one_or_none():
                            skipped_count += 1
                            continue

                        # Save cover image as MediaAsset
                        cover_image_id = None
                        if article["og_image"]:
                            img_url = urljoin(url, article["og_image"])
                            existing_img = await db.execute(
                                select(MediaAsset).where(MediaAsset.original_url == img_url)
                            )
                            if not existing_img.scalar_one_or_none():
                                media = MediaAsset(
                                    id=str(uuid.uuid4()),
                                    type=MediaType.image,
                                    original_url=img_url,
                                )
                                db.add(media)
                                await db.flush()
                                cover_image_id = media.id

                        post = NewsPost(
                            id=str(uuid.uuid4()),
                            university_id=university_id,
                            title=article["title"],
                            slug=article["slug"],
                            content_text=article["content_text"],
                            summary=article["summary"],
                            source_url=url,
                            canonical_url=url,
                            published_at=article["published_at"],
                            language=article["language"],
                            hash_fingerprint=fingerprint,
                            cover_image_id=cover_image_id,
                        )
                        db.add(post)
                        saved_count += 1

                        # Commit every 5 posts
                        if saved_count % 5 == 0:
                            await db.commit()
                            await _add_event(
                                db, job_id, university_id, ScrapeStage.PARSE,
                                f"Progress: {saved_count} saved, {skipped_count} skipped",
                                {"saved": saved_count, "skipped": skipped_count}
                            )

                    except Exception as e:
                        error_count += 1

                await db.commit()

                # ── DONE ─────────────────────────────────────────────────
                await _add_event(
                    db, job_id, university_id, ScrapeStage.SAVE_POSTS,
                    f"Complete: {saved_count} saved, {skipped_count} skipped, {error_count} errors",
                    {"saved": saved_count, "skipped": skipped_count, "errors": error_count}
                )

                final_status = ScrapeStatus.DONE if saved_count > 0 else ScrapeStatus.NO_NEWS
                await db.execute(
                    update(University).where(University.id == university_id)
                    .values(
                        scrape_status=final_status,
                        last_scraped_at=datetime.utcnow(),
                        last_error_message=None,
                    )
                )
                await db.execute(
                    update(ScrapeJob).where(ScrapeJob.id == job_id)
                    .values(
                        status=JobStatus.DONE,
                        finished_at=datetime.utcnow(),
                        totals_json={
                            "saved": saved_count,
                            "skipped": skipped_count,
                            "errors": error_count,
                        },
                    )
                )
                await db.commit()

        except Exception as exc:
            error_msg = str(exc)[:800]
            await db.execute(
                update(University).where(University.id == university_id)
                .values(scrape_status=ScrapeStatus.FAILED, last_error_message=error_msg)
            )
            await db.execute(
                update(ScrapeJob).where(ScrapeJob.id == job_id)
                .values(status=JobStatus.FAILED, finished_at=datetime.utcnow())
            )
            await db.commit()

        await engine.dispose()


# ── Celery tasks ────────────────────────────────────────────────────────────

@celery_app.task(name="tasks.scrape_university", bind=True, max_retries=0)
def scrape_university(self, university_id: str, job_id: str):
    """Celery task: scrape a single university."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_scrape_university_async(university_id, job_id))
    finally:
        loop.close()


def _scrape_university_async_in_thread(university_id: str, job_id: str):
    """
    Thread-safe wrapper for FastAPI BackgroundTasks.
    Creates its own event loop — no Celery worker required.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_scrape_university_async(university_id, job_id))
    finally:
        loop.close()


@celery_app.task(name="tasks.scrape_all_universities")
def scrape_all_universities():
    """Periodic task: queue jobs for all universities that have a website."""
    import uuid as _uuid
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session as SyncSession

    sync_engine = create_engine("sqlite:///./data/app.db")
    from app.models import University, ScrapeJob, JobStatus, JobScope, ScrapeStatus

    with SyncSession(sync_engine) as db:
        unis = db.query(University).filter(
            University.website.isnot(None),
            University.scrape_status.not_in([ScrapeStatus.IN_PROGRESS])
        ).all()

        for uni in unis:
            job = ScrapeJob(
                id=str(_uuid.uuid4()),
                scope=JobScope.SINGLE_UNIVERSITY,
                university_id=uni.id,
                status=JobStatus.QUEUED,
            )
            db.add(job)
            db.commit()
            db.refresh(job)
            _scrape_university_async_in_thread(uni.id, job.id)
