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
import os
import random
import re
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional, Set, List
from urllib.parse import parse_qs, quote_plus, urljoin, urlparse

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
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./data/app.db")
MAX_LINKS_PER_SITE = 40       # max article URLs to try per university
MAX_PAGES = 5                   # max pagination depth
REQUEST_TIMEOUT = 20            # seconds
RETRY_COUNT = 3
RETRY_BACKOFF = 2.0            # seconds (doubled each retry)
DISCOVERY_CONCURRENCY = 3
ARTICLE_FETCH_CONCURRENCY = 6
SCRAPER_WORKERS = 6

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

DIRECT_SECTION_PATHS = [
    "/news",
    "/news/",
    "/news/all",
    "/news/list",
    "/yangiliklar",
    "/yangiliklar/",
    "/yangiliklar/barchasi",
    "/yangilik",
    "/yangilik/",
    "/uz/news",
    "/uz/news/",
    "/uz/yangiliklar",
    "/oz/news",
    "/oz/news/",
    "/oz/yangiliklar",
    "/en/news",
    "/en/news/",
    "/ru/news",
    "/ru/news/",
    "/press",
    "/press/",
    "/press-center",
    "/press-center/",
    "/blog",
    "/blog/",
    "/events",
    "/events/",
    "/announcements",
    "/elonlar",
    "/e-lonlar",
    "/category/news",
]

SEARCH_BLOCKLIST_HOSTS = {
    "google.com", "www.google.com", "maps.google.com", "google.uz", "www.google.uz",
    "bing.com", "www.bing.com", "duckduckgo.com", "www.duckduckgo.com",
    "youtube.com", "www.youtube.com", "instagram.com", "www.instagram.com",
    "facebook.com", "www.facebook.com", "t.me", "telegram.me", "linkedin.com",
    "www.linkedin.com", "wikipedia.org", "www.wikipedia.org",
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
]

def _get_random_user_agent():
    return random.choice(USER_AGENTS)

HEADERS = {
    "User-Agent": _get_random_user_agent(),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "uz,en;q=0.9,ru;q=0.8",
}

SCRAPER_EXECUTOR = ThreadPoolExecutor(max_workers=SCRAPER_WORKERS, thread_name_prefix="scraper")


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
        # Rotate UA for every request
        req_headers = HEADERS.copy()
        req_headers["User-Agent"] = _get_random_user_agent()
        try:
            resp = await client.get(url, headers=req_headers, timeout=REQUEST_TIMEOUT, follow_redirects=True)
            if resp.status_code < 400:
                return resp
        except (httpx.RequestError, httpx.TimeoutException):
            pass
        if attempt < retries - 1:
            await asyncio.sleep(delay + random.uniform(0, 1))
            delay *= 2
    return None


async def _polite_delay():
    """Short jitter to avoid hammering a single server while keeping throughput high."""
    await asyncio.sleep(random.uniform(0.15, 0.45))


async def _send_telegram_notification(message: str):
    """Send a notification to a Telegram chat on critical errors."""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return
    
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        async with httpx.AsyncClient() as client:
            await client.post(url, json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"})
    except Exception as e:
        print(f"Failed to send Telegram notification: {e}")


async def _gather_fetch_texts(
    client: httpx.AsyncClient,
    urls: List[str],
    *,
    retries: int = RETRY_COUNT,
    limit: int = ARTICLE_FETCH_CONCURRENCY,
) -> List[tuple[str, Optional[str]]]:
    semaphore = asyncio.Semaphore(limit)

    async def _worker(target_url: str) -> tuple[str, Optional[str]]:
        async with semaphore:
            resp = await _fetch(client, target_url, retries=retries)
            return target_url, (resp.text if resp else None)

    return await asyncio.gather(*[_worker(url) for url in urls])


def _clean_search_result_url(url: str) -> Optional[str]:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return None
    hostname = (parsed.hostname or "").lower()
    if not hostname or any(hostname == blocked or hostname.endswith(f".{blocked}") for blocked in SEARCH_BLOCKLIST_HOSTS):
        return None
    if re.search(r"(google|bing|duckduckgo|youtube|instagram|facebook|telegram|linkedin|wikipedia)", hostname):
        return None
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")


def _extract_google_result_links(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    results: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/url?"):
            q = parse_qs(urlparse(href).query).get("q", [None])[0]
            if q:
                cleaned = _clean_search_result_url(q)
                if cleaned and cleaned not in results:
                    results.append(cleaned)
    return results


def _extract_duckduckgo_result_links(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    results: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        cleaned = _clean_search_result_url(href)
        if cleaned and cleaned not in results:
            results.append(cleaned)
    return results


def _build_site_search_queries(uni) -> List[str]:
    names = []
    for candidate in [getattr(uni, "name_uz", None), getattr(uni, "name_en", None), getattr(uni, "name_ru", None)]:
        if candidate:
            normalized = " ".join(str(candidate).split())
            if normalized and normalized not in names:
                names.append(normalized)

    suffixes = [
        "official website university uzbekistan",
        "rasmiy sayt universitet",
        "sayt universiteti",
    ]

    queries: List[str] = []
    for name in names[:2]:
        for suffix in suffixes:
            queries.append(f"{name} {suffix}")
    return queries[:6]


async def _find_university_website_via_search(client: httpx.AsyncClient, uni) -> Optional[str]:
    """Search for a better official website when stored URL is broken."""
    queries = _build_site_search_queries(uni)
    for query in queries:
        google_url = f"https://www.google.com/search?q={quote_plus(query)}&hl=en"
        resp = await _fetch(client, google_url, retries=1)
        if resp:
            for candidate in _extract_google_result_links(resp.text):
                return candidate

        ddg_url = f"https://duckduckgo.com/html/?q={quote_plus(query)}"
        resp = await _fetch(client, ddg_url, retries=1)
        if resp:
            for candidate in _extract_duckduckgo_result_links(resp.text):
                return candidate

    return None


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


def _discover_language_folders(html: str, base_url: str) -> Set[str]:
    """Find links to other language versions of the homepage (e.g., /en, /ru, /oz)."""
    soup = BeautifulSoup(html, "html.parser")
    base = urlparse(base_url)
    langs = {"/en", "/ru", "/uz", "/oz", "/en/", "/ru/", "/uz/", "/oz/"}
    found = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        full = urljoin(base_url, href)
        parsed = urlparse(full)
        if parsed.netloc and parsed.netloc != base.netloc:
            continue
        p = parsed.path.lower().rstrip("/")
        if p in [l.rstrip("/") for l in langs]:
            found.add(full)
    return found


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
        
        # Skip purely media or document files
        if clean.lower().endswith((
            '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', 
            '.zip', '.rar', '.tar', '.gz', '.7z',
            '.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp', '.bmp', '.ico',
            '.mp4', '.mp3', '.avi', '.mov', '.wmv', '.flv', '.wav'
        )):
            continue

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


def _extract_candidate_section_urls(html: str, base_url: str) -> Set[str]:
    """Find likely listing pages from navigation/footer links."""
    soup = BeautifulSoup(html, "html.parser")
    base = urlparse(base_url)
    section_urls: Set[str] = set()
    section_keywords = (
        "news", "yangilik", "yangiliklar", "xabar", "matbuot", "press",
        "blog", "event", "e'lon", "elon", "announcement", "media", "post"
    )

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = " ".join(a.get_text(" ", strip=True).lower().split())
        full = urljoin(base_url, href)
        parsed = urlparse(full)
        if parsed.netloc and parsed.netloc != base.netloc:
            continue
        normalized = full.split("#")[0].rstrip("/")
        path = parsed.path.lower()
        if any(keyword in path for keyword in section_keywords) or any(keyword in text for keyword in section_keywords):
            if normalized:
                section_urls.add(normalized)

    return section_urls


async def _discover_via_direct_sections(
    client: httpx.AsyncClient,
    base_url: str,
    candidate_section_urls: Optional[Set[str]] = None,
) -> tuple[Set[str], int]:
    """Probe common listing pages directly, useful when homepage navigation hides article links."""
    probe_urls = {
        base_url.rstrip("/") + path
        for path in DIRECT_SECTION_PATHS
    }
    if candidate_section_urls:
        probe_urls |= set(candidate_section_urls)

    section_hits = 0
    links: Set[str] = set()
    for section_url, html in await _gather_fetch_texts(
        client,
        list(probe_urls),
        retries=1,
        limit=DISCOVERY_CONCURRENCY,
    ):
        if not html:
            continue
        section_hits += 1
        links |= _extract_links_from_page(html, section_url)
        if len(links) < MAX_LINKS_PER_SITE:
            links |= await _discover_paginated(client, section_url, html)

    return links, section_hits


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
        content = og.get("content", "").strip()
        if content and not content.startswith(("data:", "javascript:")):
            return content
    # first article/main img
    for container in ["article", "main", "section"]:
        el = soup.find(container)
        if el:
            img = el.find("img", src=True)
            if img:
                src = img["src"].strip()
                if src and not src.startswith(("data:", "javascript:")):
                    return src
    return None


def _detect_language(text: str) -> str:
    """Heuristic + langdetect for Uzbek, Russian, English."""
    if not text or len(text) < 10:
        return "uz"
        
    text_lower = text.lower()
    # 1. Uzbek Cyrillic specific characters
    if any(c in text_lower for c in "ўқғҳ"):
        return "uz"
    
    # 2. Uzbek Latin combined characters/words
    uz_markers = [" o'z", " oʻz", " davlat", " universitet", " talaba", " viloyat", " tuman", " bo'lim"]
    if any(m in text_lower for m in uz_markers):
        return "uz"
        
    try:
        lang = detect(text[:2000])
        # langdetect often flags Uzbek as Albanian (sq) or Romanian (ro)
        if lang in ["sq", "ro", "tr"]:
            return "uz"
        return lang if lang else "uz"
    except LangDetectException:
        return "uz"


def _content_fingerprint(url: str, title: str) -> str:
    return hashlib.sha256(f"{url}||{title}".encode()).hexdigest()


def _is_invalid_article_payload(title: str, content_text: str, url: str) -> bool:
    title_lower = (title or "").strip().lower()
    content_lower = (content_text or "").strip().lower()

    # Hard reject obvious error pages and placeholders.
    invalid_title_markers = {
        "error 404", "404", "page not found", "not found",
        "access denied", "forbidden", "service unavailable",
    }
    if title_lower in invalid_title_markers:
        return True

    # Navigation-only / boilerplate-heavy pages should not become articles.
    boilerplate_markers = [
        "menyu", "biz haqimizda", "infratuzilma", "hamkorlik", "qabul 2026",
        "admissions", "student life", "trening va joylashtirish",
        "yangiliklar va voqealar", "school of engineering",
    ]
    boilerplate_hits = sum(1 for marker in boilerplate_markers if marker in content_lower)
    if boilerplate_hits >= 5:
        return True

    # Very short or mostly menu-like pages are usually not real news.
    if len(content_lower) < 180:
        return True

    if url.lower().endswith((".html", "/index")) and "news" not in url.lower() and "yangilik" not in url.lower():
        if boilerplate_hits >= 3:
            return True

    return False


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

    # Summary: first real paragraph (skip short/boilerplates)
    summary = None
    if content_text:
        paragraphs = [p.strip() for p in content_text.split('\n') if len(p.strip()) > 80]
        if paragraphs:
            # Use the first long paragraph as summary
            summary = paragraphs[0][:400].strip()
            if len(summary) < 100 and len(paragraphs) > 1:
                summary = paragraphs[1][:400].strip()
        else:
            summary = content_text[:300].strip()

    og_image = _extract_og_image(soup)
    language = _detect_language(content_text or title)
    
    # Enforce slug uniqueness by appending URL hash
    slug_base = re.sub(r"[^a-z0-9]+", "-", title.lower())[:100].strip("-")
    if not slug_base:
        slug_base = "article"
    url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()[:8]
    slug = f"{slug_base}-{url_hash}"

    # Plural images for gallery
    images = []
    for container_tag in ["article", "main", '[class*="content"]', '[class*="news"]']:
        # bs4 select supports css classes
        cont = soup.select_one(container_tag) if "[" in container_tag else soup.find(container_tag)
        if cont:
            for img in cont.find_all("img", src=True):
                src = img["src"].strip()
                if src and not src.startswith(("data:", "javascript:")):
                    full_img = urljoin(url, src)
                    if full_img not in images:
                        images.append(full_img)

    return {
        "title": title[:500],
        "slug": slug or "article",
        "content_text": content_text[:20000],
        "summary": summary,
        "published_at": published_at,
        "language": language,
        "og_image": og_image,
        "images": images[:10],  # limit to 10
    }


async def _discover_root_links(
    client: httpx.AsyncClient,
    root_url: str,
    homepage_html: Optional[str],
) -> tuple[str, Set[str], List[str]]:
    links: Set[str] = set()
    logs: List[str] = []

    rss_links = await _discover_via_rss(client, root_url)
    if rss_links:
        links |= rss_links
        logs.append(f"[{root_url}] RSS: {len(rss_links)} ta maqola")

    if len(links) < MAX_LINKS_PER_SITE:
        sitemap_links = await _discover_via_sitemap(client, root_url)
        if sitemap_links:
            links |= sitemap_links
            logs.append(f"[{root_url}] Sitemap: {len(sitemap_links)} ta maqola")

    if len(links) < MAX_LINKS_PER_SITE:
        html = homepage_html
        if html is None:
            resp = await _fetch(client, root_url, retries=1)
            html = resp.text if resp else None
        if html:
            page_links = await _discover_paginated(client, root_url, html)
            if page_links:
                links |= page_links
            logs.append(f"[{root_url}] Heuristic: {len(page_links)} ta maqola")

            if len(links) < MAX_LINKS_PER_SITE:
                section_candidates = _extract_candidate_section_urls(html, root_url)
                section_links, section_hits = await _discover_via_direct_sections(
                    client,
                    root_url,
                    section_candidates,
                )
                if section_links:
                    links |= section_links
                logs.append(
                    f"[{root_url}] Direct sections: {len(section_links)} ta maqola "
                    f"({section_hits} section tekshirildi)"
                )

    return root_url, links, logs


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
        attempted_site_search = False

        try:
            limits = httpx.Limits(max_connections=20, max_keepalive_connections=10)
            async with httpx.AsyncClient(
                timeout=REQUEST_TIMEOUT,
                follow_redirects=True,
                limits=limits,
            ) as client:

                # ── DISCOVER ─────────────────────────────────────────────
                await _add_event(
                    db, job_id, university_id, ScrapeStage.DISCOVER,
                    f"Starting discovery on {base_url}"
                )

                # ── LOGO EXTRACTION ──────────────────────────────────────
                if not getattr(uni, "logo_url", None):
                    try:
                        homepage_resp = await _fetch(client, base_url)
                        if homepage_resp:
                            soup = BeautifulSoup(homepage_resp.text, "html.parser")
                            logo_found = None
                            
                            # 1. Try OG image (often high quality)
                            og = soup.find("meta", property="og:image")
                            if og and og.get("content"):
                                logo_found = urljoin(base_url, og.get("content"))
                            
                            # 2. Try favicon link
                            if not logo_found:
                                fav = soup.find("link", rel=re.compile(r"icon", re.I))
                                if fav and fav.get("href"):
                                    logo_found = urljoin(base_url, fav.get("href"))
                                    
                            # Fallback: Google Favicon API (reliable)
                            if not logo_found:
                                logo_found = f"https://www.google.com/s2/favicons?domain={parsed_base.hostname}&sz=128"
                                
                            if logo_found:
                                # We need to use a clean DB update since this runs in a thread
                                await db.execute(
                                    update(University).where(University.id == university_id)
                                    .values(logo_url=logo_found)
                                )
                                await db.commit()
                                await _add_event(db, job_id, university_id, ScrapeStage.DISCOVER, f"Logo topildi va saqlandi: {logo_found}")
                    except Exception as e:
                        await _add_event(db, job_id, university_id, ScrapeStage.DISCOVER, f"Logo xatosi: {str(e)}")

                article_links: Set[str] = set()

                # ── LANGUAGE ROOTS DISCOVERY ────────────────────────────
                all_root_urls = {base_url}
                homepage_resp = await _fetch(client, base_url)
                homepage_html = homepage_resp.text if homepage_resp else None

                if not homepage_html and not attempted_site_search:
                    discovered_website = await _find_university_website_via_search(client, uni)
                    attempted_site_search = True
                    if discovered_website and discovered_website.rstrip("/") != base_url.rstrip("/"):
                        await db.execute(
                            update(University).where(University.id == university_id)
                            .values(website=discovered_website, updated_at=datetime.utcnow())
                        )
                        await db.commit()
                        await _add_event(
                            db,
                            job_id,
                            university_id,
                            ScrapeStage.DISCOVER,
                            f"Website auto-updated via search: {discovered_website}"
                        )
                        base_url = discovered_website.rstrip("/")
                        homepage_resp = await _fetch(client, base_url)
                        homepage_html = homepage_resp.text if homepage_resp else None

                if homepage_html:
                    lang_urls = _discover_language_folders(homepage_html, base_url)
                    all_root_urls |= lang_urls

                await _add_event(
                    db, job_id, university_id, ScrapeStage.DISCOVER,
                    f"Aniqlangan til bo'limlari: {', '.join(all_root_urls)}"
                )

                discovery_tasks = [
                    _discover_root_links(
                        client,
                        root_url,
                        homepage_html if root_url == base_url else None,
                    )
                    for root_url in list(all_root_urls)[:DISCOVERY_CONCURRENCY]
                ]
                remaining_root_urls = list(all_root_urls)[DISCOVERY_CONCURRENCY:]

                discovered_batches = await asyncio.gather(*discovery_tasks, return_exceptions=True)

                for root_url in remaining_root_urls:
                    discovered_batches += [await _discover_root_links(client, root_url, None)]

                for batch in discovered_batches:
                    if isinstance(batch, Exception):
                        continue
                    root_url, found_links, logs = batch
                    await _add_event(db, job_id, university_id, ScrapeStage.DISCOVER, f"Branch qidirildi: {root_url}")
                    article_links |= found_links
                    for log in logs:
                        await _add_event(db, job_id, university_id, ScrapeStage.DISCOVER, log)

                await _add_event(
                    db, job_id, university_id, ScrapeStage.DISCOVER,
                    f"Heuristic crawl: jami {len(article_links)} ta maqola topildi"
                )

                if not article_links and homepage_html and not attempted_site_search:
                    discovered_website = await _find_university_website_via_search(client, uni)
                    attempted_site_search = True
                    if discovered_website and discovered_website.rstrip("/") != base_url.rstrip("/"):
                        await db.execute(
                            update(University).where(University.id == university_id)
                            .values(website=discovered_website, updated_at=datetime.utcnow())
                        )
                        await db.commit()
                        await _add_event(
                            db,
                            job_id,
                            university_id,
                            ScrapeStage.DISCOVER,
                            f"No article links found. Website refreshed via search: {discovered_website}"
                        )

                        base_url = discovered_website.rstrip("/")
                        article_links = set()
                        homepage_resp = await _fetch(client, base_url)
                        homepage_html = homepage_resp.text if homepage_resp else None
                        all_root_urls = {base_url}
                        if homepage_html:
                            all_root_urls |= _discover_language_folders(homepage_html, base_url)

                        discovered_batches = await asyncio.gather(*[
                            _discover_root_links(
                                client,
                                root_url,
                                homepage_html if root_url == base_url else None,
                            )
                            for root_url in list(all_root_urls)[:DISCOVERY_CONCURRENCY]
                        ], return_exceptions=True)

                        for root_url in list(all_root_urls)[DISCOVERY_CONCURRENCY:]:
                            discovered_batches += [await _discover_root_links(client, root_url, None)]

                        for batch in discovered_batches:
                            if isinstance(batch, Exception):
                                continue
                            _, found_links, logs = batch
                            article_links |= found_links
                            for log in logs:
                                await _add_event(db, job_id, university_id, ScrapeStage.DISCOVER, f"[auto-search] {log}")

                # Limit total
                article_links_list = list(article_links)[:MAX_LINKS_PER_SITE]

                await _add_event(
                    db, job_id, university_id, ScrapeStage.CRAWL,
                    f"Total unique candidate URLs: {len(article_links_list)}",
                    {"total_candidates": len(article_links_list)}
                )

                # ── PARSE ────────────────────────────────────────────────
                prefetched_articles = await _gather_fetch_texts(
                    client,
                    article_links_list,
                    retries=RETRY_COUNT,
                    limit=ARTICLE_FETCH_CONCURRENCY,
                )

                for idx, (url, html) in enumerate(prefetched_articles, start=1):
                    if not html:
                        error_count += 1
                        continue

                    try:
                        article = _extract_article(html, url)
                        title = article["title"]
                        if not title or len(title) < 5:
                            skipped_count += 1
                            continue

                        if _is_invalid_article_payload(title, article["content_text"], url):
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

                        # Save other images as MediaAsset
                        for img_url in article.get("images", []):
                            # Skip if it's identical to og_image (already saved)
                            if article.get("og_image") and img_url == urljoin(url, article["og_image"]):
                                continue
                                
                            existing_asset = await db.execute(
                                select(MediaAsset).where(
                                    (MediaAsset.original_url == img_url) & 
                                    (MediaAsset.post_id == post.id)
                                )
                            )
                            if not existing_asset.scalar_one_or_none():
                                asset = MediaAsset(
                                    id=str(uuid.uuid4()),
                                    type=MediaType.image,
                                    original_url=img_url,
                                    post_id=post.id
                                )
                                db.add(asset)

                        # Commit every 5 posts
                        if saved_count > 0 and saved_count % 5 == 0:
                            await db.commit()
                            await _add_event(
                                db, job_id, university_id, ScrapeStage.PARSE,
                                f"Progress: {saved_count} saved, {skipped_count} skipped, {idx}/{len(prefetched_articles)} ko'rildi",
                                {"saved": saved_count, "skipped": skipped_count, "processed": idx}
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

                if saved_count > 0 or skipped_count > 0:
                    final_status = ScrapeStatus.DONE
                    last_error_message = None
                elif article_links_list and error_count > 0:
                    final_status = ScrapeStatus.FAILED
                    last_error_message = (
                        f"Candidates found ({len(article_links_list)}), "
                        f"but all fetch/parse attempts failed"
                    )
                else:
                    final_status = ScrapeStatus.NO_NEWS
                    last_error_message = None

                if final_status == ScrapeStatus.FAILED:
                    await _send_telegram_notification(
                        f"⚠️ <b>Scrape Failed</b>\nUniversity: {uni.name_uz}\nURL: {base_url}\nError: {last_error_message}"
                    )

                await db.execute(
                    update(University).where(University.id == university_id)
                    .values(
                        scrape_status=final_status,
                        last_scraped_at=datetime.utcnow(),
                        last_error_message=last_error_message,
                    )
                )
                await db.execute(
                    update(ScrapeJob).where(ScrapeJob.id == job_id)
                    .values(
                        status=JobStatus.DONE,
                        finished_at=datetime.utcnow(),
                        totals_json={
                            "candidates": len(article_links_list),
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


def launch_scrape_job(university_id: str, job_id: str):
    """Submit a scrape job to the shared executor so API requests don't serialize work."""
    return SCRAPER_EXECUTOR.submit(_scrape_university_async_in_thread, university_id, job_id)


@celery_app.task(name="tasks.scrape_all_universities")
def scrape_all_universities():
    """Periodic task: queue jobs for all universities that have a website."""
    import uuid as _uuid
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session as SyncSession

    # Postgres sync engine from async URL
    db_url = os.getenv("DATABASE_URL", "sqlite:///./data/app.db")
    if "+asyncpg" in db_url:
        db_url = db_url.replace("+asyncpg", "") # asyncpg -> sync (psycopg2)
    
    sync_engine = create_engine(db_url)
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
            launch_scrape_job(uni.id, job.id)
