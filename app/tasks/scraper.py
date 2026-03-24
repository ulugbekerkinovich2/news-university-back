import asyncio
import hashlib
import re
from datetime import datetime
from typing import Optional
import httpx
from bs4 import BeautifulSoup
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from app.tasks.celery_app import celery_app

DATABASE_URL = "sqlite+aiosqlite:///./data/app.db"


def _make_engine():
    return create_async_engine(DATABASE_URL, echo=False, future=True)


def _make_session(engine):
    return sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def _scrape_university_async(university_id: str, job_id: str):
    """Async scraping logic."""
    from app.models import University, NewsPost, ScrapeJob, ScrapeJobEvent, ScrapeStage, JobStatus, ScrapeStatus
    from sqlalchemy import select, update

    engine = _make_engine()
    Session = _make_session(engine)

    async with Session() as db:
        # Mark job as RUNNING
        await db.execute(
            update(ScrapeJob)
            .where(ScrapeJob.id == job_id)
            .values(status=JobStatus.RUNNING, started_at=datetime.utcnow())
        )
        # Mark university IN_PROGRESS
        await db.execute(
            update(University)
            .where(University.id == university_id)
            .values(scrape_status=ScrapeStatus.IN_PROGRESS)
        )
        await db.commit()

        # Fetch university
        result = await db.execute(select(University).where(University.id == university_id))
        uni = result.scalar_one_or_none()
        if not uni or not uni.website:
            await db.execute(
                update(University)
                .where(University.id == university_id)
                .values(scrape_status=ScrapeStatus.NO_SOURCE)
            )
            await db.execute(
                update(ScrapeJob)
                .where(ScrapeJob.id == job_id)
                .values(status=JobStatus.DONE, finished_at=datetime.utcnow())
            )
            await db.commit()
            await engine.dispose()
            return

        saved_count = 0
        try:
            async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
                # DISCOVER stage
                resp = await client.get(uni.website)
                soup = BeautifulSoup(resp.text, "html.parser")

                # Log event
                event = ScrapeJobEvent(
                    job_id=job_id,
                    university_id=university_id,
                    stage=ScrapeStage.DISCOVER,
                    message=f"Fetched homepage, status={resp.status_code}",
                )
                db.add(event)

                # Find news links (heuristic: a tags with /news/ or /yangilik/ etc.)
                news_links = set()
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if any(kw in href.lower() for kw in ["/news", "/yangilik", "/xabar", "/article", "/post"]):
                        if href.startswith("http"):
                            news_links.add(href)
                        elif href.startswith("/"):
                            news_links.add(uni.website.rstrip("/") + href)

                # CRAWL stage
                crawl_event = ScrapeJobEvent(
                    job_id=job_id,
                    university_id=university_id,
                    stage=ScrapeStage.CRAWL,
                    message=f"Found {len(news_links)} potential news links",
                )
                db.add(crawl_event)
                await db.commit()

                # PARSE stage — try first 20 links
                for url in list(news_links)[:20]:
                    try:
                        page = await client.get(url)
                        psoup = BeautifulSoup(page.text, "html.parser")

                        title_tag = psoup.find("h1") or psoup.find("title")
                        title = title_tag.get_text(strip=True) if title_tag else url

                        # Content extraction
                        content_tag = psoup.find("article") or psoup.find("main") or psoup.find("div", class_=re.compile(r"content|article|post|news", re.I))
                        content_text = content_tag.get_text("\n", strip=True) if content_tag else psoup.get_text("\n", strip=True)[:5000]

                        fingerprint = hashlib.md5(url.encode()).hexdigest()
                        slug = re.sub(r"[^a-z0-9]+", "-", title.lower())[:100]

                        # Check duplicate
                        existing = await db.execute(
                            select(NewsPost).where(NewsPost.source_url == url)
                        )
                        if existing.scalar_one_or_none():
                            continue

                        post = NewsPost(
                            university_id=university_id,
                            title=title[:500],
                            slug=slug,
                            content_text=content_text[:10000],
                            source_url=url,
                            hash_fingerprint=fingerprint,
                            language="uz",
                        )
                        db.add(post)
                        saved_count += 1
                    except Exception:
                        pass

                await db.commit()

                save_event = ScrapeJobEvent(
                    job_id=job_id,
                    university_id=university_id,
                    stage=ScrapeStage.SAVE_POSTS,
                    message=f"Saved {saved_count} posts",
                    counters_json={"saved": saved_count},
                )
                db.add(save_event)

                final_status = ScrapeStatus.DONE if saved_count > 0 else ScrapeStatus.NO_NEWS
                await db.execute(
                    update(University)
                    .where(University.id == university_id)
                    .values(scrape_status=final_status, last_scraped_at=datetime.utcnow())
                )
                await db.execute(
                    update(ScrapeJob)
                    .where(ScrapeJob.id == job_id)
                    .values(
                        status=JobStatus.DONE,
                        finished_at=datetime.utcnow(),
                        totals_json={"saved": saved_count},
                    )
                )
                await db.commit()

        except Exception as exc:
            error_msg = str(exc)[:500]
            await db.execute(
                update(University)
                .where(University.id == university_id)
                .values(scrape_status=ScrapeStatus.FAILED, last_error_message=error_msg)
            )
            await db.execute(
                update(ScrapeJob)
                .where(ScrapeJob.id == job_id)
                .values(status=JobStatus.FAILED, finished_at=datetime.utcnow())
            )
            await db.commit()

        await engine.dispose()


@celery_app.task(name="tasks.scrape_university", bind=True)
def scrape_university(self, university_id: str, job_id: str):
    """Celery task: scrape a single university."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_scrape_university_async(university_id, job_id))
    finally:
        loop.close()
