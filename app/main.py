import os
import asyncio
from dotenv import load_dotenv
load_dotenv()  # loads backend/.env

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from contextlib import asynccontextmanager
from sqladmin import Admin, ModelView

from app.core.database import engine, init_db
from app.api import api_router
from app.models import University, NewsPost, ScrapeJob, ScrapeJobEvent, MediaAsset, User, ApiKey
from app.core.database import AsyncSessionLocal
from app.services.mentalaba import backfill_unsent_exports


from fastapi.staticfiles import StaticFiles

# Ensure required directories exist
os.makedirs("data", exist_ok=True)
os.makedirs("static/logos", exist_ok=True)
MENTALABA_BACKFILL_INTERVAL_SECONDS = int(os.getenv("MENTALABA_BACKFILL_INTERVAL_SECONDS", "120"))

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    stop_event = asyncio.Event()

    async def run_mentalaba_backfill_loop():
        while not stop_event.is_set():
            try:
                async with AsyncSessionLocal() as db:
                    await backfill_unsent_exports(db)
            except Exception:
                # Keep the loop alive even if one sweep fails.
                pass

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=MENTALABA_BACKFILL_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                continue

    backfill_task = asyncio.create_task(run_mentalaba_backfill_loop())
    try:
        yield
    finally:
        stop_event.set()
        backfill_task.cancel()
        try:
            await backfill_task
        except asyncio.CancelledError:
            pass


app = FastAPI(
    title="University News Hub API",
    description="Backend API for university news aggregation and scraping",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS — allow specific origins in production
origins = os.getenv("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API routes
app.include_router(api_router)

# Static files for uploads
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response: Response = await call_next(request)
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "img-src 'self' data: https:; "
        "style-src 'self' 'unsafe-inline' https:; "
        "script-src 'self' 'unsafe-inline' 'unsafe-eval' https:; "
        "connect-src 'self' https: http:; "
        "font-src 'self' data: https:;"
    )
    return response


# ── SQLAdmin ──────────────────────────────────────────────────────────────────

admin = Admin(app, engine, title="University Hub Admin")


class UniversityAdmin(ModelView, model=University):
    name = "University"
    name_plural = "Universities"
    icon = "fa-solid fa-university"
    column_list = [University.id, University.mt_id, University.mt_slug, University.name_uz, University.region_id, University.scrape_status, University.last_scraped_at]
    column_searchable_list = [University.name_uz, University.name_en, University.name_ru, University.id, University.mt_slug]
    column_sortable_list = [University.name_uz, University.scrape_status, University.last_scraped_at, University.mt_id]


class NewsPostAdmin(ModelView, model=NewsPost):
    name = "News Post"
    name_plural = "News Posts"
    icon = "fa-solid fa-newspaper"
    column_list = [NewsPost.id, NewsPost.title, NewsPost.university_id, NewsPost.language, NewsPost.published_at, NewsPost.created_at]
    column_searchable_list = [NewsPost.title, NewsPost.source_url]
    column_sortable_list = [NewsPost.published_at, NewsPost.created_at]


class ScrapeJobAdmin(ModelView, model=ScrapeJob):
    name = "Scrape Job"
    name_plural = "Scrape Jobs"
    icon = "fa-solid fa-robot"
    column_list = [ScrapeJob.id, ScrapeJob.scope, ScrapeJob.university_id, ScrapeJob.status, ScrapeJob.started_at, ScrapeJob.finished_at, ScrapeJob.created_at]
    column_sortable_list = [ScrapeJob.created_at, ScrapeJob.status]


class ScrapeJobEventAdmin(ModelView, model=ScrapeJobEvent):
    name = "Scrape Job Event"
    name_plural = "Scrape Job Events"
    icon = "fa-solid fa-list"
    column_list = [ScrapeJobEvent.id, ScrapeJobEvent.job_id, ScrapeJobEvent.stage, ScrapeJobEvent.message, ScrapeJobEvent.timestamp]


class MediaAssetAdmin(ModelView, model=MediaAsset):
    name = "Media Asset"
    name_plural = "Media Assets"
    icon = "fa-solid fa-image"
    column_list = [MediaAsset.id, MediaAsset.type, MediaAsset.original_url, MediaAsset.post_id, MediaAsset.created_at]


class UserAdmin(ModelView, model=User):
    name = "User"
    name_plural = "Users"
    icon = "fa-solid fa-users"
    column_list = [
        User.id,
        User.email,
        User.display_name,
        User.role,
        User.approval_status,
        User.is_active,
        User.created_at,
    ]
    column_searchable_list = [User.email, User.display_name]
    # Never show hashed_password in list
    form_excluded_columns = [User.hashed_password]


class ApiKeyAdmin(ModelView, model=ApiKey):
    name = "API Key"
    name_plural = "API Keys"
    icon = "fa-solid fa-key"
    column_list = [ApiKey.id, ApiKey.name, ApiKey.key_prefix, ApiKey.is_active, ApiKey.request_count, ApiKey.created_at]


admin.add_view(UniversityAdmin)
admin.add_view(NewsPostAdmin)
admin.add_view(ScrapeJobAdmin)
admin.add_view(ScrapeJobEventAdmin)
admin.add_view(MediaAssetAdmin)
admin.add_view(UserAdmin)
admin.add_view(ApiKeyAdmin)


@app.get("/")
async def health():
    return {"status": "ok", "message": "University News Hub API is running"}


# ── Additional Documentation ────────────────────────────────────────────────

from fastapi.openapi.docs import get_swagger_ui_html, get_redoc_html

@app.get("/api-docs", include_in_schema=False)
async def custom_swagger_ui_html():
    return get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title=app.title + " - Interactive Docs",
        oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
        swagger_js_url="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js",
        swagger_css_url="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css",
    )

@app.get("/public-docs", include_in_schema=False)
async def custom_redoc_html():
    return get_redoc_html(
        openapi_url=app.openapi_url,
        title=app.title + " - Redoc",
        redoc_js_url="https://unpkg.com/redoc@next/bundles/redoc.standalone.js",
    )
