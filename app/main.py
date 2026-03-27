import os
from dotenv import load_dotenv
load_dotenv()  # loads backend/.env

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from sqladmin import Admin, ModelView

from app.core.database import engine, init_db
from app.api import api_router
from app.models import University, NewsPost, ScrapeJob, ScrapeJobEvent, MediaAsset, User, ApiKey


from fastapi.staticfiles import StaticFiles

# Ensure required directories exist
os.makedirs("data", exist_ok=True)
os.makedirs("static/logos", exist_ok=True)

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


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
