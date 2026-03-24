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


@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs("data", exist_ok=True)
    await init_db()
    yield


app = FastAPI(
    title="University News Hub API",
    description="Backend API for university news aggregation and scraping",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS — allow all origins for local dev
origins = os.getenv("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API routes
app.include_router(api_router)


# ── SQLAdmin ──────────────────────────────────────────────────────────────────

admin = Admin(app, engine, title="University Hub Admin")


class UniversityAdmin(ModelView, model=University):
    name = "University"
    name_plural = "Universities"
    icon = "fa-solid fa-university"
    column_list = [University.id, University.name_uz, University.region_id, University.scrape_status, University.last_scraped_at]
    column_searchable_list = [University.name_uz, University.name_en, University.name_ru, University.id]
    column_sortable_list = [University.name_uz, University.scrape_status, University.last_scraped_at]


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
    column_list = [User.id, User.email, User.display_name, User.role, User.is_active, User.created_at]
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
