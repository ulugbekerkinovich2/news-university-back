import uuid
import enum
from datetime import datetime
from sqlalchemy import (
    Column, String, Text, DateTime, Float, Integer, Boolean,
    Enum, ForeignKey, BigInteger, JSON
)
from sqlalchemy.orm import relationship
from app.core.database import Base


# ── Enums ────────────────────────────────────────────────────────────────────

class ScrapeStatus(str, enum.Enum):
    IDLE = "IDLE"
    IN_PROGRESS = "IN_PROGRESS"
    DONE = "DONE"
    FAILED = "FAILED"
    NO_SOURCE = "NO_SOURCE"
    NO_NEWS = "NO_NEWS"


class JobStatus(str, enum.Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class JobScope(str, enum.Enum):
    ALL_UNIVERSITIES = "ALL_UNIVERSITIES"
    SINGLE_UNIVERSITY = "SINGLE_UNIVERSITY"


class ScrapeStage(str, enum.Enum):
    DISCOVER = "DISCOVER"
    CRAWL = "CRAWL"
    PARSE = "PARSE"
    SAVE_POSTS = "SAVE_POSTS"
    SAVE_MEDIA = "SAVE_MEDIA"
    DONE = "DONE"


class MediaType(str, enum.Enum):
    image = "image"
    video = "video"


class AppRole(str, enum.Enum):
    admin = "admin"
    user = "user"


# ── Models ───────────────────────────────────────────────────────────────────

class University(Base):
    __tablename__ = "universities"

    id = Column(String, primary_key=True, index=True)
    region_id = Column(String, nullable=True, index=True)
    name_uz = Column(String, nullable=False)
    name_en = Column(String, nullable=True)
    name_ru = Column(String, nullable=True)
    website = Column(String, nullable=True)
    logo_url = Column(String, nullable=True)
    
    # Metadata for external API linking
    mt_id = Column(Integer, nullable=True, index=True)
    mt_slug = Column(String, nullable=True, index=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    scrape_status = Column(Enum(ScrapeStatus), default=ScrapeStatus.IDLE, nullable=False, index=True)
    last_scraped_at = Column(DateTime, nullable=True)
    last_error_message = Column(Text, nullable=True)

    news_posts = relationship("NewsPost", back_populates="university", cascade="all, delete-orphan")
    scrape_jobs = relationship("ScrapeJob", back_populates="university")


class MediaAsset(Base):
    __tablename__ = "media_assets"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    post_id = Column(String, ForeignKey("news_posts.id", ondelete="CASCADE"), nullable=True, index=True)
    type = Column(Enum(MediaType), default=MediaType.image, nullable=False)
    original_url = Column(String, nullable=False)
    stored_url = Column(String, nullable=True)
    provider = Column(String, nullable=True)
    width = Column(Integer, nullable=True)
    height = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    post = relationship("NewsPost", back_populates="media_assets", foreign_keys=[post_id])


class NewsPost(Base):
    __tablename__ = "news_posts"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    university_id = Column(String, ForeignKey("universities.id", ondelete="CASCADE"), nullable=False, index=True)
    title = Column(Text, nullable=False)
    slug = Column(String, nullable=False)
    summary = Column(Text, nullable=True)
    content_html = Column(Text, nullable=True)
    content_text = Column(Text, nullable=True)
    published_at = Column(DateTime, nullable=True, index=True)
    source_url = Column(String, nullable=False, unique=True)
    canonical_url = Column(String, nullable=True)
    language = Column(String, default="unknown")
    cover_image_id = Column(String, ForeignKey("media_assets.id", ondelete="SET NULL"), nullable=True)
    hash_fingerprint = Column(String, nullable=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    university = relationship("University", back_populates="news_posts")
    media_assets = relationship("MediaAsset", back_populates="post", foreign_keys=[MediaAsset.post_id])
    cover_image = relationship("MediaAsset", foreign_keys=[cover_image_id])


class ScrapeJob(Base):
    __tablename__ = "scrape_jobs"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    scope = Column(Enum(JobScope), default=JobScope.SINGLE_UNIVERSITY, nullable=False)
    university_id = Column(String, ForeignKey("universities.id", ondelete="CASCADE"), nullable=True, index=True)
    status = Column(Enum(JobStatus), default=JobStatus.QUEUED, nullable=False, index=True)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    totals_json = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    university = relationship("University", back_populates="scrape_jobs")
    events = relationship("ScrapeJobEvent", back_populates="job", cascade="all, delete-orphan")


class ScrapeJobEvent(Base):
    __tablename__ = "scrape_job_events"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    job_id = Column(String, ForeignKey("scrape_jobs.id", ondelete="CASCADE"), nullable=False, index=True)
    university_id = Column(String, ForeignKey("universities.id", ondelete="CASCADE"), nullable=True)
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    stage = Column(Enum(ScrapeStage), nullable=False)
    message = Column(Text, nullable=True)
    counters_json = Column(JSON, default=dict)

    job = relationship("ScrapeJob", back_populates="events")


class User(Base):
    __tablename__ = "users"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    email = Column(String, unique=True, nullable=False, index=True)
    display_name = Column(String, nullable=True)
    hashed_password = Column(String, nullable=False)
    role = Column(Enum(AppRole), default=AppRole.user, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class SystemSetting(Base):
    """Key-value store for app configuration (e.g. scheduler interval)."""
    __tablename__ = "system_settings"

    key = Column(String, primary_key=True)
    value = Column(Text, nullable=False)
    description = Column(String, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class ApiKey(Base):
    __tablename__ = "api_keys"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, nullable=False)
    key_hash = Column(String, nullable=False, unique=True)
    key_prefix = Column(String, nullable=False)
    created_by = Column(String, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    rate_limit_per_minute = Column(Integer, default=60)
    rate_limit_per_day = Column(Integer, default=10000)
    is_active = Column(Boolean, default=True)
    last_used_at = Column(DateTime, nullable=True)
    request_count = Column(BigInteger, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    expires_at = Column(DateTime, nullable=True)
