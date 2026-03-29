import os
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import inspect

# Keep the app aligned with the scraper/README defaults so local runs use one DB.
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./data/app.db")

engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()


async def get_db():
    async with AsyncSessionLocal() as session:
        yield session


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.run_sync(_migrate_existing_schema)


def _migrate_existing_schema(sync_conn):
    inspector = inspect(sync_conn)
    dialect = sync_conn.dialect.name
    tables = set(inspector.get_table_names())
    if "users" not in tables:
        return

    columns = {col["name"] for col in inspector.get_columns("users")}
    statements = []

    if "approval_status" not in columns:
        statements.append(
            "ALTER TABLE users ADD COLUMN approval_status VARCHAR(32) NOT NULL DEFAULT 'APPROVED'"
        )
    if "permissions_json" not in columns:
        statements.append(
            "ALTER TABLE users ADD COLUMN permissions_json TEXT NOT NULL DEFAULT '[]'"
        )
    if "approved_by" not in columns:
        statements.append("ALTER TABLE users ADD COLUMN approved_by VARCHAR")
    if "approved_at" not in columns:
        timestamp_type = "TIMESTAMP" if dialect == "postgresql" else "DATETIME"
        statements.append(f"ALTER TABLE users ADD COLUMN approved_at {timestamp_type}")
    if "failed_login_attempts" not in columns:
        statements.append("ALTER TABLE users ADD COLUMN failed_login_attempts INTEGER NOT NULL DEFAULT 0")
    if "locked_until" not in columns:
        timestamp_type = "TIMESTAMP" if dialect == "postgresql" else "DATETIME"
        statements.append(f"ALTER TABLE users ADD COLUMN locked_until {timestamp_type}")
    if "last_login_at" not in columns:
        timestamp_type = "TIMESTAMP" if dialect == "postgresql" else "DATETIME"
        statements.append(f"ALTER TABLE users ADD COLUMN last_login_at {timestamp_type}")

    for statement in statements:
        sync_conn.exec_driver_sql(statement)

    if "news_posts" not in tables:
        return

    news_columns = {col["name"] for col in inspector.get_columns("news_posts")}
    news_statements = []
    if "moderation_status" not in news_columns:
        news_statements.append(
            "ALTER TABLE news_posts ADD COLUMN moderation_status VARCHAR(32) NOT NULL DEFAULT 'APPROVED'"
        )
    if "moderation_notes" not in news_columns:
        news_statements.append("ALTER TABLE news_posts ADD COLUMN moderation_notes TEXT")
    if "moderated_by" not in news_columns:
        news_statements.append("ALTER TABLE news_posts ADD COLUMN moderated_by VARCHAR")
    if "moderated_at" not in news_columns:
        timestamp_type = "TIMESTAMP" if dialect == "postgresql" else "DATETIME"
        news_statements.append(f"ALTER TABLE news_posts ADD COLUMN moderated_at {timestamp_type}")

    for statement in news_statements:
        sync_conn.exec_driver_sql(statement)
