import asyncio
import os
import sys

# Ensure backend directory is in path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from app.core.database import init_db
# Import all models to ensure they are registered with Base.metadata
from app.models import University, NewsPost, ScrapeJob, ScrapeJobEvent, MediaAsset, User, ApiKey

async def main():
    print("🚀 Creating database tables on the server...")
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("❌ Error: DATABASE_URL not found in .env file")
        return
    
    print(f"📡 Using database: {database_url.split('@')[-1]}") # Log host only for security
    
    try:
        await init_db()
        print("✅ Database tables created successfully!")
    except Exception as e:
        print(f"❌ Error creating tables: {e}")

if __name__ == "__main__":
    asyncio.run(main())
