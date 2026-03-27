"""
Seed universities from universities.json into the SQLite database.
Run: ./venv/bin/python seed_universities.py
"""

import asyncio
import json
import os
import sys
from datetime import datetime

# Set working directory to backend folder
os.chdir(os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

async def seed():
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy import select
    from app.core.database import init_db, engine
    from app.models import University, ScrapeStatus

    # Ensure tables exist
    await init_db()

    Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    # Load JSON
    with open("universities.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"📂 Loaded {len(data)} universities from universities.json")

    inserted = 0
    skipped = 0
    updated = 0

    async with Session() as db:
        for item in data:
            uni_id = str(item["id"])

            # Check if already exists
            result = await db.execute(
                select(University).where(University.id == uni_id)
            )
            existing = result.scalar_one_or_none()

            website = item.get("website", "").strip()
            if website and not website.startswith("http"):
                website = "https://" + website

            if existing:
                # Update website if missing
                if not existing.website and website:
                    existing.website = website
                    existing.updated_at = datetime.utcnow()
                    updated += 1
                else:
                    skipped += 1
                continue

            uni = University(
                id=uni_id,
                region_id=str(item.get("region_id", "")) if item.get("region_id") else None,
                name_uz=item.get("name_uz", "").strip(),
                name_en=item.get("name_en", "").strip() or None,
                name_ru=item.get("name_ru", "").strip() or None,
                website=website or None,
                scrape_status=ScrapeStatus.IDLE,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            db.add(uni)
            inserted += 1

            # Commit every 50
            if inserted % 50 == 0:
                await db.commit()
                print(f"  ✅ {inserted} inserted so far...")

        await db.commit()

    print(f"\n🎉 Done!")
    print(f"  ✅ Inserted: {inserted}")
    print(f"  🔄 Updated:  {updated}")
    print(f"  ⏭️  Skipped:  {skipped}")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(seed())
