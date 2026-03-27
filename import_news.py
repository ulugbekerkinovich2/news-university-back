import asyncio
import json
import os
import sys
from datetime import datetime

# Ensure backend directory is in path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from app.core.database import engine, AsyncSessionLocal
from app.models import NewsPost, MediaAsset
from sqlalchemy import select

INPUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "news_dump.json")

async def import_data():
    print(f"🚀 Importing news from {INPUT_PATH}...")
    if not os.path.exists(INPUT_PATH):
        print(f"❌ Error: {INPUT_PATH} not found.")
        return

    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    posts_data = data.get("news_posts", [])
    assets_data = data.get("media_assets", [])

    print(f"  📦 Found {len(posts_data)} posts and {len(assets_data)} assets in dump.")

    async with AsyncSessionLocal() as db:
        # 1. Import News Posts (First pass - no cover_image_id)
        print("  📝 Importing news posts (first pass)...")
        cover_image_map = {} # post_id -> cover_image_id
        inserted_posts = 0
        for p in posts_data:
            post_id = p["id"]
            cover_image_id = p.pop("cover_image_id", None)
            if cover_image_id:
                cover_image_map[post_id] = cover_image_id

            # Check if exists
            res = await db.execute(select(NewsPost).where(NewsPost.id == post_id))
            if res.scalar_one_or_none():
                continue

            # Parse dates
            for date_field in ["published_at", "created_at", "updated_at"]:
                if p.get(date_field):
                    try:
                        p[date_field] = datetime.fromisoformat(p[date_field].replace("Z", "+00:00"))
                    except:
                        p[date_field] = datetime.utcnow()

            post = NewsPost(**p)
            post.cover_image_id = None # Set to None for now to avoid FK error
            db.add(post)
            inserted_posts += 1
            if inserted_posts % 100 == 0:
                await db.commit()
                print(f"    ✅ {inserted_posts} posts inserted...")

        await db.commit()
        print(f"  ✅ Finished posts first pass. Total inserted: {inserted_posts}")

        # 2. Import Media Assets
        print("  🖼️ Importing media assets...")
        inserted_assets = 0
        for a in assets_data:
            res = await db.execute(select(MediaAsset).where(MediaAsset.id == a["id"]))
            if res.scalar_one_or_none():
                continue

            if a.get("created_at"):
                try:
                    a["created_at"] = datetime.fromisoformat(a["created_at"].replace("Z", "+00:00"))
                except:
                    a["created_at"] = datetime.utcnow()

            asset = MediaAsset(**a)
            db.add(asset)
            inserted_assets += 1
            if inserted_assets % 100 == 0:
                await db.commit()
                print(f"    ✅ {inserted_assets} assets inserted...")

        await db.commit()
        print(f"  ✅ Finished assets. Total inserted: {inserted_assets}")

        # 3. Update News Posts with cover_image_id
        print("  🔗 Updating news posts with cover image IDs...")
        updated_posts = 0
        for post_id, cover_image_id in cover_image_map.items():
            res = await db.execute(select(NewsPost).where(NewsPost.id == post_id))
            post = res.scalar_one_or_none()
            if post:
                post.cover_image_id = cover_image_id
                updated_posts += 1
                if updated_posts % 100 == 0:
                    await db.commit()
                    print(f"    ✅ {updated_posts} posts updated...")
        
        await db.commit()
        print(f"  ✅ Finished updates. Total updated: {updated_posts}")

    print("🎉 Data migration complete!")

if __name__ == "__main__":
    asyncio.run(import_data())
