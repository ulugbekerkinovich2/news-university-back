import sqlite3
import json
import os
from datetime import datetime

# Path to local SQLite db
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "app.db")
OUTPUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "news_dump.json")

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def export():
    print(f"🚀 Exporting news from {DB_PATH}...")
    if not os.path.exists(DB_PATH):
        print(f"❌ Error: Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = dict_factory
    cursor = conn.cursor()

    # 1. Export news_posts
    cursor.execute("SELECT * FROM news_posts")
    posts = cursor.fetchall()
    print(f"  ✅ Found {len(posts)} news posts.")

    # 2. Export media_assets
    cursor.execute("SELECT * FROM media_assets")
    assets = cursor.fetchall()
    print(f"  ✅ Found {len(assets)} media assets.")

    data = {
        "news_posts": posts,
        "media_assets": assets
    }

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"🎉 Done! Exported to {OUTPUT_PATH}")
    conn.close()

if __name__ == "__main__":
    export()
