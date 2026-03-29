import os
from fastapi import APIRouter
from app.api.endpoints import universities, news, jobs, auth, stats, api_keys, settings, dashboard, mentalaba

api_router = APIRouter(prefix="/api/v1")
api_router.include_router(auth.router)
api_router.include_router(dashboard.router)
api_router.include_router(universities.router)
api_router.include_router(news.router)
api_router.include_router(jobs.router)
api_router.include_router(stats.router)
api_router.include_router(api_keys.router)
api_router.include_router(settings.router)
api_router.include_router(mentalaba.router)
