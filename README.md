# University Hub — Backend

FastAPI backend for the University News Hub project.

## Stack
- **FastAPI** + **SQLAlchemy (async)** + **SQLite**
- **SQLAdmin** admin panel at `/admin`
- **Celery** with filesystem broker (no Docker needed)
- **Python 3.10**

## Setup

```bash
python3.10 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
# API server
./start.sh
# → http://localhost:8000
# → http://localhost:8000/docs  (Swagger)
# → http://localhost:8000/admin (Admin panel)

# Celery worker (separate terminal)
./start_worker.sh
```

## Create First Admin

```bash
curl -X POST http://localhost:8000/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@example.com","password":"yourpassword"}'
```

## API Endpoints

| Prefix | Description |
|--------|-------------|
| `/api/v1/auth` | Register, login, user info |
| `/api/v1/universities` | CRUD + search + pagination |
| `/api/v1/news` | News posts with filters |
| `/api/v1/jobs` | Scrape job management |
| `/api/v1/stats` | Dashboard statistics |
| `/api/v1/api-keys` | API key management |

## Environment

```
DATABASE_URL=sqlite+aiosqlite:///./data/app.db
SECRET_KEY=your-secret-key
```
