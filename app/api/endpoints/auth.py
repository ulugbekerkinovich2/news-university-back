import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import create_access_token, decode_token, get_password_hash, verify_password
from app.models import AppRole, User

router = APIRouter(prefix="/auth", tags=["Auth"])
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/token")
MAX_FAILED_LOGIN_ATTEMPTS = 5
LOGIN_LOCK_MINUTES = 15

ADMIN_PERMISSIONS = [
    "view_universities",
    "manage_news",
    "manage_users",
    "manage_universities",
    "manage_scraping",
    "manage_api_keys",
    "manage_settings",
    "view_dashboard",
    "export_data",
]
USER_PERMISSIONS = [
    "view_dashboard",
    "view_universities",
]
ALLOWED_PERMISSIONS = sorted(set(ADMIN_PERMISSIONS + USER_PERMISSIONS))


class UserRegister(BaseModel):
    email: str
    password: str = Field(min_length=6)
    display_name: Optional[str] = None


class UserApprovePayload(BaseModel):
    role: str = "user"
    permissions: List[str] = []
    is_active: bool = True


class UserUpdatePayload(BaseModel):
    role: Optional[str] = None
    permissions: Optional[List[str]] = None
    is_active: Optional[bool] = None
    approval_status: Optional[str] = None


class UserOut(BaseModel):
    id: str
    email: str
    display_name: Optional[str]
    role: str
    is_active: bool
    approval_status: str
    permissions: List[str]
    approved_by: Optional[str]
    approved_at: Optional[datetime]
    created_at: datetime


class RegisterResponse(BaseModel):
    user: UserOut
    message: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str
    user: UserOut


def _normalize_email(email: str) -> str:
    normalized = (email or "").strip().lower()
    if not normalized or "@" not in normalized:
        raise HTTPException(status_code=400, detail="Valid email is required")
    return normalized


def _validate_password_strength(password: str) -> None:
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")
    if not re.search(r"[A-Za-z]", password):
        raise HTTPException(status_code=400, detail="Password must include a letter")
    if not re.search(r"\d", password):
        raise HTTPException(status_code=400, detail="Password must include a number")


def _normalize_role(role: Optional[str]) -> AppRole:
    value = (role or "user").lower()
    if value not in {AppRole.admin.value, AppRole.user.value}:
        raise HTTPException(status_code=400, detail="Invalid role")
    return AppRole(value)


def _normalize_permissions(permissions: Optional[List[str]]) -> List[str]:
    normalized = sorted({permission for permission in permissions or [] if permission})
    invalid = [permission for permission in normalized if permission not in ALLOWED_PERMISSIONS]
    if invalid:
        raise HTTPException(status_code=400, detail=f"Invalid permissions: {', '.join(invalid)}")
    return normalized


def _role_value(user: User) -> str:
    return str(user.role.value if hasattr(user.role, "value") else user.role)


def _stored_permissions(user: User) -> List[str]:
    raw = getattr(user, "permissions_json", "[]") or "[]"
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed if isinstance(item, str)]


def _effective_permissions(user: User) -> List[str]:
    if _role_value(user) == AppRole.admin.value:
        return ADMIN_PERMISSIONS
    return sorted(set(USER_PERMISSIONS + _stored_permissions(user)))


def _serialize_user(user: User) -> UserOut:
    return UserOut(
        id=user.id,
        email=user.email,
        display_name=user.display_name,
        role=_role_value(user),
        is_active=user.is_active,
        approval_status=getattr(user, "approval_status", "APPROVED") or "APPROVED",
        permissions=_effective_permissions(user),
        approved_by=getattr(user, "approved_by", None),
        approved_at=getattr(user, "approved_at", None),
        created_at=user.created_at,
    )


async def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_db),
) -> User:
    payload = decode_token(token)
    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    user_id = payload.get("sub")
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
    if not user.is_active:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Account is inactive")
    if getattr(user, "approval_status", "APPROVED") != "APPROVED":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Account approval is pending")
    return user


async def require_admin(current_user: User = Depends(get_current_user)) -> User:
    if current_user.role != AppRole.admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")
    return current_user


def require_permission(permission: str):
    async def dependency(current_user: User = Depends(get_current_user)) -> User:
        if current_user.role == AppRole.admin:
            return current_user
        if permission not in _effective_permissions(current_user):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Permission denied")
        return current_user

    return dependency


def require_any_permission(permissions: List[str]):
    async def dependency(current_user: User = Depends(get_current_user)) -> User:
        if current_user.role == AppRole.admin:
            return current_user
        effective_permissions = _effective_permissions(current_user)
        if not any(permission in effective_permissions for permission in permissions):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Permission denied")
        return current_user

    return dependency


@router.post("/register", response_model=RegisterResponse, status_code=201)
async def register(data: UserRegister, db: AsyncSession = Depends(get_db)):
    email = _normalize_email(data.email)
    _validate_password_strength(data.password)

    existing = await db.execute(select(User).where(User.email == email))
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Email already registered")

    users_count = (await db.execute(select(User.id))).scalars().first()
    is_first_user = users_count is None
    now = datetime.utcnow()

    user = User(
        email=email,
        display_name=(data.display_name or "").strip() or None,
        hashed_password=get_password_hash(data.password),
        role=AppRole.admin if is_first_user else AppRole.user,
        is_active=True if is_first_user else False,
        approval_status="APPROVED" if is_first_user else "PENDING",
        permissions_json=json.dumps(ADMIN_PERMISSIONS if is_first_user else USER_PERMISSIONS),
        approved_at=now if is_first_user else None,
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)

    message = (
        "Birinchi admin akkaunt yaratildi."
        if is_first_user
        else "So'rov yuborildi. Admin tasdiqlagach tizimga kira olasiz."
    )
    return {"user": _serialize_user(user), "message": message}


@router.post("/token", response_model=TokenResponse)
async def login(form: OAuth2PasswordRequestForm = Depends(), db: AsyncSession = Depends(get_db)):
    email = _normalize_email(form.username)
    result = await db.execute(select(User).where(User.email == email))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=401, detail="Incorrect email or password")
    now = datetime.utcnow()
    if getattr(user, "locked_until", None) and user.locked_until > now:
        remaining_minutes = max(1, int((user.locked_until - now).total_seconds() // 60))
        raise HTTPException(status_code=423, detail=f"Account is temporarily locked for {remaining_minutes} minute(s)")
    if not verify_password(form.password, user.hashed_password):
        user.failed_login_attempts = int(getattr(user, "failed_login_attempts", 0) or 0) + 1
        if user.failed_login_attempts >= MAX_FAILED_LOGIN_ATTEMPTS:
            user.locked_until = now + timedelta(minutes=LOGIN_LOCK_MINUTES)
            user.failed_login_attempts = 0
        await db.commit()
        raise HTTPException(status_code=401, detail="Incorrect email or password")
    if getattr(user, "approval_status", "APPROVED") == "PENDING":
        raise HTTPException(status_code=403, detail="Account approval is pending")
    if getattr(user, "approval_status", "APPROVED") == "REJECTED":
        raise HTTPException(status_code=403, detail="Account request was rejected")
    if not user.is_active:
        raise HTTPException(status_code=403, detail="Account is inactive")

    user.failed_login_attempts = 0
    user.locked_until = None
    user.last_login_at = now
    await db.commit()
    await db.refresh(user)
    token = create_access_token(data={"sub": user.id})
    return {"access_token": token, "token_type": "bearer", "user": _serialize_user(user)}


@router.get("/me", response_model=UserOut)
async def me(current_user: User = Depends(get_current_user)):
    return _serialize_user(current_user)


@router.get("/permissions", response_model=List[str], dependencies=[Depends(require_admin)])
async def list_permissions():
    return ALLOWED_PERMISSIONS


@router.get("/users", response_model=List[UserOut], dependencies=[Depends(require_admin)])
async def list_users(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).order_by(User.created_at.desc()))
    return [_serialize_user(user) for user in result.scalars().all()]


@router.put("/users/{user_id}/approve", response_model=UserOut)
async def approve_user(
    user_id: str,
    payload: UserApprovePayload,
    db: AsyncSession = Depends(get_db),
    current_admin: User = Depends(require_admin),
):
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.role = _normalize_role(payload.role)
    user.permissions_json = json.dumps(_normalize_permissions(payload.permissions))
    user.is_active = payload.is_active
    user.approval_status = "APPROVED"
    user.approved_by = current_admin.id
    user.approved_at = datetime.utcnow()

    await db.commit()
    await db.refresh(user)
    return _serialize_user(user)


@router.put("/users/{user_id}", response_model=UserOut)
async def update_user_access(
    user_id: str,
    payload: UserUpdatePayload,
    db: AsyncSession = Depends(get_db),
    current_admin: User = Depends(require_admin),
):
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if payload.role is not None:
        user.role = _normalize_role(payload.role)
    if payload.permissions is not None:
        user.permissions_json = json.dumps(_normalize_permissions(payload.permissions))
    if payload.is_active is not None:
        user.is_active = payload.is_active
    if payload.approval_status is not None:
        normalized_status = payload.approval_status.upper()
        if normalized_status not in {"PENDING", "APPROVED", "REJECTED"}:
            raise HTTPException(status_code=400, detail="Invalid approval status")
        user.approval_status = normalized_status
        if normalized_status == "APPROVED":
            user.approved_by = current_admin.id
            user.approved_at = datetime.utcnow()

    await db.commit()
    await db.refresh(user)
    return _serialize_user(user)
