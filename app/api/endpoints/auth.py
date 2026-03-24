from datetime import datetime
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from pydantic import BaseModel

from app.core.database import get_db
from app.core.security import verify_password, get_password_hash, create_access_token, decode_token
from app.models import User, AppRole

router = APIRouter(prefix="/auth", tags=["Auth"])
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/token")


class UserRegister(BaseModel):
    email: str
    password: str
    display_name: Optional[str] = None


class UserOut(BaseModel):
    id: str
    email: str
    display_name: Optional[str]
    role: str
    is_active: bool
    created_at: datetime

    class Config:
        orm_mode = True


class TokenResponse(BaseModel):
    access_token: str
    token_type: str
    user: UserOut


async def get_current_user(token: str = Depends(oauth2_scheme), db: AsyncSession = Depends(get_db)) -> User:
    payload = decode_token(token)
    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    user_id = payload.get("sub")
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found or inactive")
    return user


async def require_admin(current_user: User = Depends(get_current_user)) -> User:
    if current_user.role != AppRole.admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")
    return current_user


@router.post("/register", response_model=UserOut, status_code=201)
async def register(data: UserRegister, db: AsyncSession = Depends(get_db)):
    existing = await db.execute(select(User).where(User.email == data.email))
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Email already registered")
    user = User(
        email=data.email,
        display_name=data.display_name,
        hashed_password=get_password_hash(data.password),
        role=AppRole.admin,  # First registered user becomes admin
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)
    return user


@router.post("/token", response_model=TokenResponse)
async def login(form: OAuth2PasswordRequestForm = Depends(), db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(User.email == form.username))
    user = result.scalar_one_or_none()
    if not user or not verify_password(form.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Incorrect email or password")
    token = create_access_token(data={"sub": user.id})
    return {"access_token": token, "token_type": "bearer", "user": user}


@router.get("/me", response_model=UserOut)
async def me(current_user: User = Depends(get_current_user)):
    return current_user


@router.get("/users", response_model=List[UserOut])
async def list_users(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).order_by(User.created_at.desc()))
    return result.scalars().all()
