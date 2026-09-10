from typing import Optional
from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlmodel import Field, SQLModel


class User(SQLModel, table=True):
    __tablename__ = "users"

    id: UUID = Field(default_factory=uuid4, primary_key=True)

    # 基础信息
    username: str = Field(max_length=50, unique=True, index=True, nullable=False)
    email: Optional[str] = Field(max_length=255, unique=True, index=True, nullable=True)

    hashed_password: Optional[str] = Field(max_length=255, nullable=True)

    # 时间戳
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
