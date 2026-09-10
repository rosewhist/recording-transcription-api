from datetime import datetime, timezone
from typing import Optional
from uuid import UUID, uuid4

from sqlalchemy import Column, JSON
from sqlmodel import Field, SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

from api.repository.dao.task_types import RECOVERABLE_STATUSES, TERMINAL_STATUSES, TaskStatus

__all__ = [
    "Task",
    "TaskDao",
    "TaskStatus",
    "RECOVERABLE_STATUSES",
    "TERMINAL_STATUSES",
]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Task(SQLModel, table=True):
    __tablename__ = "tasks"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    recording_id: UUID = Field(foreign_key="recordings.id", unique=True, index=True)  # 一条录音一个任务
    status: str = Field(default=TaskStatus.PENDING.value, max_length=20, index=True)
    transcript: Optional[str] = Field(default=None)
    summary: Optional[str] = Field(default=None)
    key_points: Optional[list] = Field(default=None, sa_column=Column(JSON))
    todos: Optional[list] = Field(default=None, sa_column=Column(JSON))
    error_message: Optional[str] = Field(default=None, max_length=1000)
    attempts: int = Field(default=0)  # 累计流水线运行次数（含自动重试）
    created_at: datetime = Field(default_factory=_utcnow, index=True)
    updated_at: datetime = Field(default_factory=_utcnow)


class TaskDao:
    """任务表的基础封装（单任务短事务操作）。"""

    def __init__(self, session: AsyncSession):
        self.session = session
