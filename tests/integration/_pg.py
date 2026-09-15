"""集成测试的行工厂与读取辅助（按需插入真实行，不 mock 任何 DAO/SQL）。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional
from uuid import UUID, uuid4

from sqlmodel.ext.asyncio.session import AsyncSession

from api.repository.dao.recording import Recording
from api.repository.dao.task import Task
from api.repository.dao.task_types import TaskStatus


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def make_recording(
    session: AsyncSession,
    *,
    file_name: str = "demo.wav",
    file_hash: Optional[str] = None,
    created_at: Optional[datetime] = None,
) -> Recording:
    recording = Recording(
        file_name=file_name,
        file_path=f"/tmp/{file_name}",
        file_size=1024,
        file_hash=file_hash or uuid4().hex * 2,  # 64 hex chars，贴合 VARCHAR(64)
    )
    if created_at is not None:
        recording.created_at = created_at
    session.add(recording)
    await session.flush()
    return recording


async def make_task(
    session: AsyncSession,
    recording: Recording,
    *,
    status: str = TaskStatus.PENDING.value,
    locked_by: Optional[str] = None,
    lease_seconds: Optional[float] = None,
    retry_count: int = 0,
    next_retry_at: Optional[datetime] = None,
    created_at: Optional[datetime] = None,
) -> Task:
    """插入一条任务；``lease_seconds`` 为正表示租约未过期，为负表示已过期。"""
    now = utcnow()
    task = Task(
        recording_id=recording.id,
        status=status,
        retry_count=retry_count,
        locked_by=locked_by,
        next_retry_at=next_retry_at or now,
        created_at=created_at or now,
        updated_at=now,
    )
    if lease_seconds is not None:
        task.lease_expires_at = now + timedelta(seconds=lease_seconds)
    session.add(task)
    await session.flush()
    return task


async def fetch_task(session: AsyncSession, task_id: UUID) -> Optional[Task]:
    """重新从库里读，绕过 identity map 里的陈旧副本。"""
    return await session.get(Task, task_id, populate_existing=True)


async def fetch_recording(session: AsyncSession, recording_id: UUID) -> Optional[Recording]:
    return await session.get(Recording, recording_id, populate_existing=True)
