"""Worker 公共辅助工具。"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator, Callable, Union
from uuid import UUID

from api.repository.dao.task import TaskDao

TaskId = Union[UUID, str]


class LeaseLostError(RuntimeError):
    """处理过程中租约丢失时抛出；中止当前流程，不做本地重试。"""


def as_uuid(task_id: TaskId) -> UUID:
    return task_id if isinstance(task_id, UUID) else UUID(str(task_id))


@asynccontextmanager
async def task_tx(db_session_factory: Callable) -> AsyncIterator[TaskDao]:
    """围绕 TaskDao 读写的短事务。"""
    async with db_session_factory() as session:
        try:
            yield TaskDao(session)
            await session.commit()
        except Exception:
            await session.rollback()
            raise
