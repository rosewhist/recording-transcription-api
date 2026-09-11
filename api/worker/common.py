"""Shared worker helpers."""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator, Callable, Union
from uuid import UUID

from api.repository.dao.task import TaskDao

TaskId = Union[UUID, str]


class LeaseLostError(RuntimeError):
    """Raised when the task lease is lost mid-processing; abort without local retry."""


def as_uuid(task_id: TaskId) -> UUID:
    return task_id if isinstance(task_id, UUID) else UUID(str(task_id))


@asynccontextmanager
async def task_tx(db_session_factory: Callable) -> AsyncIterator[TaskDao]:
    """One short DB transaction around TaskDao writes/reads."""
    async with db_session_factory() as session:
        try:
            yield TaskDao(session)
            await session.commit()
        except Exception:
            await session.rollback()
            raise
