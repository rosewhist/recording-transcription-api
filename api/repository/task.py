"""任务仓储。"""
from __future__ import annotations

from typing import Optional
from uuid import UUID

from api.core.db.db_connector import AsyncSessionFactory
from api.repository.dao import Task
from api.repository.dao.task import TaskDao


class TaskRepository:
    async def get_by_id(self, task_id: UUID) -> Optional[Task]:
        async with AsyncSessionFactory() as session:
            return await TaskDao(session).get_by_id(task_id)

    async def requeue_failed(self, task_id: UUID) -> Optional[Task]:
        async with AsyncSessionFactory() as session:
            task = await TaskDao(session).requeue_failed(task_id)
            if task is not None:
                await session.commit()
            return task
