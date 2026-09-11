from __future__ import annotations

from uuid import UUID

from api.core.logger import get_logger
from api.domain.exceptions import TaskNotFoundError
from api.repository.dao import Task
from api.repository.task import TaskRepository

logger = get_logger(__name__)


class TaskService:
    def __init__(self, repo: TaskRepository):
        self.repo = repo

    async def get(self, task_id: UUID) -> Task:
        task = await self.repo.get_by_id(task_id)
        if task is None:
            raise TaskNotFoundError(f"任务不存在: {task_id}")
        logger.info("查询任务，task_id=%s，status=%s", task.id, task.status)
        return task
