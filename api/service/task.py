from __future__ import annotations

from typing import Optional
from uuid import UUID

from api.core.logger import get_logger, log_context
from api.domain.exceptions import (
    TaskNotFoundError,
    TaskNotRetryableError,
)
from api.repository.dao import Task
from api.repository.dao.task_types import RECOVERABLE_STATUS_VALUES, TaskStatus
from api.repository.task import TaskRepository
from api.worker import TaskWorker

logger = get_logger(__name__)


class TaskService:
    def __init__(
        self,
        repo: TaskRepository,
        worker: Optional[TaskWorker] = None,
    ):
        self.repo = repo
        self.worker = worker

    async def get(self, task_id: UUID) -> Task:
        with log_context(task_id=task_id):
            task = await self.repo.get_by_id(task_id)
            if task is None:
                raise TaskNotFoundError(f"任务不存在: {task_id}")
            logger.info("查询任务", event="task.fetched", status=task.status)
            return task

    async def retry(self, task_id: UUID) -> Task:
        with log_context(task_id=task_id):
            task = await self.repo.requeue_failed(task_id)
            if task is not None:
                logger.info("任务已重入队", event="task.retry", status=task.status)
                if self.worker is not None:
                    await self.worker.submit(task.id)
                return task

            task = await self.repo.get_by_id(task_id)
            if task is None:
                raise TaskNotFoundError(f"任务不存在: {task_id}")

            if task.status in RECOVERABLE_STATUS_VALUES:
                logger.info(
                    "重试请求幂等命中",
                    event="task.retry.idempotent",
                    status=task.status,
                )
                return task

            if task.status == TaskStatus.FAILED.value:
                # Rare race: became failed again or first update lost; try once more.
                task = await self.repo.requeue_failed(task_id)
                if task is not None:
                    logger.info(
                        "任务竞态后重入队", event="task.retry", status=task.status
                    )
                    if self.worker is not None:
                        await self.worker.submit(task.id)
                    return task
                task = await self.repo.get_by_id(task_id)
                if task is None:
                    raise TaskNotFoundError(f"任务不存在: {task_id}")
                if task.status in RECOVERABLE_STATUS_VALUES:
                    return task

        raise TaskNotRetryableError(
            f"仅 failed 状态可重试，当前状态: {task.status}"
        )
