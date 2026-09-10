from typing import Optional
from uuid import UUID

from api.domain.exceptions import TaskNotFoundError, TaskNotRetryableError
from api.repository.dao.task import Task
from api.repository.dao.task_types import RECOVERABLE_STATUSES, TaskStatus
from api.repository.task import TaskRepository
from api.worker import TaskWorker


class TaskService:
    def __init__(self, repo: TaskRepository, worker: Optional[TaskWorker] = None):
        self.repo = repo
        self.worker = worker
