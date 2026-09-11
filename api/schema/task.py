from __future__ import annotations

from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel

from api.repository.dao.task import Task
from api.repository.dao.task_types import TaskStatus


class TaskStatusResponse(BaseModel):
    task_id: UUID
    recording_id: UUID
    status: TaskStatus
    retry_count: int
    error_msg: Optional[str] = None
    transcript: Optional[str] = None
    summary: Optional[dict[str, Any]] = None


def to_task_status_response(task: Task) -> TaskStatusResponse:
    return TaskStatusResponse(
        task_id=task.id,
        recording_id=task.recording_id,
        status=TaskStatus(task.status),
        retry_count=task.retry_count,
        error_msg=task.error_msg,
        transcript=task.transcript,
        summary=task.summary,
    )
