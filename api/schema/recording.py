from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel

from api.repository.dao.recording import Recording
from api.repository.dao.task import Task
from api.repository.dao.task_types import TaskStatus


class RecordingUploadResponse(BaseModel):
    recording_id: UUID
    task_id: UUID
    status: TaskStatus


def to_upload_response(recording: Recording, task: Task) -> RecordingUploadResponse:
    return RecordingUploadResponse(
        recording_id=recording.id,
        task_id=task.id,
        status=TaskStatus(task.status),
    )
