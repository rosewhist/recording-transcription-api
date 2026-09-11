from __future__ import annotations

from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel

from api.repository.dao.recording import Recording
from api.repository.dao.task import Task
from api.repository.dao.task_types import TaskStatus


class RecordingUploadResponse(BaseModel):
    recording_id: UUID
    task_id: UUID
    status: TaskStatus


class RecordingListItem(BaseModel):
    recording_id: UUID
    file_name: str
    file_size: int
    created_at: datetime
    task_id: Optional[UUID] = None
    status: Optional[TaskStatus] = None


class RecordingListResponse(BaseModel):
    items: list[RecordingListItem]
    page: int
    page_size: int
    total: int


class RecordingDetailResponse(BaseModel):
    recording_id: UUID
    file_name: str
    file_size: int
    created_at: datetime
    task_id: UUID
    status: TaskStatus
    error_msg: Optional[str] = None
    transcript: Optional[str] = None
    summary: Optional[dict[str, Any]] = None


def to_upload_response(recording: Recording, task: Task) -> RecordingUploadResponse:
    return RecordingUploadResponse(
        recording_id=recording.id,
        task_id=task.id,
        status=TaskStatus(task.status),
    )


def to_list_item(recording: Recording, task: Optional[Task]) -> RecordingListItem:
    return RecordingListItem(
        recording_id=recording.id,
        file_name=recording.file_name,
        file_size=recording.file_size,
        created_at=recording.created_at,
        task_id=task.id if task is not None else None,
        status=TaskStatus(task.status) if task is not None else None,
    )


def to_list_response(
    items: list[tuple[Recording, Optional[Task]]],
    *,
    page: int,
    page_size: int,
    total: int,
) -> RecordingListResponse:
    return RecordingListResponse(
        items=[to_list_item(recording, task) for recording, task in items],
        page=page,
        page_size=page_size,
        total=total,
    )


def to_detail_response(recording: Recording, task: Task) -> RecordingDetailResponse:
    status = TaskStatus(task.status)
    done = status == TaskStatus.DONE
    return RecordingDetailResponse(
        recording_id=recording.id,
        file_name=recording.file_name,
        file_size=recording.file_size,
        created_at=recording.created_at,
        task_id=task.id,
        status=status,
        error_msg=task.error_msg,
        transcript=task.transcript if done else None,
        summary=task.summary if done else None,
    )
