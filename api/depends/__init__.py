from typing import Annotated

from fastapi import Depends, Request

from api.service.recording import RecordingService
from api.service.task import TaskService


def get_recording_service(request: Request) -> RecordingService:
    return request.app.state.recording_service


def get_task_service(request: Request) -> TaskService:
    return request.app.state.task_service


RecordingServiceDep = Annotated[RecordingService, Depends(get_recording_service)]
TaskServiceDep = Annotated[TaskService, Depends(get_task_service)]
