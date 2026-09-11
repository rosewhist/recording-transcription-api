from typing import Annotated

from fastapi import Depends, Request

from api.service.recording import RecordingService


def get_recording_service(request: Request) -> RecordingService:
    return request.app.state.recording_service


RecordingServiceDep = Annotated[RecordingService, Depends(get_recording_service)]
