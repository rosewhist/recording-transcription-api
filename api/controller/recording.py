"""录音路由：上传 / 列表 / 详情 / 删除 / 摘要 SSE 流。"""
from typing import Annotated
from fastapi import APIRouter, File, Response, UploadFile, status
from api.depends import RecordingServiceDep
from api.schema.recording import (
    RecordingUploadResponse,
    to_upload_response,
)

router = APIRouter(tags=["recordings"])


@router.post("/recordings", response_model=RecordingUploadResponse)
async def upload_recording(
        file: Annotated[UploadFile, File(description="音频文件：wav/mp3/m4a/aac，≤50MB")],
        service: RecordingServiceDep,
        response: Response,
):
    recording, task, created = await service.upload(file)
    if task is None:
        raise RuntimeError("task is None")
    response.status_code = status.HTTP_201_CREATED if created else status.HTTP_200_OK
    return to_upload_response(recording, task)
