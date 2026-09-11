"""录音路由：上传 / 列表 / 详情 / 删除。"""
from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, File, Query, Response, UploadFile, status

from api.depends import RecordingServiceDep
from api.schema.recording import (
    RecordingDetailResponse,
    RecordingListResponse,
    RecordingUploadResponse,
    to_detail_response,
    to_list_response,
    to_upload_response,
)

router = APIRouter(tags=["recordings"])


@router.post("/recordings", response_model=RecordingUploadResponse)
async def upload_recording(
    file: Annotated[UploadFile, File(description="音频文件：wav/mp3/m4a/aac，≤50MB")],
    service: RecordingServiceDep,
    response: Response,
):
    """上传录音：保存文件并创建处理任务后立即返回（异步处理）。"""
    recording, task, created = await service.upload(file)
    response.status_code = status.HTTP_201_CREATED if created else status.HTTP_200_OK
    return to_upload_response(recording, task)


@router.get("/recordings", response_model=RecordingListResponse)
async def list_recordings(
    service: RecordingServiceDep,
    page: Annotated[int, Query(ge=1, description="页码，从 1 开始")] = 1,
    page_size: Annotated[int, Query(ge=1, le=100, description="每页条数，1~100")] = 20,
):
    """录音列表：按创建时间倒序，含每条最新任务状态。"""
    items, total = await service.list_recordings(page=page, page_size=page_size)
    return to_list_response(items, page=page, page_size=page_size, total=total)


@router.get("/recordings/{recording_id}", response_model=RecordingDetailResponse)
async def get_recording(recording_id: UUID, service: RecordingServiceDep):
    """录音详情：处理完成时包含 transcript 与摘要结果。"""
    recording, task = await service.get_recording(recording_id)
    return to_detail_response(recording, task)


@router.delete("/recordings/{recording_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_recording(recording_id: UUID, service: RecordingServiceDep):
    """删除录音（含本地文件与关联任务）。"""
    await service.delete_recording(recording_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
