"""录音路由：上传 / 列表 / 详情 / 删除 / 摘要 SSE。"""
from __future__ import annotations

import json
from typing import Annotated, Any, AsyncIterator
from uuid import UUID

from fastapi import APIRouter, File, Query, Request, Response, UploadFile, status
from fastapi.responses import StreamingResponse

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


def _sse_pack(event: str, data: Any) -> str:
    payload = json.dumps(data, ensure_ascii=False)
    return f"event: {event}\ndata: {payload}\n\n"


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


@router.get("/recordings/{recording_id}/summary/stream")
async def stream_recording_summary(
    recording_id: UUID,
    request: Request,
    service: RecordingServiceDep,
):
    """以 SSE 流式返回摘要生成过程（不写库）。"""
    # Raise 404/400/503 before headers are flushed.
    transcript = await service.prepare_summary_stream(recording_id)
    summarizer = service.summarizer
    assert summarizer is not None

    async def event_source() -> AsyncIterator[str]:
        async for name, payload in summarizer.summarize_stream(transcript):
            if await request.is_disconnected():
                break
            if name == "delta":
                yield _sse_pack("delta", {"text": payload})
            elif name == "done":
                yield _sse_pack("done", payload)
            elif name == "error":
                yield _sse_pack("error", {"message": str(payload)})
            else:
                yield _sse_pack(name, payload)

    return StreamingResponse(
        event_source(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


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
