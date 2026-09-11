from __future__ import annotations

from pathlib import Path
from typing import Optional

from fastapi import UploadFile

from api.config.settings import Settings, get_settings
from api.core.logger import bind_task_trace, get_logger, trace_id_var
from api.domain.exceptions import InvalidRequestError, TaskNotFoundError
from api.repository.dao import Recording, Task
from api.repository.recording import RecordingRepository
from api.utils import file as file_utils
from api.worker import TaskWorker

logger = get_logger(__name__)

_MAX_PAGE_SIZE = 100


def _require_task(task: Optional[Task]) -> Task:
    if task is None:
        raise TaskNotFoundError("录音存在但处理任务缺失")
    return task


def _format_size_mb(size_bytes: int) -> str:
    return f"{size_bytes / (1024 * 1024):.2f}MB"


def _bind_task_for_logs(task: Task) -> None:
    """After task_id is known, correlate remaining request logs with worker via task-*."""
    request_id = trace_id_var.get()
    bind_task_trace(task.id)
    logger.info(
        "绑定任务链路，task_id=%s，request_id=%s",
        task.id,
        request_id,
    )


class RecordingService:
    def __init__(self, repo: RecordingRepository, worker: Optional[TaskWorker] = None):
        self.repo = repo
        self.worker = worker
        self.settings: Settings = get_settings()

    async def upload(self, upload_file: UploadFile) -> tuple[Recording, Task, bool]:
        s = self.settings
        filename, ext, content = await file_utils.validate_upload(
            upload_file,
            allowed_ext=s.allowed_ext_set,
            max_size_bytes=s.max_file_size_bytes,
            max_size_mb=s.MAX_FILE_SIZE_MB,
        )
        fmt = ext.lstrip(".") or "unknown"
        logger.info(
            "收到上传请求，文件=%s，大小=%s，格式=%s",
            filename,
            _format_size_mb(len(content)),
            fmt,
        )

        file_hash = file_utils.compute_sha256(content)
        recording, task = await self.repo.get_by_hash(file_hash)
        if recording is not None:
            task = _require_task(task)
            _bind_task_for_logs(task)
            logger.info(
                "上传命中幂等，recording_id=%s，task_id=%s，状态=%s",
                recording.id,
                task.id,
                task.status,
            )
            return recording, task, False

        saved_path = await file_utils.save_bytes_async(
            content, ext, upload_dir=Path(s.UPLOAD_DIR)
        )
        keep_file = False
        try:
            recording, task, created = await self.repo.create_recording_and_task(
                file_name=filename,
                file_path=saved_path,
                file_size=len(content),
                file_hash=file_hash,
            )
            keep_file = created
        finally:
            if not keep_file:
                await file_utils.remove_file_async(saved_path)

        task = _require_task(task)
        _bind_task_for_logs(task)
        if created:
            logger.info(
                "任务已创建，recording_id=%s，task_id=%s，状态=%s",
                recording.id,
                task.id,
                task.status,
            )
            if self.worker is not None:
                await self.worker.submit(task.id)
        else:
            logger.info(
                "并发下命中已有任务，recording_id=%s，task_id=%s，状态=%s",
                recording.id,
                task.id,
                task.status,
            )
        return recording, task, created

    async def list_recordings(
        self, *, page: int, page_size: int
    ) -> tuple[list[tuple[Recording, Optional[Task]]], int]:
        if page < 1:
            raise InvalidRequestError("page 必须 >= 1")
        if page_size < 1 or page_size > _MAX_PAGE_SIZE:
            raise InvalidRequestError(f"page_size 必须在 1~{_MAX_PAGE_SIZE} 之间")

        items, total = await self.repo.list_page(page=page, page_size=page_size)
        logger.info(
            "查询录音列表，page=%s，page_size=%s，total=%s，返回=%s",
            page,
            page_size,
            total,
            len(items),
        )
        return items, total
