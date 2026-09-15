from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Optional
from uuid import UUID

from fastapi import UploadFile

from api.config.settings import Settings, get_settings
from api.core.logger import bind_task_trace, get_logger, trace_id_var
from api.domain.exceptions import (
    InvalidRequestError,
    RecordingNotFoundError,
    ServiceUnavailableError,
    TaskNotFoundError,
)
from api.repository.dao import Recording, Task
from api.repository.recording import RecordingRepository
from api.service.summarizer import SummarizerService
from api.utils import file as file_utils
from api.worker import TaskWorker

logger = get_logger(__name__)

_MAX_PAGE_SIZE = 100
_REPLAY_CHUNK_SIZE = 16


@dataclass
class SummaryStreamContext:
    """Resolved inputs for one SSE summary stream (validated up-front)."""

    task_id: UUID
    transcript: str
    existing_summary: Optional[dict[str, Any]]


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
    def __init__(
        self,
        repo: RecordingRepository,
        worker: Optional[TaskWorker] = None,
        summarizer: Optional[SummarizerService] = None,
    ):
        self.repo = repo
        self.worker = worker
        self.summarizer = summarizer
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

    async def get_recording(self, recording_id: UUID) -> tuple[Recording, Task]:
        recording, task = await self.repo.get_by_id(recording_id)
        if recording is None:
            raise RecordingNotFoundError(f"录音不存在: {recording_id}")
        task = _require_task(task)
        logger.info(
            "查询录音详情，recording_id=%s，task_id=%s，status=%s",
            recording.id,
            task.id,
            task.status,
        )
        return recording, task

    async def delete_recording(self, recording_id: UUID) -> None:
        file_path = await self.repo.delete(recording_id)
        if file_path is None:
            raise RecordingNotFoundError(f"录音不存在: {recording_id}")
        try:
            await file_utils.remove_file_async(file_path)
        except Exception:
            logger.warning(
                "录音 DB 已删除但磁盘文件清理失败，recording_id=%s，path=%s",
                recording_id,
                file_path,
                exc_info=True,
            )
        else:
            logger.info(
                "录音已删除，recording_id=%s，file_path=%s",
                recording_id,
                file_path,
            )

    async def prepare_summary_stream(self, recording_id: UUID) -> SummaryStreamContext:
        """Validate recording/task; resolve reuse vs. generate.

        Raises before the SSE response starts so the client still gets a real
        status code (404/400/503).
        """
        recording, task = await self.repo.get_by_id(recording_id)
        if recording is None:
            raise RecordingNotFoundError(f"录音不存在: {recording_id}")
        task = _require_task(task)

        # Reuse: replay the stored summary without calling the LLM again.
        # Invariant: summary is only persisted once the task reaches done, so
        # its presence implies a completed pipeline. Revisit if summary ever
        # gets written mid-run.
        existing_summary = task.summary or None
        if existing_summary is not None:
            logger.info(
                "复用已存摘要，recording_id=%s，task_id=%s",
                recording.id,
                task.id,
            )
            return SummaryStreamContext(
                task_id=task.id,
                transcript=task.transcript or "",
                existing_summary=existing_summary,
            )

        # Generate: require a transcript and a usable LLM.
        transcript = (task.transcript or "").strip()
        if not transcript:
            raise InvalidRequestError("尚无转写文本，无法流式摘要")
        if self.summarizer is None or not self.summarizer.can_stream:
            raise ServiceUnavailableError(
                "未配置 LLM_API_KEY，且 WORKER_ALLOW_MOCK_LLM=false，无法流式摘要"
            )
        logger.info(
            "开始流式摘要，recording_id=%s，task_id=%s，transcript_chars=%s",
            recording.id,
            task.id,
            len(transcript),
        )
        return SummaryStreamContext(
            task_id=task.id,
            transcript=transcript,
            existing_summary=None,
        )

    async def stream_summary(
        self, ctx: SummaryStreamContext
    ) -> AsyncIterator[tuple[str, Any]]:
        """Yield SSE ``(event, payload)`` pairs for the recording's summary.

        Reuses the stored summary when present (no LLM call); otherwise streams
        a fresh summary and writes it back so later reads see the same result.
        """
        if ctx.existing_summary is not None:
            async for event in self._replay_stored_summary(ctx.existing_summary):
                yield event
            return

        # Defensive; prepare_summary_stream already guarantees a summarizer here.
        if self.summarizer is None:
            yield ("error", "LLM is not configured")
            return

        persisted = False
        async for name, payload in self.summarizer.summarize_stream(ctx.transcript):
            if name == "done" and not persisted and isinstance(payload, dict):
                persisted = True
                await self._persist_streamed_summary(ctx.task_id, payload)
            yield name, payload

    async def _replay_stored_summary(
        self, summary: dict[str, Any]
    ) -> AsyncIterator[tuple[str, Any]]:
        """Re-emit a stored summary as delta chunks followed by ``done``."""
        raw = json.dumps(summary, ensure_ascii=False)
        for i in range(0, len(raw), _REPLAY_CHUNK_SIZE):
            yield ("delta", raw[i : i + _REPLAY_CHUNK_SIZE])
        yield ("done", summary)

    async def _persist_streamed_summary(
        self, task_id: UUID, summary: dict[str, Any]
    ) -> None:
        """Best-effort writeback of an SSE-generated summary (never overwrites)."""
        try:
            saved = await self.repo.set_task_summary_if_absent(
                task_id, summary=summary
            )
        except Exception:
            logger.warning(
                "[task=%s] SSE 摘要回写失败（不影响流式响应）", task_id, exc_info=True
            )
            return
        if saved:
            logger.info("[task=%s] SSE 摘要已回写数据库", task_id)
        else:
            logger.info("[task=%s] SSE 摘要未回写（已存在摘要）", task_id)
