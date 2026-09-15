from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Optional
from uuid import UUID

from fastapi import UploadFile

from api.config.settings import Settings, get_settings
from api.core.logger import get_logger, log_context
from api.domain.exceptions import (
    InvalidRequestError,
    RecordingNotFoundError,
    ServiceUnavailableError,
    TaskNotFoundError,
)
from api.repository.dao import Recording, Task
from api.repository.dao.task_types import TaskStatus
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
        filename = upload_file.filename or ""
        ext = file_utils.validate_extension(filename, allowed_ext=s.allowed_ext_set)
        fmt = ext.lstrip(".") or "unknown"
        logger.info(
            "收到上传请求", event="upload.received", file_name=filename, file_format=fmt
        )

        staged = await file_utils.stage_upload(
            upload_file,
            ext=ext,
            upload_dir=Path(s.UPLOAD_DIR),
            max_size_bytes=s.max_file_size_bytes,
            max_size_mb=s.MAX_FILE_SIZE_MB,
        )
        logger.info(
            "上传已落盘",
            event="upload.staged",
            file_name=filename,
            file_size=staged.size,
            file_hash=staged.sha256,
        )

        # 先查重：命中则丢弃暂存件，不产生垃圾文件（幂等）。
        existing_recording, existing_task = await self.repo.get_by_hash(staged.sha256)
        if existing_recording is not None:
            await asyncio.to_thread(staged.discard)
            existing_task = _require_task(existing_task)
            with log_context(
                recording_id=existing_recording.id, task_id=existing_task.id
            ):
                logger.info(
                    "上传命中幂等",
                    event="upload.idempotent_hit",
                    status=existing_task.status,
                )
            return existing_recording, existing_task, False

        # 未命中：原子提交到最终路径，再落库；落库失败则回滚文件。
        try:
            saved_path = await asyncio.to_thread(staged.commit)
        except BaseException:  # 含 CancelledError：请求中断也必须清掉 .part
            await asyncio.to_thread(staged.discard)
            raise

        keep_file = False
        try:
            recording, task, created = await self.repo.create_recording_and_task(
                file_name=filename,
                file_path=saved_path,
                file_size=staged.size,
                file_hash=staged.sha256,
            )
            keep_file = created
        finally:
            # created=False = 并发同哈希竞态中落败（repo 未插入新记录），
            # 本请求提交的文件即孤儿，须删除；落库异常同理回滚。
            # 依赖 repo 契约：created=False 时不得重复插入记录。
            if not keep_file:
                await file_utils.remove_file_async(saved_path)

        task = _require_task(task)
        with log_context(recording_id=recording.id, task_id=task.id):
            if created:
                logger.info(
                    "任务已创建",
                    event="task.created",
                    status=task.status,
                    file_name=filename,
                    file_size=staged.size,
                    file_hash=staged.sha256,
                )
                if self.worker is not None:
                    await self.worker.submit(task.id)
            else:
                logger.info(
                    "并发下命中已有任务",
                    event="task.created.race",
                    status=task.status,
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
            "查询录音列表",
            event="recording.list",
            page=page,
            page_size=page_size,
            total=total,
            returned=len(items),
        )
        return items, total

    async def get_recording(self, recording_id: UUID) -> tuple[Recording, Task]:
        recording, task = await self.repo.get_by_id(recording_id)
        if recording is None:
            raise RecordingNotFoundError(f"录音不存在: {recording_id}")
        task = _require_task(task)
        with log_context(recording_id=recording.id, task_id=task.id):
            logger.info("查询录音详情", event="recording.fetched", status=task.status)
        return recording, task

    async def delete_recording(self, recording_id: UUID) -> None:
        with log_context(recording_id=recording_id):
            file_path = await self.repo.delete(recording_id)
            if file_path is None:
                raise RecordingNotFoundError(f"录音不存在: {recording_id}")
            try:
                await file_utils.remove_file_async(file_path)
            except Exception:
                logger.opt(exception=True).warning(
                    "录音 DB 已删除但磁盘文件清理失败",
                    event="recording.delete.file_cleanup_failed",
                    path=file_path,
                )
            else:
                logger.info("录音已删除", event="recording.deleted", path=file_path)

    async def prepare_summary_stream(self, recording_id: UUID) -> SummaryStreamContext:
        """Validate recording/task; resolve reuse vs. generate.

        Raises before the SSE response starts so the client still gets a real
        status code (404/400/503).
        """
        with log_context(recording_id=recording_id):
            recording, task = await self.repo.get_by_id(recording_id)
            if recording is None:
                raise RecordingNotFoundError(f"录音不存在: {recording_id}")
            task = _require_task(task)

            # 复用：回放已存摘要，不再调用 LLM。
            # 不变量：``tasks.summary`` 只由 ``mark_done`` 写入（``requeue_failed``
            # 清空），因此「有摘要」等价于「流水线已完成」——与详情接口暴露结果的
            # 条件一致。这里显式再校验 done，使两个接口在任何写入路径下都不会出现
            # 「SSE 给了摘要、详情却说没有」的分歧。
            existing_summary = task.summary or None
            if existing_summary is not None and task.status == TaskStatus.DONE.value:
                logger.info(
                    "复用已存摘要", event="summary.reuse", task_id=str(task.id)
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
                "开始流式摘要",
                event="summary.stream.start",
                task_id=str(task.id),
                transcript_chars=len(transcript),
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

        Reuses the stored summary when the task completed (no LLM call); otherwise
        streams a freshly generated summary **without persisting it** —
        ``tasks.summary`` stays owned by the pipeline, so a task that has not
        reached ``done`` never holds a result the detail endpoint would hide.
        """
        if ctx.existing_summary is not None:
            async for event in self._replay_stored_summary(ctx.existing_summary):
                yield event
            return

        # Defensive; prepare_summary_stream already guarantees a summarizer here.
        if self.summarizer is None:
            yield ("error", "LLM is not configured")
            return

        async for event in self.summarizer.summarize_stream(ctx.transcript):
            yield event

    async def _replay_stored_summary(
        self, summary: dict[str, Any]
    ) -> AsyncIterator[tuple[str, Any]]:
        """Re-emit a stored summary as delta chunks followed by ``done``."""
        raw = json.dumps(summary, ensure_ascii=False)
        for i in range(0, len(raw), _REPLAY_CHUNK_SIZE):
            yield ("delta", raw[i : i + _REPLAY_CHUNK_SIZE])
        yield ("done", summary)
