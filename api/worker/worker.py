"""TaskWorker facade: wires dispatcher + executor lifecycle."""
from __future__ import annotations

import asyncio
from typing import Optional
from uuid import UUID

from api.config.settings import Settings, get_settings
from api.core.db.db_connector import AsyncSessionFactory
from api.core.logger import get_logger, set_worker_id
from api.service.summarizer import SummarizerService
from api.service.transcriber import TranscriberService
from api.worker.dispatcher import TaskDispatcher
from api.worker.executor import TaskExecutor
from api.worker.identity import resolve_worker_id

logger = get_logger(__name__)


class TaskWorker:
    """Compose TaskDispatcher + TaskExecutor; keep start/stop/submit for callers."""

    def __init__(
        self,
        db_session_factory=None,
        *,
        summarizer: Optional[SummarizerService] = None,
        transcriber: Optional[TranscriberService] = None,
        settings: Optional[Settings] = None,
        max_concurrency: Optional[int] = None,
        max_batch_size: Optional[int] = None,
        lease_seconds: Optional[int] = None,
        max_retries: Optional[int] = None,
        poll_idle_seconds: Optional[float] = None,
        allow_mock_llm: Optional[bool] = None,
        worker_id: Optional[str] = None,
    ):
        s = settings or get_settings()
        self.db_session_factory = db_session_factory or AsyncSessionFactory
        self.summarizer = summarizer
        self.transcriber = transcriber or TranscriberService()
        self.max_concurrency = (
            max_concurrency if max_concurrency is not None else s.WORKER_MAX_CONCURRENCY
        )
        self.max_batch_size = (
            max_batch_size if max_batch_size is not None else s.WORKER_MAX_BATCH_SIZE
        )
        self.lease_seconds = (
            lease_seconds if lease_seconds is not None else s.WORKER_LEASE_SECONDS
        )
        self.max_retries = (
            max_retries if max_retries is not None else s.WORKER_MAX_RETRIES
        )
        self.poll_idle_seconds = (
            poll_idle_seconds
            if poll_idle_seconds is not None
            else s.WORKER_POLL_IDLE_SECONDS
        )
        self.allow_mock_llm = (
            allow_mock_llm if allow_mock_llm is not None else s.WORKER_ALLOW_MOCK_LLM
        )
        self.worker_id = worker_id or resolve_worker_id(s)
        lease_renew_interval = max(5.0, self.lease_seconds / 3.0)
        reclaim_interval = max(30.0, self.lease_seconds / 2.0)

        self.stop_event = asyncio.Event()
        self._wake_event = asyncio.Event()
        self._started = False

        self.executor = TaskExecutor(
            db_session_factory=self.db_session_factory,
            worker_id=self.worker_id,
            lease_seconds=self.lease_seconds,
            max_retries=self.max_retries,
            lease_renew_interval=lease_renew_interval,
            transcriber=self.transcriber,
            summarizer=self.summarizer,
            allow_mock_llm=self.allow_mock_llm,
        )
        self.dispatcher = TaskDispatcher(
            db_session_factory=self.db_session_factory,
            executor=self.executor,
            worker_id=self.worker_id,
            lease_seconds=self.lease_seconds,
            max_concurrency=self.max_concurrency,
            max_batch_size=self.max_batch_size,
            poll_idle_seconds=self.poll_idle_seconds,
            reclaim_interval=reclaim_interval,
            stop_event=self.stop_event,
            wake_event=self._wake_event,
        )

    async def start(self) -> None:
        set_worker_id(self.worker_id)
        await self.dispatcher.start()
        self._started = True
        logger.info(
            "TaskWorker 已启动",
            event="worker.started",
            max_concurrency=self.max_concurrency,
            lease_seconds=self.lease_seconds,
            reclaim_interval=self.dispatcher.reclaim_interval,
            allow_mock_llm=self.allow_mock_llm,
        )

    async def stop(self, timeout_seconds: int = 30) -> None:
        self.stop_event.set()
        self._wake_event.set()
        self._started = False

        await self.dispatcher.stop_loop(timeout_seconds=5)

        running = self.dispatcher.running_tasks
        if not running:
            logger.info("TaskWorker 已优雅退出（无在途任务）", event="worker.stopped")
            if self.summarizer is not None:
                await self.summarizer.aclose()
            return

        logger.info(
            "正在等待在途任务执行完毕", event="worker.stopping", in_flight=len(running)
        )
        _done, pending = await asyncio.wait(
            running.values(), timeout=timeout_seconds
        )

        if pending:
            pending_ids = [tid for tid, t in running.items() if t in pending]
            logger.warning(
                "在途任务超时未完成，强制取消并重置 DB",
                event="worker.shutdown_timeout",
                pending=len(pending),
                task_ids=pending_ids,
            )
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            await self.executor.reset_aborted(pending_ids)

        if self.summarizer is not None:
            await self.summarizer.aclose()
        logger.info("TaskWorker 优雅关闭完成", event="worker.stopped")

    async def submit(self, task_id: UUID) -> None:
        """上传侧通知；唤醒派发器尽快拾取 pending。"""
        if not self._started:
            raise RuntimeError("TaskWorker 尚未启动")
        logger.info(
            "任务已提交，等待 worker 拾取", event="task.submitted", task_id=str(task_id)
        )
        self.dispatcher.wake()

    async def enqueue(self, task_id: UUID) -> None:
        await self.submit(task_id)
