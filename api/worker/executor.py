"""Single-task executor: lease, ASR, summarize, status transitions."""
from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional, Sequence
from uuid import UUID

from api.core.logger import bind_task_trace, get_logger, trace_id_var
from api.repository.dao.recording import RecordingDao
from api.repository.dao.task_types import TaskStatus
from api.service.summarizer import SummarizerService
from api.service.transcriber import TranscriberService
from api.worker.common import LeaseLostError, as_uuid, task_tx

logger = get_logger(__name__)


class TaskExecutor:
    def __init__(
        self,
        *,
        db_session_factory,
        worker_id: str,
        lease_seconds: int,
        max_retries: int,
        lease_renew_interval: float,
        transcriber: TranscriberService,
        summarizer: Optional[SummarizerService] = None,
        allow_mock_llm: bool = False,
    ) -> None:
        self.db_session_factory = db_session_factory
        self.worker_id = worker_id
        self.lease_seconds = lease_seconds
        self.max_retries = max_retries
        self._lease_renew_interval = lease_renew_interval
        self.transcriber = transcriber
        self.summarizer = summarizer
        self.allow_mock_llm = allow_mock_llm

    async def process(self, task_id: UUID) -> None:
        # Prefer inherited task-* from dispatcher; re-bind for safety.
        token = bind_task_trace(task_id)
        t0 = time.perf_counter()
        try:
            async with task_tx(self.db_session_factory) as dao:
                task = await dao.get_by_id(task_id)
                if task is None:
                    logger.warning("[task=%s] 不存在，跳过", task_id)
                    return
                if (
                    task.status != TaskStatus.TRANSCRIBING.value
                    or task.locked_by != self.worker_id
                ):
                    logger.warning(
                        "[task=%s] 状态/租约不符 (status=%s, locked_by=%s)，跳过",
                        task_id,
                        task.status,
                        task.locked_by,
                    )
                    return
                recording_id = task.recording_id

            async with self._lease_keepalive(task_id) as lease_lost:
                transcript = await self._await_while_leased(
                    self._do_asr(task_id, recording_id),
                    lease_lost,
                )

                async with task_tx(self.db_session_factory) as dao:
                    ok = await dao.mark_summarizing(
                        task_id,
                        worker_id=self.worker_id,
                        transcript=transcript,
                    )
                    if not ok:
                        raise LeaseLostError(
                            f"Task {task_id} 租约丢失，无法进入 summarizing"
                        )
                logger.info(
                    "[task=%s] 状态变更: %s -> %s",
                    task_id,
                    TaskStatus.TRANSCRIBING.value,
                    TaskStatus.SUMMARIZING.value,
                )

                summary = await self._await_while_leased(
                    self._summarize(task_id, transcript),
                    lease_lost,
                )

                async with task_tx(self.db_session_factory) as dao:
                    ok = await dao.mark_done(
                        task_id,
                        worker_id=self.worker_id,
                        summary=summary,
                    )
                    if not ok:
                        raise LeaseLostError(
                            f"Task {task_id} 租约丢失，无法标记 done"
                        )

                elapsed = time.perf_counter() - t0
                logger.info(
                    "[task=%s] 状态变更: %s -> %s",
                    task_id,
                    TaskStatus.SUMMARIZING.value,
                    TaskStatus.DONE.value,
                )
                logger.info(
                    "[task=%s] 任务执行成功，总耗时: %.1fs，状态: %s",
                    task_id,
                    elapsed,
                    TaskStatus.DONE.value,
                )

        except asyncio.CancelledError:
            logger.info("[task=%s] 被取消", task_id)
            raise
        except LeaseLostError:
            logger.warning(
                "[task=%s] 租约丢失，中止本机处理（由其它实例或回收逻辑接管）",
                task_id,
            )
        except Exception as e:
            logger.exception("[task=%s] 处理失败", task_id)
            await self._handle_task_failure(task_id, str(e))
        finally:
            trace_id_var.reset(token)

    async def reset_aborted(self, task_ids: Sequence[str]) -> None:
        if not task_ids:
            return
        ids = [as_uuid(tid) for tid in task_ids]
        async with task_tx(self.db_session_factory) as dao:
            n = await dao.reset_aborted(
                ids,
                worker_id=self.worker_id,
                error_msg="服务优雅关闭超时，任务被中断，准备重试",
            )
        if n:
            logger.warning(
                "优雅关闭超时，重置 %d 个被取消任务回 pending",
                n,
            )

    @asynccontextmanager
    async def _lease_keepalive(self, task_id: UUID) -> AsyncIterator[asyncio.Event]:
        lease_lost = asyncio.Event()
        if not await self._renew_lease(task_id):
            logger.warning(
                "[task=%s] 首次续约失败（租约可能已丢失）", task_id
            )
            lease_lost.set()
            yield lease_lost
            return

        renew_task = asyncio.create_task(
            self._lease_renew_loop(task_id, lease_lost),
            name=f"lease-{task_id}",
        )
        try:
            yield lease_lost
        finally:
            renew_task.cancel()
            await asyncio.gather(renew_task, return_exceptions=True)

    async def _lease_renew_loop(
        self, task_id: UUID, lease_lost: asyncio.Event
    ) -> None:
        try:
            while True:
                await asyncio.sleep(self._lease_renew_interval)
                ok = await self._renew_lease(task_id)
                if not ok:
                    logger.warning(
                        "[task=%s] 续约失败（租约可能已丢失）", task_id
                    )
                    lease_lost.set()
                    return
        except asyncio.CancelledError:
            raise

    async def _await_while_leased(self, coro, lease_lost: asyncio.Event):
        if lease_lost.is_set():
            raise LeaseLostError("lease already lost")

        work = asyncio.create_task(coro)
        watcher = asyncio.create_task(lease_lost.wait())
        try:
            await asyncio.wait(
                {work, watcher},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if lease_lost.is_set():
                if not work.done():
                    work.cancel()
                await asyncio.gather(work, return_exceptions=True)
                raise LeaseLostError("lease lost during processing")
            return work.result()
        finally:
            if not watcher.done():
                watcher.cancel()
                await asyncio.gather(watcher, return_exceptions=True)

    async def _handle_task_failure(self, task_id: UUID, error_msg: str) -> None:
        async with task_tx(self.db_session_factory) as dao:
            row = await dao.mark_failure(
                task_id,
                worker_id=self.worker_id,
                error_msg=error_msg,
                max_retries=self.max_retries,
            )

        if row is None:
            logger.warning(
                "[task=%s] 失败处理未生效（可能已非本 worker 持有）", task_id
            )
            return

        new_count, new_status = row
        max_attempts = self.max_retries + 1
        if new_status == TaskStatus.PENDING.value:
            delay = 2 ** new_count
            logger.warning(
                "[task=%s] 状态变更: in_flight -> %s；第 %d/%d 次失败，"
                "将在 %ds 后重新入队，错误: %s",
                task_id,
                TaskStatus.PENDING.value,
                new_count,
                max_attempts,
                delay,
                (error_msg or "")[:200],
            )
        else:
            logger.error(
                "[task=%s] 状态变更: in_flight -> %s；已失败 %d/%d 次，"
                "重试耗尽，错误: %s",
                task_id,
                TaskStatus.FAILED.value,
                new_count,
                max_attempts,
                (error_msg or "")[:500],
            )

    async def _renew_lease(self, task_id: UUID) -> bool:
        async with task_tx(self.db_session_factory) as dao:
            return await dao.renew_lease(
                task_id,
                worker_id=self.worker_id,
                lease_seconds=self.lease_seconds,
            )

    async def _do_asr(self, task_id: UUID, recording_id: UUID) -> str:
        async with self.db_session_factory() as session:
            recording = await RecordingDao(session).get_by_id(recording_id)
            if recording is None:
                raise RuntimeError(f"Task {task_id} 关联录音 {recording_id} 不存在")
            file_path = recording.file_path

        return await self.transcriber.transcribe(
            file_path=file_path,
            task_id=task_id,
        )

    async def _summarize(self, task_id: UUID, transcript: str) -> dict[str, Any]:
        t0 = time.perf_counter()
        if self.summarizer is not None:
            result = await self.summarizer.summarize(transcript)
            logger.info(
                "[task=%s] LLM 摘要生成成功，耗时: %.1fs",
                task_id,
                time.perf_counter() - t0,
            )
            return result
        if self.allow_mock_llm:
            logger.warning("WORKER_ALLOW_MOCK_LLM=true，使用占位摘要 [task=%s]", task_id)
            await asyncio.sleep(0.1)
            result = {
                "summary": "模拟摘要",
                "key_points": ["要点1"],
                "todos": ["待办1"],
            }
            logger.info(
                "[task=%s] LLM 占位摘要生成成功，耗时: %.1fs",
                task_id,
                time.perf_counter() - t0,
            )
            return result
        raise RuntimeError(
            "未配置 LLM_API_KEY，且 WORKER_ALLOW_MOCK_LLM=false，无法生成摘要"
        )
