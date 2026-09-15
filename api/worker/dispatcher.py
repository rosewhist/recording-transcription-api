"""Task dispatcher: reclaim/claim pending work and schedule executors."""
from __future__ import annotations

import asyncio
import time
from typing import Optional
from uuid import UUID

from api.core.logger import bind_task_id, get_logger, task_id_var
from api.repository.dao.task_types import TaskStatus
from api.worker.common import task_tx
from api.worker.executor import TaskExecutor

logger = get_logger(__name__)


class TaskDispatcher:
    def __init__(
        self,
        *,
        db_session_factory,
        executor: TaskExecutor,
        worker_id: str,
        lease_seconds: int,
        max_concurrency: int,
        max_batch_size: int,
        poll_idle_seconds: float,
        reclaim_interval: float,
        stop_event: asyncio.Event,
        wake_event: asyncio.Event,
    ) -> None:
        self.db_session_factory = db_session_factory
        self.executor = executor
        self.worker_id = worker_id
        self.lease_seconds = lease_seconds
        self.max_concurrency = max_concurrency
        self.max_batch_size = max_batch_size
        self.poll_idle_seconds = poll_idle_seconds
        self.reclaim_interval = reclaim_interval
        self._last_reclaim_at = 0.0

        self.stop_event = stop_event
        self._wake_event = wake_event
        self.running_tasks: dict[str, asyncio.Task] = {}
        self._loop_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        await self.recover_zombie_tasks(include_own_locks=True)
        self._last_reclaim_at = time.monotonic()
        self.stop_event.clear()
        self._wake_event.clear()
        self._loop_task = asyncio.create_task(
            self._dispatcher_loop(), name="task-worker-dispatcher"
        )

    async def stop_loop(self, timeout_seconds: float = 5.0) -> None:
        if self._loop_task is None:
            return
        try:
            await asyncio.wait_for(self._loop_task, timeout=timeout_seconds)
        except asyncio.TimeoutError:
            self._loop_task.cancel()
            await asyncio.gather(self._loop_task, return_exceptions=True)
        self._loop_task = None

    def wake(self) -> None:
        self._wake_event.set()

    async def recover_zombie_tasks(self, *, include_own_locks: bool = True) -> None:
        if include_own_locks:
            reason = "服务重启/租约过期，任务被中断，准备重试"
            event = "worker.recover.zombie"
            message = "服务重启/租约过期，重置僵尸任务为 pending"
        else:
            reason = "租约过期，任务被中断，准备重试"
            event = "worker.recover.expired_lease"
            message = "周期回收：重置过期租约任务为 pending"

        async with task_tx(self.db_session_factory) as dao:
            n = await dao.reclaim_in_flight(
                worker_id=self.worker_id,
                include_own_locks=include_own_locks,
                error_msg=reason,
            )
        if n:
            logger.warning(message, event=event, count=n)

    async def _dispatcher_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                inflight = len(self.running_tasks)
                if inflight >= self.max_concurrency:
                    await self._maybe_reclaim_expired_leases()
                    await self._interruptible_sleep(0.5)
                    continue

                batch = min(self.max_concurrency - inflight, self.max_batch_size)
                task_ids = await self._reclaim_and_claim(batch)

                if task_ids:
                    logger.info(
                        "派发器拉取到任务，开始调度",
                        event="dispatcher.claimed",
                        count=len(task_ids),
                        task_ids=[str(tid) for tid in task_ids],
                    )
                    for task_id in task_ids:
                        self._schedule_task(task_id)
                    continue

                await self._wait_for_work(self.poll_idle_seconds)

            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("派发器异常", event="dispatcher.error")
                await self._interruptible_sleep(5.0)

    def _schedule_task(self, task_id: UUID) -> None:
        """Bind task-* trace into the child task context, then spawn executor."""
        token = bind_task_id(task_id)
        try:
            logger.info(
                "任务状态变更",
                event="task.status_changed",
                status_from=TaskStatus.PENDING.value,
                status_to=TaskStatus.TRANSCRIBING.value,
            )
            key = str(task_id)
            t = asyncio.create_task(
                self.executor.process(task_id),
                name=f"task-{key}",
            )
            self.running_tasks[key] = t
            t.add_done_callback(
                lambda _fut, tid=key: self.running_tasks.pop(tid, None)
            )
        finally:
            task_id_var.reset(token)

    async def _interruptible_sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self.stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def _wait_for_work(self, seconds: float) -> None:
        self._wake_event.clear()
        stop_waiter = asyncio.create_task(self.stop_event.wait())
        wake_waiter = asyncio.create_task(self._wake_event.wait())
        try:
            _done, pending = await asyncio.wait(
                {stop_waiter, wake_waiter},
                timeout=seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for t in pending:
                t.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        finally:
            if not stop_waiter.done():
                stop_waiter.cancel()
            if not wake_waiter.done():
                wake_waiter.cancel()

    def _reclaim_due(self) -> bool:
        return time.monotonic() - self._last_reclaim_at >= self.reclaim_interval

    async def _reclaim_and_claim(self, limit: int) -> list[UUID]:
        do_reclaim = self._reclaim_due()
        async with task_tx(self.db_session_factory) as dao:
            if do_reclaim:
                self._last_reclaim_at = time.monotonic()
                n = await dao.reclaim_in_flight(
                    worker_id=self.worker_id,
                    include_own_locks=False,
                    error_msg="租约过期，任务被中断，准备重试",
                )
                if n:
                    logger.warning(
                        "周期回收：重置过期租约任务为 pending",
                        event="worker.recover.expired_lease",
                        count=n,
                    )
            return await dao.claim_pending(
                limit=limit,
                worker_id=self.worker_id,
                lease_seconds=self.lease_seconds,
            )

    async def _maybe_reclaim_expired_leases(self) -> None:
        if not self._reclaim_due():
            return
        self._last_reclaim_at = time.monotonic()
        await self.recover_zombie_tasks(include_own_locks=False)
