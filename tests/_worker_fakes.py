"""Worker 测试共用的内存替身与构造辅助。

单独抽出来，避免多个测试模块各自复刻一份 ``TaskDao`` 语义而彼此漂移；
真实的 SQL 语义由 ``tests/integration`` 里的 Postgres 集成测试负责。
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from api.repository.dao.recording import Recording
from api.repository.dao.task import Task
from api.repository.dao.task_types import TaskStatus
from api.service.transcriber import TranscriberService
from api.worker.executor import TaskExecutor

WORKER_ID = "worker-test"


class _SessionCM:
    async def __aenter__(self):
        return MagicMock()

    async def __aexit__(self, *args):
        return False


def session_factory():
    return _SessionCM()


class FakeTaskDao:
    """``TaskDao`` 的内存替身：用 Python 复刻锁定 / 租约 / 重试语义。

    ``lease_ok_sequence`` 按调用顺序返回续约结果（末项重复），用于模拟处理过程中
    租约丢失；默认行为与旧实现一致。
    """

    def __init__(
        self,
        task: Optional[Task],
        *,
        lease_ok_sequence: Optional[list[bool]] = None,
    ) -> None:
        self.task = task
        self.transitions: list[str] = []
        self.failure_calls = 0
        self.lease_ok = True
        self.renew_calls = 0
        self.lease_ok_sequence = lease_ok_sequence

    async def get_by_id(self, task_id):
        if self.task is None or self.task.id != task_id:
            return None
        return self.task

    async def renew_lease(self, task_id, *, worker_id: str, lease_seconds: int) -> bool:
        self.renew_calls += 1
        if self.lease_ok_sequence is not None:
            idx = min(self.renew_calls - 1, len(self.lease_ok_sequence) - 1)
            return self.lease_ok_sequence[idx]
        return bool(
            self.lease_ok
            and self.task is not None
            and self.task.id == task_id
            and self.task.locked_by == worker_id
        )

    async def mark_summarizing(
        self, task_id, *, worker_id: str, transcript: str
    ) -> bool:
        if (
            self.task is None
            or self.task.id != task_id
            or self.task.locked_by != worker_id
        ):
            return False
        self.task.status = TaskStatus.SUMMARIZING.value
        self.task.transcript = transcript
        self.transitions.append(TaskStatus.SUMMARIZING.value)
        return True

    async def mark_done(
        self, task_id, *, worker_id: str, summary: dict[str, Any]
    ) -> bool:
        if (
            self.task is None
            or self.task.id != task_id
            or self.task.locked_by != worker_id
        ):
            return False
        self.task.status = TaskStatus.DONE.value
        self.task.summary = summary
        self.task.locked_by = None
        self.task.error_msg = None
        self.transitions.append(TaskStatus.DONE.value)
        return True

    async def mark_failure(
        self,
        task_id,
        *,
        worker_id: str,
        error_msg: str,
        max_retries: int,
    ) -> Optional[tuple[int, str]]:
        if (
            self.task is None
            or self.task.id != task_id
            or self.task.locked_by != worker_id
        ):
            return None
        self.failure_calls += 1
        new_count = self.task.retry_count + 1
        self.task.retry_count = new_count
        self.task.error_msg = error_msg
        self.task.locked_by = None
        if new_count <= max_retries:
            self.task.status = TaskStatus.PENDING.value
        else:
            self.task.status = TaskStatus.FAILED.value
        self.transitions.append(self.task.status)
        return new_count, self.task.status

    async def reset_aborted(self, task_ids, *, worker_id: str, error_msg: str) -> int:
        if self.task is None or self.task.id not in task_ids:
            return 0
        if self.task.locked_by != worker_id:
            return 0
        self.task.status = TaskStatus.PENDING.value
        self.task.error_msg = error_msg
        self.task.locked_by = None
        return 1


class SlowTranscriber:
    """可控耗时的转写替身，记录是否被取消（用于租约丢失 / 优雅关闭路径）。"""

    def __init__(self, delay: float = 5.0, *, on_start=None) -> None:
        self.delay = delay
        self.on_start = on_start
        self.started = 0
        self.cancelled = False
        self.finished = False

    async def transcribe(self, *, file_path: str, task_id=None) -> str:
        self.started += 1
        if self.on_start is not None:
            self.on_start()
        try:
            await asyncio.sleep(self.delay)
        except BaseException:
            self.cancelled = True
            raise
        self.finished = True
        return "转写文本"


def make_recording() -> Recording:
    return Recording(
        id=uuid4(),
        file_name="demo.wav",
        file_path="/tmp/demo.wav",
        file_size=12,
        file_hash="a" * 64,
    )


def inflight_task(rec: Recording, *, retry_count: int = 0) -> Task:
    return Task(
        id=uuid4(),
        recording_id=rec.id,
        status=TaskStatus.TRANSCRIBING.value,
        locked_by=WORKER_ID,
        retry_count=retry_count,
    )


def instant_asr(*, fail: bool = False) -> TranscriberService:
    return TranscriberService(
        min_delay_seconds=0,
        max_delay_seconds=0,
        fail_rate=1.0 if fail else 0.0,
    )


def make_executor(
    *,
    dao: FakeTaskDao,
    transcriber,
    summarizer=None,
    allow_mock_llm: bool = False,
    max_retries: int = 3,
    lease_renew_interval: float = 3600,
):
    """构造 executor 并返回 (executor, 替换 task_tx 的上下文管理器)。"""

    @asynccontextmanager
    async def _tx(_factory):
        yield dao

    executor = TaskExecutor(
        db_session_factory=session_factory,
        worker_id=WORKER_ID,
        lease_seconds=300,
        max_retries=max_retries,
        lease_renew_interval=lease_renew_interval,
        transcriber=transcriber,
        summarizer=summarizer,
        allow_mock_llm=allow_mock_llm,
    )
    return executor, _tx


def patch_recording(rec: Recording):
    rec_dao = MagicMock()
    rec_dao.get_by_id = AsyncMock(return_value=rec)
    return patch("api.worker.executor.RecordingDao", return_value=rec_dao)
