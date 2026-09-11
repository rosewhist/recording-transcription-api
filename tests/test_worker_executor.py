"""Worker 执行器状态机：pending 抢占后 transcribing → summarizing → done / failed。"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from api.repository.dao.recording import Recording
from api.repository.dao.task import Task
from api.repository.dao.task_types import TaskStatus
from api.service.transcriber import TranscriberService
from api.worker.executor import TaskExecutor

WORKER_ID = "worker-test"
_SUMMARY = {"summary": "一句话", "key_points": ["k1"], "todos": ["t1"]}


class _SessionCM:
    async def __aenter__(self):
        return MagicMock()

    async def __aexit__(self, *args):
        return False


def _session_factory():
    return _SessionCM()


class FakeTaskDao:
    def __init__(self, task: Task):
        self.task = task
        self.transitions: list[str] = []
        self.failure_calls = 0
        self.lease_ok = True

    async def get_by_id(self, task_id):
        if self.task is None or self.task.id != task_id:
            return None
        return self.task

    async def renew_lease(self, task_id, *, worker_id: str, lease_seconds: int) -> bool:
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


def _recording() -> Recording:
    return Recording(
        id=uuid4(),
        file_name="demo.wav",
        file_path="/tmp/demo.wav",
        file_size=12,
        file_hash="a" * 64,
    )


def _inflight_task(recording: Recording, *, retry_count: int = 0) -> Task:
    return Task(
        id=uuid4(),
        recording_id=recording.id,
        status=TaskStatus.TRANSCRIBING.value,
        locked_by=WORKER_ID,
        retry_count=retry_count,
    )


def _instant_asr(*, fail: bool = False) -> TranscriberService:
    return TranscriberService(
        min_delay_seconds=0,
        max_delay_seconds=0,
        fail_rate=1.0 if fail else 0.0,
    )


def _executor(
    *,
    dao: FakeTaskDao,
    transcriber: TranscriberService,
    summarizer=None,
    allow_mock_llm: bool = False,
    max_retries: int = 3,
) -> TaskExecutor:
    @asynccontextmanager
    async def _tx(_factory):
        yield dao

    executor = TaskExecutor(
        db_session_factory=_session_factory,
        worker_id=WORKER_ID,
        lease_seconds=300,
        max_retries=max_retries,
        lease_renew_interval=3600,
        transcriber=transcriber,
        summarizer=summarizer,
        allow_mock_llm=allow_mock_llm,
    )
    return executor, _tx


def _patch_recording(recording: Recording):
    rec_dao = MagicMock()
    rec_dao.get_by_id = AsyncMock(return_value=recording)
    return patch("api.worker.executor.RecordingDao", return_value=rec_dao)


@pytest.mark.asyncio
async def test_process_reaches_done_via_transcribing_and_summarizing():
    recording = _recording()
    task = _inflight_task(recording)
    dao = FakeTaskDao(task)
    summarizer = MagicMock()
    summarizer.summarize = AsyncMock(return_value=_SUMMARY)
    executor, tx = _executor(
        dao=dao,
        transcriber=_instant_asr(),
        summarizer=summarizer,
    )

    with patch("api.worker.executor.task_tx", tx), _patch_recording(recording):
        await executor.process(task.id)

    assert dao.transitions == [
        TaskStatus.SUMMARIZING.value,
        TaskStatus.DONE.value,
    ]
    assert task.status == TaskStatus.DONE.value
    assert task.transcript and "模拟转写" in task.transcript
    assert task.summary == _SUMMARY
    summarizer.summarize.assert_awaited_once()


@pytest.mark.asyncio
async def test_asr_failure_requeues_pending_before_retry_exhausted():
    recording = _recording()
    task = _inflight_task(recording, retry_count=0)
    dao = FakeTaskDao(task)
    executor, tx = _executor(
        dao=dao,
        transcriber=_instant_asr(fail=True),
        summarizer=MagicMock(),
        max_retries=3,
    )

    with patch("api.worker.executor.task_tx", tx), _patch_recording(recording):
        await executor.process(task.id)

    assert dao.failure_calls == 1
    assert task.retry_count == 1
    assert task.status == TaskStatus.PENDING.value
    assert task.error_msg and "ASR Mock 失败" in task.error_msg


@pytest.mark.asyncio
async def test_fourth_failure_marks_failed():
    recording = _recording()
    task = _inflight_task(recording, retry_count=3)
    dao = FakeTaskDao(task)
    executor, tx = _executor(
        dao=dao,
        transcriber=_instant_asr(fail=True),
        summarizer=MagicMock(),
        max_retries=3,
    )

    with patch("api.worker.executor.task_tx", tx), _patch_recording(recording):
        await executor.process(task.id)

    assert task.retry_count == 4
    assert task.status == TaskStatus.FAILED.value


@pytest.mark.asyncio
async def test_summarizer_failure_uses_same_retry_contract():
    recording = _recording()
    task = _inflight_task(recording)
    dao = FakeTaskDao(task)
    summarizer = MagicMock()
    summarizer.summarize = AsyncMock(side_effect=RuntimeError("LLM timeout"))
    executor, tx = _executor(
        dao=dao,
        transcriber=_instant_asr(),
        summarizer=summarizer,
        max_retries=3,
    )

    with patch("api.worker.executor.task_tx", tx), _patch_recording(recording):
        await executor.process(task.id)

    assert task.status == TaskStatus.PENDING.value
    assert task.transcript and "模拟转写" in task.transcript
    assert task.error_msg == "LLM timeout"


@pytest.mark.asyncio
async def test_mock_llm_when_summarizer_missing():
    recording = _recording()
    task = _inflight_task(recording)
    dao = FakeTaskDao(task)
    executor, tx = _executor(
        dao=dao,
        transcriber=_instant_asr(),
        summarizer=None,
        allow_mock_llm=True,
    )

    with patch("api.worker.executor.task_tx", tx), _patch_recording(recording):
        await executor.process(task.id)

    assert task.status == TaskStatus.DONE.value
    assert task.summary == {
        "summary": "模拟摘要",
        "key_points": ["要点1"],
        "todos": ["待办1"],
    }


@pytest.mark.asyncio
async def test_missing_llm_fails_when_mock_disabled():
    recording = _recording()
    task = _inflight_task(recording)
    dao = FakeTaskDao(task)
    executor, tx = _executor(
        dao=dao,
        transcriber=_instant_asr(),
        summarizer=None,
        allow_mock_llm=False,
    )

    with patch("api.worker.executor.task_tx", tx), _patch_recording(recording):
        await executor.process(task.id)

    assert task.status == TaskStatus.PENDING.value
    assert "LLM_API_KEY" in (task.error_msg or "")


@pytest.mark.asyncio
async def test_skips_when_task_missing():
    recording = _recording()
    task = _inflight_task(recording)
    dao = FakeTaskDao(task)
    dao.task = None
    executor, tx = _executor(dao=dao, transcriber=_instant_asr())

    with patch("api.worker.executor.task_tx", tx), _patch_recording(recording):
        await executor.process(task.id)

    assert dao.failure_calls == 0


@pytest.mark.asyncio
async def test_skips_when_not_held_by_this_worker():
    recording = _recording()
    task = _inflight_task(recording)
    task.status = TaskStatus.PENDING.value
    dao = FakeTaskDao(task)
    executor, tx = _executor(dao=dao, transcriber=_instant_asr())

    with patch("api.worker.executor.task_tx", tx), _patch_recording(recording):
        await executor.process(task.id)

    assert task.status == TaskStatus.PENDING.value
    assert dao.failure_calls == 0


@pytest.mark.asyncio
async def test_lease_lost_does_not_mark_failure():
    recording = _recording()
    task = _inflight_task(recording)
    dao = FakeTaskDao(task)
    dao.lease_ok = False
    summarizer = MagicMock()
    summarizer.summarize = AsyncMock(return_value=_SUMMARY)
    executor, tx = _executor(
        dao=dao,
        transcriber=_instant_asr(),
        summarizer=summarizer,
    )

    with patch("api.worker.executor.task_tx", tx), _patch_recording(recording):
        await executor.process(task.id)

    assert dao.failure_calls == 0
    assert task.status == TaskStatus.TRANSCRIBING.value
    summarizer.summarize.assert_not_called()


@pytest.mark.asyncio
async def test_reset_aborted_returns_task_to_pending():
    recording = _recording()
    task = _inflight_task(recording)
    dao = FakeTaskDao(task)
    executor, tx = _executor(dao=dao, transcriber=_instant_asr())

    with patch("api.worker.executor.task_tx", tx):
        await executor.reset_aborted([str(task.id)])

    assert task.status == TaskStatus.PENDING.value
    assert "优雅关闭" in (task.error_msg or "")
