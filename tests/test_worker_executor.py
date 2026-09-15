"""Worker 执行器状态机：pending 抢占后 transcribing → summarizing → done / failed。"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from _worker_fakes import (
    WORKER_ID,
    FakeTaskDao,
    instant_asr,
    inflight_task,
    make_executor,
    make_recording,
    patch_recording,
)

from api.repository.dao.task_types import TaskStatus

_SUMMARY = {"summary": "一句话", "key_points": ["k1"], "todos": ["t1"]}


@pytest.mark.asyncio
async def test_process_reaches_done_via_transcribing_and_summarizing():
    recording = make_recording()
    task = inflight_task(recording)
    dao = FakeTaskDao(task)
    summarizer = MagicMock()
    summarizer.summarize = AsyncMock(return_value=_SUMMARY)
    executor, tx = make_executor(
        dao=dao,
        transcriber=instant_asr(),
        summarizer=summarizer,
    )

    with patch("api.worker.executor.task_tx", tx), patch_recording(recording):
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
    recording = make_recording()
    task = inflight_task(recording, retry_count=0)
    dao = FakeTaskDao(task)
    executor, tx = make_executor(
        dao=dao,
        transcriber=instant_asr(fail=True),
        summarizer=MagicMock(),
        max_retries=3,
    )

    with patch("api.worker.executor.task_tx", tx), patch_recording(recording):
        await executor.process(task.id)

    assert dao.failure_calls == 1
    assert task.retry_count == 1
    assert task.status == TaskStatus.PENDING.value
    assert task.error_msg and "ASR Mock 失败" in task.error_msg


@pytest.mark.asyncio
async def test_fourth_failure_marks_failed():
    recording = make_recording()
    task = inflight_task(recording, retry_count=3)
    dao = FakeTaskDao(task)
    executor, tx = make_executor(
        dao=dao,
        transcriber=instant_asr(fail=True),
        summarizer=MagicMock(),
        max_retries=3,
    )

    with patch("api.worker.executor.task_tx", tx), patch_recording(recording):
        await executor.process(task.id)

    assert task.retry_count == 4
    assert task.status == TaskStatus.FAILED.value


@pytest.mark.asyncio
async def test_summarizer_failure_uses_same_retry_contract():
    recording = make_recording()
    task = inflight_task(recording)
    dao = FakeTaskDao(task)
    summarizer = MagicMock()
    summarizer.summarize = AsyncMock(side_effect=RuntimeError("LLM timeout"))
    executor, tx = make_executor(
        dao=dao,
        transcriber=instant_asr(),
        summarizer=summarizer,
        max_retries=3,
    )

    with patch("api.worker.executor.task_tx", tx), patch_recording(recording):
        await executor.process(task.id)

    assert task.status == TaskStatus.PENDING.value
    assert task.transcript and "模拟转写" in task.transcript
    assert task.error_msg == "LLM timeout"


@pytest.mark.asyncio
async def test_mock_llm_when_summarizer_missing():
    recording = make_recording()
    task = inflight_task(recording)
    dao = FakeTaskDao(task)
    executor, tx = make_executor(
        dao=dao,
        transcriber=instant_asr(),
        summarizer=None,
        allow_mock_llm=True,
    )

    with patch("api.worker.executor.task_tx", tx), patch_recording(recording):
        await executor.process(task.id)

    assert task.status == TaskStatus.DONE.value
    assert task.summary == {
        "summary": "模拟摘要",
        "key_points": ["要点1"],
        "todos": ["待办1"],
    }


@pytest.mark.asyncio
async def test_missing_llm_fails_when_mock_disabled():
    recording = make_recording()
    task = inflight_task(recording)
    dao = FakeTaskDao(task)
    executor, tx = make_executor(
        dao=dao,
        transcriber=instant_asr(),
        summarizer=None,
        allow_mock_llm=False,
    )

    with patch("api.worker.executor.task_tx", tx), patch_recording(recording):
        await executor.process(task.id)

    assert task.status == TaskStatus.PENDING.value
    assert "LLM_API_KEY" in (task.error_msg or "")


@pytest.mark.asyncio
async def test_skips_when_task_missing():
    recording = make_recording()
    task = inflight_task(recording)
    dao = FakeTaskDao(task)
    dao.task = None
    executor, tx = make_executor(dao=dao, transcriber=instant_asr())

    with patch("api.worker.executor.task_tx", tx), patch_recording(recording):
        await executor.process(task.id)

    assert dao.failure_calls == 0


@pytest.mark.asyncio
async def test_skips_when_not_held_by_this_worker():
    recording = make_recording()
    task = inflight_task(recording)
    task.status = TaskStatus.PENDING.value
    dao = FakeTaskDao(task)
    executor, tx = make_executor(dao=dao, transcriber=instant_asr())

    with patch("api.worker.executor.task_tx", tx), patch_recording(recording):
        await executor.process(task.id)

    assert task.status == TaskStatus.PENDING.value
    assert dao.failure_calls == 0


@pytest.mark.asyncio
async def test_lease_lost_does_not_mark_failure():
    recording = make_recording()
    task = inflight_task(recording)
    dao = FakeTaskDao(task)
    dao.lease_ok = False
    summarizer = MagicMock()
    summarizer.summarize = AsyncMock(return_value=_SUMMARY)
    executor, tx = make_executor(
        dao=dao,
        transcriber=instant_asr(),
        summarizer=summarizer,
    )

    with patch("api.worker.executor.task_tx", tx), patch_recording(recording):
        await executor.process(task.id)

    assert dao.failure_calls == 0
    assert task.status == TaskStatus.TRANSCRIBING.value
    summarizer.summarize.assert_not_called()


@pytest.mark.asyncio
async def test_reset_aborted_returns_task_to_pending():
    recording = make_recording()
    task = inflight_task(recording)
    dao = FakeTaskDao(task)
    executor, tx = make_executor(dao=dao, transcriber=instant_asr())

    with patch("api.worker.executor.task_tx", tx):
        await executor.reset_aborted([str(task.id)])

    assert task.status == TaskStatus.PENDING.value
    assert "优雅关闭" in (task.error_msg or "")
