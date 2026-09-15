"""执行器在「租约丢失 / 被取消 / fencing 拒绝」下的行为。

这些路径都是生产里真实会发生、且旧测试完全没覆盖的：续约在 ASR/LLM 执行**期间**
失败、处理中途锁被别的 worker 抢走、以及优雅关闭时的任务取消。
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from _worker_fakes import (
    FakeTaskDao,
    SlowTranscriber,
    instant_asr,
    inflight_task,
    make_executor,
    make_recording,
    patch_recording,
)

from api.repository.dao.task_types import TaskStatus

_SUMMARY = {"summary": "一句话", "key_points": [], "todos": []}


@pytest.mark.asyncio
async def test_mid_flight_lease_loss_cancels_work_without_marking_failure():
    """ASR 执行**期间**续约失败：取消在途工作、中止本机处理、不改写任务状态。

    首次续约成功、循环内第二次失败 —— 覆盖 ``_lease_renew_loop`` 在真实处理过程中
    发现租约丢失的路径（旧测试只覆盖了「首次续约即失败」）。
    """
    recording = make_recording()
    task = inflight_task(recording)
    dao = FakeTaskDao(task, lease_ok_sequence=[True, False])
    transcriber = SlowTranscriber(delay=5.0)
    executor, tx = make_executor(
        dao=dao,
        transcriber=transcriber,
        summarizer=MagicMock(),
        lease_renew_interval=0.01,
    )

    with patch("api.worker.executor.task_tx", tx), patch_recording(recording):
        await asyncio.wait_for(executor.process(task.id), timeout=5)

    assert dao.renew_calls >= 2  # 首次 + 循环内那次失败
    assert transcriber.cancelled is True
    assert transcriber.finished is False
    assert dao.failure_calls == 0
    assert dao.transitions == []
    assert task.status == TaskStatus.TRANSCRIBING.value
    assert task.transcript is None


@pytest.mark.asyncio
async def test_mark_summarizing_rejected_after_lease_stolen():
    """ASR 完成后锁已被他人接管：``mark_summarizing`` 被 fencing 守卫拒绝 → 租约丢失。

    任务不能落成 ``summarizing``，也不能计入失败重试。
    """
    recording = make_recording()
    task = inflight_task(recording)
    dao = FakeTaskDao(task)
    transcriber = SlowTranscriber(
        delay=0.0,
        on_start=lambda: setattr(task, "locked_by", "worker-other"),
    )
    executor, tx = make_executor(
        dao=dao,
        transcriber=transcriber,
        summarizer=MagicMock(),
    )

    with patch("api.worker.executor.task_tx", tx), patch_recording(recording):
        await executor.process(task.id)

    assert dao.failure_calls == 0
    assert dao.transitions == []
    assert task.status == TaskStatus.TRANSCRIBING.value
    assert task.locked_by == "worker-other"
    assert task.transcript is None


@pytest.mark.asyncio
async def test_mark_done_rejected_after_lease_stolen():
    """摘要完成后锁已被他人接管：``mark_done`` 被拒 → 不写入摘要、不计失败。"""
    recording = make_recording()
    task = inflight_task(recording)
    dao = FakeTaskDao(task)

    async def _steal_then_summarize(_transcript: str) -> dict:
        task.locked_by = "worker-other"
        return _SUMMARY

    summarizer = MagicMock()
    summarizer.summarize = AsyncMock(side_effect=_steal_then_summarize)
    executor, tx = make_executor(
        dao=dao,
        transcriber=instant_asr(),
        summarizer=summarizer,
    )

    with patch("api.worker.executor.task_tx", tx), patch_recording(recording):
        await executor.process(task.id)

    assert dao.failure_calls == 0
    assert dao.transitions == [TaskStatus.SUMMARIZING.value]
    assert task.status == TaskStatus.SUMMARIZING.value
    assert task.summary is None


@pytest.mark.asyncio
async def test_cancelled_task_propagates_and_cancels_in_flight_work():
    """优雅关闭取消任务：向上传播 CancelledError，不写失败状态。

    同时必须把在途的 ASR/LLM 调用一并取消——否则进程退出后仍会跑完并计费，
    且留下无人 await 的孤儿任务。
    """
    recording = make_recording()
    task = inflight_task(recording)
    dao = FakeTaskDao(task)
    transcriber = SlowTranscriber(delay=30.0)
    executor, tx = make_executor(dao=dao, transcriber=transcriber)

    with patch("api.worker.executor.task_tx", tx), patch_recording(recording):
        run = asyncio.create_task(executor.process(task.id))
        await asyncio.sleep(0.05)  # 让 ASR 进入等待
        assert transcriber.started == 1
        run.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(run, timeout=5)

    assert transcriber.cancelled is True
    assert dao.failure_calls == 0
    assert dao.transitions == []
    assert task.status == TaskStatus.TRANSCRIBING.value


@pytest.mark.asyncio
async def test_cancelled_task_leaves_no_pending_asr_orphan():
    """取消后不应残留仍在运行的任务（``all_tasks`` 里不能还有 ASR 协程）。"""
    recording = make_recording()
    task = inflight_task(recording)
    dao = FakeTaskDao(task)
    transcriber = SlowTranscriber(delay=30.0)
    executor, tx = make_executor(dao=dao, transcriber=transcriber)

    with patch("api.worker.executor.task_tx", tx), patch_recording(recording):
        run = asyncio.create_task(executor.process(task.id))
        await asyncio.sleep(0.05)
        run.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(run, timeout=5)
        await asyncio.sleep(0)  # 让取消落地

        stragglers = [
            t
            for t in asyncio.all_tasks()
            if t is not asyncio.current_task() and not t.done()
        ]

    assert stragglers == [], f"取消后仍有未完成任务: {stragglers}"
