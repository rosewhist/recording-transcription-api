"""Dispatcher：重启回收僵尸任务、并发上限调度。"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from api.worker.dispatcher import TaskDispatcher


def _dispatcher(*, executor, max_concurrency: int = 3) -> TaskDispatcher:
    return TaskDispatcher(
        db_session_factory=lambda: MagicMock(),
        executor=executor,
        worker_id="worker-test",
        lease_seconds=300,
        max_concurrency=max_concurrency,
        max_batch_size=50,
        poll_idle_seconds=0.05,
        reclaim_interval=30.0,
        stop_event=asyncio.Event(),
        wake_event=asyncio.Event(),
    )


@pytest.fixture
def fake_dao() -> MagicMock:
    dao = MagicMock()
    dao.reclaim_in_flight = AsyncMock(return_value=2)
    dao.claim_pending = AsyncMock(return_value=[])
    return dao


@pytest.mark.asyncio
async def test_recover_zombie_tasks_reclaims_own_locks(fake_dao: MagicMock, monkeypatch):
    executor = MagicMock()
    dispatcher = _dispatcher(executor=executor)

    @asynccontextmanager
    async def _tx(_factory):
        yield fake_dao

    monkeypatch.setattr("api.worker.dispatcher.task_tx", _tx)

    await dispatcher.recover_zombie_tasks(include_own_locks=True)

    fake_dao.reclaim_in_flight.assert_awaited_once()
    kwargs = fake_dao.reclaim_in_flight.await_args.kwargs
    assert kwargs["worker_id"] == "worker-test"
    assert kwargs["include_own_locks"] is True
    assert "重启" in kwargs["error_msg"] or "租约过期" in kwargs["error_msg"]


@pytest.mark.asyncio
async def test_recover_expired_leases_does_not_include_own_locks(
    fake_dao: MagicMock, monkeypatch
):
    dispatcher = _dispatcher(executor=MagicMock())

    @asynccontextmanager
    async def _tx(_factory):
        yield fake_dao

    monkeypatch.setattr("api.worker.dispatcher.task_tx", _tx)

    await dispatcher.recover_zombie_tasks(include_own_locks=False)

    kwargs = fake_dao.reclaim_in_flight.await_args.kwargs
    assert kwargs["include_own_locks"] is False


@pytest.mark.asyncio
async def test_start_reclaims_then_stop(fake_dao: MagicMock, monkeypatch):
    fake_dao.reclaim_in_flight = AsyncMock(return_value=1)
    fake_dao.claim_pending = AsyncMock(return_value=[])
    dispatcher = _dispatcher(executor=MagicMock())

    @asynccontextmanager
    async def _tx(_factory):
        yield fake_dao

    monkeypatch.setattr("api.worker.dispatcher.task_tx", _tx)

    await dispatcher.start()
    try:
        kwargs = fake_dao.reclaim_in_flight.await_args.kwargs
        assert kwargs["include_own_locks"] is True
    finally:
        dispatcher.stop_event.set()
        dispatcher.wake()
        await dispatcher.stop_loop(timeout_seconds=2)


@pytest.mark.asyncio
async def test_schedule_task_tracks_running_and_cleans_up():
    started = asyncio.Event()
    release = asyncio.Event()

    async def _process(_task_id):
        started.set()
        await release.wait()

    executor = MagicMock()
    executor.process = _process
    dispatcher = _dispatcher(executor=executor, max_concurrency=1)
    task_id = uuid4()

    dispatcher._schedule_task(task_id)
    key = str(task_id)
    assert key in dispatcher.running_tasks
    await asyncio.wait_for(started.wait(), timeout=1)
    assert len(dispatcher.running_tasks) == 1

    release.set()
    await dispatcher.running_tasks[key]
    assert key not in dispatcher.running_tasks


@pytest.mark.asyncio
async def test_dispatcher_loop_respects_max_concurrency(monkeypatch):
    release = asyncio.Event()
    in_flight = 0
    max_seen = 0
    claimed = [uuid4(), uuid4(), uuid4()]

    async def _process(_task_id):
        nonlocal in_flight, max_seen
        in_flight += 1
        max_seen = max(max_seen, in_flight)
        try:
            await release.wait()
        finally:
            in_flight -= 1

    dao = MagicMock()
    dao.reclaim_in_flight = AsyncMock(return_value=0)

    async def _claim(*, limit, worker_id, lease_seconds):
        if not claimed:
            return []
        batch = claimed[:limit]
        del claimed[: len(batch)]
        return batch

    dao.claim_pending = AsyncMock(side_effect=_claim)

    executor = MagicMock()
    executor.process = _process
    dispatcher = _dispatcher(executor=executor, max_concurrency=2)

    @asynccontextmanager
    async def _tx(_factory):
        yield dao

    monkeypatch.setattr("api.worker.dispatcher.task_tx", _tx)

    await dispatcher.start()
    try:
        for _ in range(50):
            if max_seen >= 2 and len(dispatcher.running_tasks) == 2:
                break
            await asyncio.sleep(0.02)
        assert max_seen == 2
        assert len(dispatcher.running_tasks) == 2
    finally:
        release.set()
        dispatcher.stop_event.set()
        dispatcher.wake()
        await dispatcher.stop_loop(timeout_seconds=2)
