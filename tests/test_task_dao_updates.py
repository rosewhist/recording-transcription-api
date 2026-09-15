"""TaskDao 状态机 / 租约更新：SQLAlchemy Core 路径。"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.dialects import postgresql

from api.repository.dao.task import TaskDao


def _compile(stmt, *, literal_binds: bool = False) -> str:
    kwargs = {"literal_binds": True} if literal_binds else {}
    return str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs=kwargs))


@pytest.mark.asyncio
async def test_renew_lease_updates_when_locked_by_worker():
    session = MagicMock()
    session.execute = AsyncMock(return_value=MagicMock(rowcount=1))
    dao = TaskDao(session)

    ok = await dao.renew_lease(uuid4(), worker_id="w1", lease_seconds=90)
    assert ok is True
    compiled = _compile(session.execute.await_args.args[0])
    assert "UPDATE tasks" in compiled
    assert "make_interval" in compiled
    assert "locked_by" in compiled


@pytest.mark.asyncio
async def test_mark_summarizing_and_done():
    session = MagicMock()
    session.execute = AsyncMock(return_value=MagicMock(rowcount=1))
    dao = TaskDao(session)
    task_id = uuid4()

    assert await dao.mark_summarizing(task_id, worker_id="w1", transcript="hi") is True
    summarizing_sql = _compile(session.execute.await_args.args[0], literal_binds=True)
    assert "summarizing" in summarizing_sql
    assert "transcript" in summarizing_sql

    assert await dao.mark_done(task_id, worker_id="w1", summary={"t": 1}) is True
    done_sql = _compile(session.execute.await_args.args[0])
    assert "UPDATE tasks" in done_sql
    assert "summary" in done_sql
    assert "locked_by" in done_sql


@pytest.mark.asyncio
async def test_mark_failure_returns_retry_tuple_or_none():
    session = MagicMock()
    result = MagicMock()
    result.first.return_value = (2, "pending")
    session.execute = AsyncMock(return_value=result)
    dao = TaskDao(session)

    out = await dao.mark_failure(
        uuid4(), worker_id="w1", error_msg="boom", max_retries=3
    )
    assert out == (2, "pending")
    compiled = _compile(session.execute.await_args.args[0])
    assert "CASE" in compiled.upper()
    assert "power" in compiled.lower()
    assert "RETURNING" in compiled.upper()

    result.first.return_value = None
    assert (
        await dao.mark_failure(uuid4(), worker_id="w1", error_msg="x", max_retries=3)
        is None
    )


@pytest.mark.asyncio
async def test_requeue_failed_loads_task_when_updated():
    task_id = uuid4()
    session = MagicMock()
    result = MagicMock()
    result.first.return_value = (task_id,)
    session.execute = AsyncMock(return_value=result)
    session.get = AsyncMock(return_value=MagicMock(id=task_id))
    dao = TaskDao(session)

    task = await dao.requeue_failed(task_id)
    assert task is not None
    assert task.id == task_id
    compiled = _compile(session.execute.await_args.args[0], literal_binds=True)
    assert "failed" in compiled
    assert "pending" in compiled
    assert "RETURNING" in compiled.upper()


@pytest.mark.asyncio
async def test_requeue_failed_returns_none_when_not_failed():
    session = MagicMock()
    result = MagicMock()
    result.first.return_value = None
    session.execute = AsyncMock(return_value=result)
    dao = TaskDao(session)

    assert await dao.requeue_failed(uuid4()) is None
    session.get.assert_not_called()


@pytest.mark.asyncio
async def test_reclaim_in_flight_include_own_locks_filter():
    session = MagicMock()
    session.execute = AsyncMock(return_value=MagicMock(rowcount=3))
    dao = TaskDao(session)

    n = await dao.reclaim_in_flight(
        worker_id="w1", include_own_locks=True, error_msg="reclaim"
    )
    assert n == 3
    compiled = _compile(session.execute.await_args.args[0])
    assert "UPDATE tasks" in compiled
    assert "locked_by" in compiled
    assert "lease_expires_at" in compiled
    assert " OR " in compiled.upper()


@pytest.mark.asyncio
async def test_reclaim_in_flight_expired_only():
    session = MagicMock()
    session.execute = AsyncMock(return_value=MagicMock(rowcount=1))
    dao = TaskDao(session)

    n = await dao.reclaim_in_flight(
        worker_id="w1", include_own_locks=False, error_msg="expired"
    )
    assert n == 1
    compiled = _compile(session.execute.await_args.args[0])
    assert "lease_expires_at" in compiled
    assert " OR " not in compiled.upper().split("WHERE", 1)[-1]


@pytest.mark.asyncio
async def test_reset_aborted_empty_and_nonempty():
    session = MagicMock()
    session.execute = AsyncMock(return_value=MagicMock(rowcount=2))
    dao = TaskDao(session)

    assert await dao.reset_aborted([], worker_id="w1", error_msg="abort") == 0
    session.execute.assert_not_called()

    n = await dao.reset_aborted(
        [uuid4(), uuid4()], worker_id="w1", error_msg="abort"
    )
    assert n == 2
    compiled = _compile(session.execute.await_args.args[0])
    assert "UPDATE tasks" in compiled
    assert "locked_by" in compiled


@pytest.mark.asyncio
async def test_set_summary_if_absent_guards_on_null_summary():
    task_id = uuid4()
    session = MagicMock()
    result = MagicMock()
    result.first.return_value = (task_id,)
    session.execute = AsyncMock(return_value=result)
    dao = TaskDao(session)

    assert await dao.set_summary_if_absent(task_id, summary={"t": 1}) is True
    compiled = _compile(session.execute.await_args.args[0])
    assert "UPDATE tasks" in compiled
    assert "summary" in compiled
    assert "IS NULL" in compiled.upper()
    assert "RETURNING" in compiled.upper()

    result.first.return_value = None
    assert await dao.set_summary_if_absent(task_id, summary={"t": 2}) is False
