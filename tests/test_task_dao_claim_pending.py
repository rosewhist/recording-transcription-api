"""TaskDao.claim_pending：Core select+update / SKIP LOCKED 路径。"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.dialects import postgresql

from api.repository.dao.task import TaskDao, TaskStatus


@pytest.mark.asyncio
async def test_claim_pending_limit_non_positive_returns_empty():
    session = MagicMock()
    dao = TaskDao(session)

    assert await dao.claim_pending(limit=0, worker_id="w", lease_seconds=30) == []
    assert await dao.claim_pending(limit=-1, worker_id="w", lease_seconds=30) == []
    session.scalars.assert_not_called()
    session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_claim_pending_no_rows_skips_update():
    session = MagicMock()
    session.scalars = AsyncMock(return_value=iter([]))
    session.execute = AsyncMock()
    dao = TaskDao(session)

    assert await dao.claim_pending(limit=5, worker_id="w1", lease_seconds=60) == []
    session.scalars.assert_awaited_once()
    session.execute.assert_not_called()

    pick_stmt = session.scalars.await_args.args[0]
    compiled = str(pick_stmt.compile(dialect=postgresql.dialect()))
    assert "FOR UPDATE" in compiled
    assert "SKIP LOCKED" in compiled


@pytest.mark.asyncio
async def test_claim_pending_updates_locked_ids_and_returns_them():
    task_id = uuid4()
    session = MagicMock()
    session.scalars = AsyncMock(return_value=iter([task_id]))

    result = MagicMock()
    result.scalars.return_value.all.return_value = [task_id]
    session.execute = AsyncMock(return_value=result)

    dao = TaskDao(session)
    claimed = await dao.claim_pending(limit=3, worker_id="worker-a", lease_seconds=120)

    assert claimed == [task_id]
    session.execute.assert_awaited_once()

    claim_stmt = session.execute.await_args.args[0]
    compiled = str(
        claim_stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})
    )
    assert "UPDATE tasks" in compiled
    assert TaskStatus.TRANSCRIBING.value in compiled
    assert "worker-a" in compiled
    assert "make_interval" in compiled
    assert "RETURNING" in compiled
