"""仓储列表：验证按 created_at 倒序分页，并保留 DAO 返回顺序。"""
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from api.repository.dao.recording import Recording, RecordingDao
from api.repository.dao.task import Task
from api.repository.dao.task_types import TaskStatus
from api.repository.recording import RecordingRepository


def _recording(*, created_at: datetime, name: str = "demo.mp3") -> Recording:
    return Recording(
        id=uuid4(),
        file_name=name,
        file_path=f"/tmp/{name}",
        file_size=12,
        file_hash=uuid4().hex + "0" * 32,
        created_at=created_at,
    )


@pytest.mark.asyncio
async def test_dao_list_page_orders_by_created_at_desc():
    captured: dict = {}

    async def _scalars(stmt):
        captured["stmt"] = stmt
        result = MagicMock()
        result.all = MagicMock(return_value=[])
        return result

    session = MagicMock()
    session.scalars = AsyncMock(side_effect=_scalars)

    await RecordingDao(session).list_page(offset=10, limit=5)

    sql = str(
        captured["stmt"].compile(compile_kwargs={"literal_binds": True})
    ).lower()
    assert "order by recordings.created_at desc" in sql
    assert "limit 5" in sql
    assert "offset 10" in sql


@pytest.mark.asyncio
async def test_repo_list_page_preserves_order_and_attaches_latest_task():
    now = datetime.now(timezone.utc)
    newer = _recording(created_at=now, name="new.mp3")
    older = _recording(created_at=now - timedelta(hours=1), name="old.mp3")
    # DAO 已按 created_at 倒序返回
    recordings = [newer, older]
    task_newer = Task(
        id=uuid4(),
        recording_id=newer.id,
        status=TaskStatus.DONE.value,
    )
    task_older = Task(
        id=uuid4(),
        recording_id=older.id,
        status=TaskStatus.PENDING.value,
    )

    recording_dao = MagicMock()
    recording_dao.count_all = AsyncMock(return_value=2)
    recording_dao.list_page = AsyncMock(return_value=recordings)
    task_dao = MagicMock()
    task_dao.list_by_recording_ids = AsyncMock(
        return_value=[task_older, task_newer]  # 故意乱序，仓储应按 recording 顺序组装
    )
    mock_session = MagicMock()

    @asynccontextmanager
    async def _fake_session():
        yield mock_session

    with patch("api.repository.recording._session", _fake_session), patch(
        "api.repository.recording.RecordingDao", return_value=recording_dao
    ), patch("api.repository.recording.TaskDao", return_value=task_dao):
        items, total = await RecordingRepository().list_page(page=2, page_size=10)

    assert total == 2
    recording_dao.list_page.assert_awaited_once_with(offset=10, limit=10)
    task_dao.list_by_recording_ids.assert_awaited_once_with([newer.id, older.id])
    assert [r.id for r, _ in items] == [newer.id, older.id]
    assert items[0][1].id == task_newer.id
    assert items[0][1].status == TaskStatus.DONE.value
    assert items[1][1].id == task_older.id
    assert items[1][1].status == TaskStatus.PENDING.value
