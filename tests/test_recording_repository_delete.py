"""仓储删除：验证先删关联 task，再删 recording（不依赖真实 DB）。"""
from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from api.repository.dao.recording import Recording
from api.repository.recording import RecordingRepository


@pytest.mark.asyncio
async def test_delete_removes_task_then_recording_and_returns_path():
    recording_id = uuid4()
    recording = Recording(
        id=recording_id,
        file_name="demo.mp3",
        file_path="/tmp/demo.mp3",
        file_size=12,
        file_hash="c" * 64,
    )
    mock_session = MagicMock()
    order: list[str] = []

    recording_dao = MagicMock()
    recording_dao.get_by_id = AsyncMock(return_value=recording)

    async def _delete_task(rid):
        order.append("task")
        return 1

    async def _delete_recording(rid):
        order.append("recording")
        return True

    async def _commit():
        order.append("commit")

    task_dao = MagicMock()
    task_dao.delete_by_recording = AsyncMock(side_effect=_delete_task)
    recording_dao.delete = AsyncMock(side_effect=_delete_recording)
    mock_session.commit = AsyncMock(side_effect=_commit)

    @asynccontextmanager
    async def _fake_session():
        yield mock_session

    with patch("api.repository.recording._session", _fake_session), patch(
        "api.repository.recording.RecordingDao", return_value=recording_dao
    ), patch("api.repository.recording.TaskDao", return_value=task_dao):
        path = await RecordingRepository().delete(recording_id)

    assert path == "/tmp/demo.mp3"
    task_dao.delete_by_recording.assert_awaited_once_with(recording_id)
    recording_dao.delete.assert_awaited_once_with(recording_id)
    mock_session.commit.assert_awaited_once()
    assert order == ["task", "recording", "commit"]


@pytest.mark.asyncio
async def test_delete_missing_recording_returns_none_without_side_effects():
    recording_id = uuid4()
    mock_session = MagicMock()
    mock_session.commit = AsyncMock()

    recording_dao = MagicMock()
    recording_dao.get_by_id = AsyncMock(return_value=None)
    recording_dao.delete = AsyncMock()
    task_dao = MagicMock()
    task_dao.delete_by_recording = AsyncMock()

    @asynccontextmanager
    async def _fake_session():
        yield mock_session

    with patch("api.repository.recording._session", _fake_session), patch(
        "api.repository.recording.RecordingDao", return_value=recording_dao
    ), patch("api.repository.recording.TaskDao", return_value=task_dao):
        path = await RecordingRepository().delete(recording_id)

    assert path is None
    task_dao.delete_by_recording.assert_not_called()
    recording_dao.delete.assert_not_called()
    mock_session.commit.assert_not_called()
