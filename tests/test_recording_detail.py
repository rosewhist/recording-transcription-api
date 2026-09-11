from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from httpx import AsyncClient

from api.repository.dao.recording import Recording
from api.repository.dao.task import Task
from api.repository.dao.task_types import TaskStatus


def _recording() -> Recording:
    return Recording(
        id=uuid4(),
        file_name="demo.mp3",
        file_path="/tmp/demo.mp3",
        file_size=12,
        file_hash="b" * 64,
        created_at=datetime.now(timezone.utc),
    )


@pytest.mark.asyncio
async def test_get_recording_pending_omits_results(
    client: AsyncClient,
    mock_repo: MagicMock,
):
    recording = _recording()
    task = Task(
        id=uuid4(),
        recording_id=recording.id,
        status=TaskStatus.PENDING.value,
        transcript="should-hide",
        summary={"summary": "hide"},
    )
    mock_repo.get_by_id = AsyncMock(return_value=(recording, task))

    resp = await client.get(f"/v1/recordings/{recording.id}")

    assert resp.status_code == 200
    body = resp.json()
    assert body["recording_id"] == str(recording.id)
    assert body["file_name"] == "demo.mp3"
    assert body["task_id"] == str(task.id)
    assert body["status"] == TaskStatus.PENDING.value
    assert body["transcript"] is None
    assert body["summary"] is None
    mock_repo.get_by_id.assert_awaited_once_with(recording.id)


@pytest.mark.asyncio
async def test_get_recording_done_includes_results(
    client: AsyncClient,
    mock_repo: MagicMock,
):
    recording = _recording()
    task = Task(
        id=uuid4(),
        recording_id=recording.id,
        status=TaskStatus.DONE.value,
        transcript="hello world",
        summary={"summary": "一句话", "key_points": ["a"], "todos": []},
    )
    mock_repo.get_by_id = AsyncMock(return_value=(recording, task))

    resp = await client.get(f"/v1/recordings/{recording.id}")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == TaskStatus.DONE.value
    assert body["transcript"] == "hello world"
    assert body["summary"] == {
        "summary": "一句话",
        "key_points": ["a"],
        "todos": [],
    }


@pytest.mark.asyncio
async def test_get_recording_failed_includes_error(
    client: AsyncClient,
    mock_repo: MagicMock,
):
    recording = _recording()
    task = Task(
        id=uuid4(),
        recording_id=recording.id,
        status=TaskStatus.FAILED.value,
        error_msg="mock asr failed",
        transcript="partial",
        summary={"summary": "nope"},
    )
    mock_repo.get_by_id = AsyncMock(return_value=(recording, task))

    resp = await client.get(f"/v1/recordings/{recording.id}")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == TaskStatus.FAILED.value
    assert body["error_msg"] == "mock asr failed"
    assert body["transcript"] is None
    assert body["summary"] is None


@pytest.mark.asyncio
async def test_get_recording_not_found(client: AsyncClient, mock_repo: MagicMock):
    missing_id = uuid4()
    mock_repo.get_by_id = AsyncMock(return_value=(None, None))

    resp = await client.get(f"/v1/recordings/{missing_id}")

    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "recording_not_found"
