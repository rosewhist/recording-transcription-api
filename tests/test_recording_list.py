from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from httpx import AsyncClient

from api.repository.dao.recording import Recording
from api.repository.dao.task import Task
from api.repository.dao.task_types import TaskStatus


def _recording(*, name: str = "demo.mp3", size: int = 12) -> Recording:
    return Recording(
        id=uuid4(),
        file_name=name,
        file_path=f"/tmp/{name}",
        file_size=size,
        file_hash=uuid4().hex + "0" * 32,
        created_at=datetime.now(timezone.utc),
    )


@pytest.mark.asyncio
async def test_list_recordings_default_pagination(
    client: AsyncClient,
    mock_repo: MagicMock,
):
    recording = _recording()
    task = Task(
        id=uuid4(),
        recording_id=recording.id,
        status=TaskStatus.TRANSCRIBING.value,
    )
    mock_repo.list_page = AsyncMock(return_value=([(recording, task)], 1))

    resp = await client.get("/v1/recordings")

    assert resp.status_code == 200
    body = resp.json()
    assert body["page"] == 1
    assert body["page_size"] == 20
    assert body["total"] == 1
    assert len(body["items"]) == 1
    item = body["items"][0]
    assert item["recording_id"] == str(recording.id)
    assert item["file_name"] == recording.file_name
    assert item["file_size"] == recording.file_size
    assert item["task_id"] == str(task.id)
    assert item["status"] == TaskStatus.TRANSCRIBING.value
    mock_repo.list_page.assert_awaited_once_with(page=1, page_size=20)


@pytest.mark.asyncio
async def test_list_recordings_custom_page(
    client: AsyncClient,
    mock_repo: MagicMock,
):
    mock_repo.list_page = AsyncMock(return_value=([], 40))

    resp = await client.get("/v1/recordings", params={"page": 2, "page_size": 10})

    assert resp.status_code == 200
    body = resp.json()
    assert body["page"] == 2
    assert body["page_size"] == 10
    assert body["total"] == 40
    assert body["items"] == []
    mock_repo.list_page.assert_awaited_once_with(page=2, page_size=10)


@pytest.mark.asyncio
async def test_list_recordings_empty(
    client: AsyncClient,
    mock_repo: MagicMock,
):
    mock_repo.list_page = AsyncMock(return_value=([], 0))

    resp = await client.get("/v1/recordings")

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 0
    assert body["items"] == []


@pytest.mark.asyncio
async def test_list_recordings_rejects_invalid_page(client: AsyncClient):
    resp = await client.get("/v1/recordings", params={"page": 0})

    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "validation_error"


@pytest.mark.asyncio
async def test_list_recordings_rejects_invalid_page_size(client: AsyncClient):
    resp = await client.get("/v1/recordings", params={"page_size": 101})

    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "validation_error"
