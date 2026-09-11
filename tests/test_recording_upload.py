from __future__ import annotations

from io import BytesIO
from unittest.mock import MagicMock

import pytest
from httpx import AsyncClient

from api.repository.dao.recording import Recording
from api.repository.dao.task import Task
from api.repository.dao.task_types import TaskStatus
from api.service.recording import RecordingService


def _mp3_files(content: bytes = b"fake-audio-bytes", name: str = "demo.mp3"):
    return {"file": (name, BytesIO(content), "audio/mpeg")}


@pytest.mark.asyncio
async def test_upload_creates_recording_and_task(
    client: AsyncClient,
    recording_service: RecordingService,
    mock_repo: MagicMock,
    mock_worker: MagicMock,
    sample_recording: Recording,
    sample_task: Task,
):
    mock_repo.create_recording_and_task.return_value = (
        sample_recording,
        sample_task,
        True,
    )

    resp = await client.post("/v1/recordings", files=_mp3_files())

    assert resp.status_code == 201
    body = resp.json()
    assert body["recording_id"] == str(sample_recording.id)
    assert body["task_id"] == str(sample_task.id)
    assert body["status"] == TaskStatus.PENDING.value
    mock_worker.submit.assert_awaited_once_with(sample_task.id)
    mock_repo.create_recording_and_task.assert_awaited_once()


@pytest.mark.asyncio
async def test_upload_idempotent_returns_200(
    client: AsyncClient,
    mock_repo: MagicMock,
    mock_worker: MagicMock,
    sample_recording: Recording,
    sample_task: Task,
):
    mock_repo.get_by_hash.return_value = (sample_recording, sample_task)

    resp = await client.post("/v1/recordings", files=_mp3_files())

    assert resp.status_code == 200
    body = resp.json()
    assert body["recording_id"] == str(sample_recording.id)
    assert body["task_id"] == str(sample_task.id)
    assert body["status"] == TaskStatus.PENDING.value
    mock_repo.create_recording_and_task.assert_not_called()
    mock_worker.submit.assert_not_called()


@pytest.mark.asyncio
async def test_upload_rejects_unsupported_extension(client: AsyncClient):
    resp = await client.post(
        "/v1/recordings",
        files=_mp3_files(name="notes.txt"),
    )

    assert resp.status_code == 400
    err = resp.json()["error"]
    assert err["code"] == "invalid_upload"
    assert "扩展名" in err["message"]


@pytest.mark.asyncio
async def test_upload_rejects_empty_file(client: AsyncClient):
    resp = await client.post("/v1/recordings", files=_mp3_files(content=b""))

    assert resp.status_code == 400
    err = resp.json()["error"]
    assert err["code"] == "invalid_upload"
    assert "为空" in err["message"]


@pytest.mark.asyncio
async def test_upload_missing_file_returns_validation_error(client: AsyncClient):
    resp = await client.post("/v1/recordings")

    assert resp.status_code == 400
    err = resp.json()["error"]
    assert err["code"] == "validation_error"
