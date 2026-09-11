from __future__ import annotations

from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from httpx import AsyncClient

from api.repository.dao.recording import Recording
from api.repository.dao.task import Task
from api.repository.dao.task_types import TaskStatus
from api.service.recording import RecordingService


def _audio_files(
    content: bytes = b"fake-audio-bytes",
    name: str = "demo.mp3",
    content_type: str = "audio/mpeg",
):
    return {"file": (name, BytesIO(content), content_type)}


@pytest.mark.asyncio
async def test_upload_creates_recording_and_task(
    client: AsyncClient,
    recording_service: RecordingService,
    mock_repo: MagicMock,
    mock_worker: MagicMock,
    sample_recording: Recording,
    sample_task: Task,
    upload_dir: Path,
):
    content = b"fake-audio-bytes"
    mock_repo.create_recording_and_task.return_value = (
        sample_recording,
        sample_task,
        True,
    )

    resp = await client.post("/v1/recordings", files=_audio_files(content=content))

    assert resp.status_code == 201
    body = resp.json()
    assert body["recording_id"] == str(sample_recording.id)
    assert body["task_id"] == str(sample_task.id)
    assert body["status"] == TaskStatus.PENDING.value
    mock_worker.submit.assert_awaited_once_with(sample_task.id)
    mock_repo.create_recording_and_task.assert_awaited_once()

    call_kwargs = mock_repo.create_recording_and_task.await_args.kwargs
    saved_path = Path(call_kwargs["file_path"])
    assert call_kwargs["file_name"] == "demo.mp3"
    assert call_kwargs["file_size"] == len(content)
    assert saved_path.exists()
    assert saved_path.parent == upload_dir
    assert saved_path.read_bytes() == content


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name,content_type",
    [
        ("demo.wav", "audio/wav"),
        ("demo.mp3", "audio/mpeg"),
        ("demo.m4a", "audio/mp4"),
        ("demo.aac", "audio/aac"),
    ],
)
async def test_upload_accepts_allowed_extensions(
    client: AsyncClient,
    mock_repo: MagicMock,
    sample_recording: Recording,
    sample_task: Task,
    name: str,
    content_type: str,
):
    mock_repo.create_recording_and_task.return_value = (
        sample_recording,
        sample_task,
        True,
    )

    resp = await client.post(
        "/v1/recordings",
        files=_audio_files(name=name, content_type=content_type),
    )

    assert resp.status_code == 201
    mock_repo.create_recording_and_task.assert_awaited_once()
    assert mock_repo.create_recording_and_task.await_args.kwargs["file_name"] == name


@pytest.mark.asyncio
async def test_upload_idempotent_returns_200(
    client: AsyncClient,
    mock_repo: MagicMock,
    mock_worker: MagicMock,
    sample_recording: Recording,
    sample_task: Task,
):
    mock_repo.get_by_hash.return_value = (sample_recording, sample_task)

    resp = await client.post("/v1/recordings", files=_audio_files())

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
        files=_audio_files(name="notes.txt"),
    )

    assert resp.status_code == 400
    err = resp.json()["error"]
    assert err["code"] == "invalid_upload"
    assert "扩展名" in err["message"]


@pytest.mark.asyncio
async def test_upload_rejects_empty_file(client: AsyncClient):
    resp = await client.post("/v1/recordings", files=_audio_files(content=b""))

    assert resp.status_code == 400
    err = resp.json()["error"]
    assert err["code"] == "invalid_upload"
    assert "为空" in err["message"]


@pytest.mark.asyncio
async def test_upload_rejects_oversized_file(
    client: AsyncClient,
    recording_service: RecordingService,
):
    # 用 1MB 上限替代真实 50MB，避免分配超大内存
    recording_service.settings.MAX_FILE_SIZE_MB = 1
    content = b"x" * (1 * 1024 * 1024 + 1)

    resp = await client.post("/v1/recordings", files=_audio_files(content=content))

    assert resp.status_code == 400
    err = resp.json()["error"]
    assert err["code"] == "invalid_upload"
    assert "1MB" in err["message"]


@pytest.mark.asyncio
async def test_upload_missing_file_returns_validation_error(client: AsyncClient):
    resp = await client.post("/v1/recordings")

    assert resp.status_code == 400
    err = resp.json()["error"]
    assert err["code"] == "validation_error"
