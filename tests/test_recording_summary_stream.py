from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from httpx import AsyncClient

from api.repository.dao.recording import Recording
from api.repository.dao.task import Task
from api.repository.dao.task_types import TaskStatus
from api.service.recording import RecordingService
from api.service.summarizer import SummarizerService


async def _fake_stream(_transcript: str):
    yield ("delta", '{"summary"')
    yield ("delta", ': "hi"}')
    yield (
        "done",
        {"summary": "hi", "key_points": ["a"], "todos": []},
    )


@pytest.fixture
def mock_summarizer() -> MagicMock:
    summarizer = MagicMock(spec=SummarizerService)
    summarizer.can_stream = True
    summarizer.summarize_stream = _fake_stream
    return summarizer


@pytest.fixture
def recording_service(
    upload_dir, mock_repo, mock_worker, mock_summarizer
) -> RecordingService:
    return RecordingService(
        repo=mock_repo,
        worker=mock_worker,
        summarizer=mock_summarizer,
    )


@pytest.mark.asyncio
async def test_summary_stream_sse(
    client: AsyncClient,
    mock_repo: MagicMock,
    sample_recording: Recording,
):
    task = Task(
        id=uuid4(),
        recording_id=sample_recording.id,
        status=TaskStatus.DONE.value,
        transcript="hello transcript",
    )
    mock_repo.get_by_id = AsyncMock(return_value=(sample_recording, task))

    resp = await client.get(
        f"/v1/recordings/{sample_recording.id}/summary/stream"
    )

    assert resp.status_code == 200
    assert "text/event-stream" in resp.headers["content-type"]
    body = resp.text
    assert "event: delta" in body
    assert 'data: {"text":' in body
    assert "event: done" in body
    assert '"summary": "hi"' in body


@pytest.mark.asyncio
async def test_summary_stream_requires_transcript(
    client: AsyncClient,
    mock_repo: MagicMock,
    sample_recording: Recording,
    sample_task: Task,
):
    sample_task.transcript = None
    mock_repo.get_by_id = AsyncMock(return_value=(sample_recording, sample_task))

    resp = await client.get(
        f"/v1/recordings/{sample_recording.id}/summary/stream"
    )

    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_request"


@pytest.mark.asyncio
async def test_summary_stream_not_found(
    client: AsyncClient,
    mock_repo: MagicMock,
):
    missing = uuid4()
    mock_repo.get_by_id = AsyncMock(return_value=(None, None))

    resp = await client.get(f"/v1/recordings/{missing}/summary/stream")

    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "recording_not_found"


@pytest.mark.asyncio
async def test_summary_stream_unavailable_without_llm(
    client: AsyncClient,
    mock_repo: MagicMock,
    sample_recording: Recording,
    recording_service: RecordingService,
):
    task = Task(
        id=uuid4(),
        recording_id=sample_recording.id,
        status=TaskStatus.DONE.value,
        transcript="hello",
    )
    mock_repo.get_by_id = AsyncMock(return_value=(sample_recording, task))
    recording_service.summarizer = None

    resp = await client.get(
        f"/v1/recordings/{sample_recording.id}/summary/stream"
    )

    assert resp.status_code == 503
    assert resp.json()["error"]["code"] == "service_unavailable"
