from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from httpx import AsyncClient

from api.repository.dao.recording import Recording
from api.repository.dao.task import Task
from api.repository.dao.task_types import TaskStatus
from api.service.recording import RecordingService
from api.service.summarizer import SummarizerService

_DONE_PAYLOAD = {"summary": "hi", "key_points": ["a"], "todos": ["t1"]}


async def _fake_stream(_transcript: str):
    yield ("delta", '{"summary"')
    yield ("delta", ': "hi"}')
    yield ("done", _DONE_PAYLOAD)


async def _error_stream(_transcript: str):
    yield ("delta", "partial")
    yield ("error", "llm timeout")


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


def _done_task(recording: Recording, *, transcript: str = "hello transcript") -> Task:
    return Task(
        id=uuid4(),
        recording_id=recording.id,
        status=TaskStatus.DONE.value,
        transcript=transcript,
    )


def _assert_no_repo_writes(mock_repo: MagicMock) -> None:
    mock_repo.create_recording_and_task.assert_not_called()
    mock_repo.delete.assert_not_called()


@pytest.mark.asyncio
async def test_summary_stream_sse(
    client: AsyncClient,
    mock_repo: MagicMock,
    sample_recording: Recording,
):
    task = _done_task(sample_recording)
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
    assert f"data: {json.dumps(_DONE_PAYLOAD, ensure_ascii=False)}" in body
    mock_repo.get_by_id.assert_awaited_once_with(sample_recording.id)
    _assert_no_repo_writes(mock_repo)


@pytest.mark.asyncio
async def test_summary_stream_emits_error_event(
    client: AsyncClient,
    mock_repo: MagicMock,
    mock_summarizer: MagicMock,
    sample_recording: Recording,
):
    task = _done_task(sample_recording)
    mock_repo.get_by_id = AsyncMock(return_value=(sample_recording, task))
    mock_summarizer.summarize_stream = _error_stream

    resp = await client.get(
        f"/v1/recordings/{sample_recording.id}/summary/stream"
    )

    assert resp.status_code == 200
    assert "text/event-stream" in resp.headers["content-type"]
    body = resp.text
    assert "event: delta" in body
    assert "event: error" in body
    assert 'data: {"message": "llm timeout"}' in body
    mock_repo.set_task_summary_if_absent.assert_not_awaited()
    _assert_no_repo_writes(mock_repo)


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
    task = _done_task(sample_recording, transcript="hello")
    mock_repo.get_by_id = AsyncMock(return_value=(sample_recording, task))
    recording_service.summarizer = None

    resp = await client.get(
        f"/v1/recordings/{sample_recording.id}/summary/stream"
    )

    assert resp.status_code == 503
    assert resp.json()["error"]["code"] == "service_unavailable"


@pytest.mark.asyncio
async def test_summary_stream_unavailable_when_cannot_stream(
    client: AsyncClient,
    mock_repo: MagicMock,
    mock_summarizer: MagicMock,
    sample_recording: Recording,
):
    task = _done_task(sample_recording, transcript="hello")
    mock_repo.get_by_id = AsyncMock(return_value=(sample_recording, task))
    mock_summarizer.can_stream = False

    resp = await client.get(
        f"/v1/recordings/{sample_recording.id}/summary/stream"
    )

    assert resp.status_code == 503
    assert resp.json()["error"]["code"] == "service_unavailable"

@pytest.mark.asyncio
async def test_summary_stream_reuses_stored_summary(
    client: AsyncClient,
    mock_repo: MagicMock,
    mock_summarizer: MagicMock,
    sample_recording: Recording,
):
    stored = {"summary": "已存摘要", "key_points": ["k1"], "todos": ["t1"]}
    task = _done_task(sample_recording)
    task.summary = stored
    mock_repo.get_by_id = AsyncMock(return_value=(sample_recording, task))
    mock_summarizer.summarize_stream = MagicMock(
        side_effect=AssertionError("LLM 不应在复用路径被调用")
    )

    resp = await client.get(f"/v1/recordings/{sample_recording.id}/summary/stream")

    assert resp.status_code == 200
    body = resp.text
    assert "event: delta" in body
    assert "event: done" in body
    assert f"data: {json.dumps(stored, ensure_ascii=False)}" in body
    mock_summarizer.summarize_stream.assert_not_called()
    mock_repo.set_task_summary_if_absent.assert_not_awaited()


@pytest.mark.asyncio
async def test_summary_stream_reuse_needs_no_llm(
    client: AsyncClient,
    mock_repo: MagicMock,
    sample_recording: Recording,
    recording_service: RecordingService,
):
    task = _done_task(sample_recording)
    task.summary = {"summary": "已存", "key_points": [], "todos": []}
    mock_repo.get_by_id = AsyncMock(return_value=(sample_recording, task))
    recording_service.summarizer = None

    resp = await client.get(f"/v1/recordings/{sample_recording.id}/summary/stream")

    assert resp.status_code == 200
    assert "event: done" in resp.text


@pytest.mark.asyncio
async def test_summary_stream_writes_back_generated_summary(
    client: AsyncClient,
    mock_repo: MagicMock,
    sample_recording: Recording,
):
    task = _done_task(sample_recording)  # 有 transcript、无已存摘要
    mock_repo.get_by_id = AsyncMock(return_value=(sample_recording, task))

    resp = await client.get(f"/v1/recordings/{sample_recording.id}/summary/stream")

    assert resp.status_code == 200
    mock_repo.set_task_summary_if_absent.assert_awaited_once_with(
        task.id, summary=_DONE_PAYLOAD
    )
