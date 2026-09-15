from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from httpx import AsyncClient

from api.controller.recording import stream_recording_summary
from api.repository.dao.recording import Recording
from api.repository.dao.task import Task
from api.repository.dao.task_types import TaskStatus
from api.service.recording import _REPLAY_CHUNK_SIZE, RecordingService
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


def _assert_repo_read_only(mock_repo: MagicMock) -> None:
    """SSE 路径只读：除 ``get_by_id`` 外不得触碰仓储（尤其不得写摘要）。"""
    called = {name for name, _args, _kwargs in mock_repo.mock_calls}
    assert called <= {"get_by_id"}, f"意外的仓储调用: {called}"


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
    _assert_repo_read_only(mock_repo)


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
    _assert_repo_read_only(mock_repo)


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
    _assert_repo_read_only(mock_repo)


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
async def test_summary_stream_does_not_persist_generated_summary(
    client: AsyncClient,
    mock_repo: MagicMock,
    sample_recording: Recording,
):
    """摘要由流水线在 ``done`` 时写入，流式生成不得回写。

    否则非 ``done`` 的任务会持有一份详情接口刻意隐藏的结果，两个接口对同一条数据
    给出矛盾答案。
    """
    task = _done_task(sample_recording)  # 有 transcript、无已存摘要
    mock_repo.get_by_id = AsyncMock(return_value=(sample_recording, task))

    resp = await client.get(f"/v1/recordings/{sample_recording.id}/summary/stream")

    assert resp.status_code == 200
    assert "event: done" in resp.text
    _assert_repo_read_only(mock_repo)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status", [TaskStatus.SUMMARIZING.value, TaskStatus.FAILED.value]
)
async def test_summary_stream_ignores_stored_summary_when_not_done(
    client: AsyncClient,
    mock_repo: MagicMock,
    mock_summarizer: MagicMock,
    sample_recording: Recording,
    status: str,
):
    """非 ``done`` 状态即使库里有摘要也不复用：与详情接口「仅 done 暴露结果」一致。"""
    task = Task(
        id=uuid4(),
        recording_id=sample_recording.id,
        status=status,
        transcript="hello transcript",
        summary={"summary": "中途残留的摘要", "key_points": [], "todos": []},
    )
    mock_repo.get_by_id = AsyncMock(return_value=(sample_recording, task))
    mock_summarizer.summarize_stream = MagicMock(
        side_effect=lambda transcript: _fake_stream(transcript)
    )

    resp = await client.get(f"/v1/recordings/{sample_recording.id}/summary/stream")

    assert resp.status_code == 200
    mock_summarizer.summarize_stream.assert_called_once()
    assert f"data: {json.dumps(_DONE_PAYLOAD, ensure_ascii=False)}" in resp.text
    assert "中途残留的摘要" not in resp.text
    _assert_repo_read_only(mock_repo)


@pytest.mark.asyncio
async def test_summary_stream_replay_chunks_cover_exact_json(
    client: AsyncClient,
    mock_repo: MagicMock,
    sample_recording: Recording,
):
    """回放按固定分片切分，客户端拼接分片必须还原出完整 JSON。"""
    stored = {
        "summary": "一段足够长的摘要文本用于跨多个分片" * 2,
        "key_points": ["k1", "k2"],
        "todos": ["t1"],
    }
    task = _done_task(sample_recording)
    task.summary = stored
    mock_repo.get_by_id = AsyncMock(return_value=(sample_recording, task))

    resp = await client.get(f"/v1/recordings/{sample_recording.id}/summary/stream")

    assert resp.status_code == 200
    deltas = [
        json.loads(line[len("data: ") :])["text"]
        for line in resp.text.splitlines()
        if line.startswith("data: ") and '"text"' in line
    ]
    expected = json.dumps(stored, ensure_ascii=False)
    assert len(deltas) > 1
    assert "".join(deltas) == expected
    assert all(len(d) == _REPLAY_CHUNK_SIZE for d in deltas[:-1])
    assert f"data: {expected}" in resp.text


class _DisconnectingRequest:
    """模拟客户端：第 ``disconnect_after`` 次探测之后报告已断开。"""

    def __init__(self, disconnect_after: int) -> None:
        self._calls = 0
        self._after = disconnect_after

    async def is_disconnected(self) -> bool:
        self._calls += 1
        return self._calls > self._after


@pytest.mark.asyncio
async def test_summary_stream_stops_emitting_on_client_disconnect(
    mock_repo: MagicMock,
    sample_recording: Recording,
    recording_service: RecordingService,
):
    """客户端断开后不再继续消费 LLM 流（避免为没人接收的响应继续计费）。"""
    task = _done_task(sample_recording)
    mock_repo.get_by_id = AsyncMock(return_value=(sample_recording, task))
    request = _DisconnectingRequest(disconnect_after=1)

    response = await stream_recording_summary(
        sample_recording.id, request, recording_service
    )
    chunks = [chunk async for chunk in response.body_iterator]

    assert len(chunks) == 1
    assert chunks[0].startswith("event: delta")
