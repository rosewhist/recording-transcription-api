from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from openai import APITimeoutError

from api.core.llm.llm import LLMParseError, LLMSummarizer, SummaryResult

_VALID = '{"summary": "一句话摘要", "key_points": ["要点 1"], "todos": ["待办 1"]}'


def _summarizer(*, max_retries: int = 3) -> LLMSummarizer:
    return LLMSummarizer(
        api_key="test",
        base_url="https://example.invalid/v1",
        model="test-model",
        max_retries=max_retries,
        base_backoff=0.0,
        client=MagicMock(),
    )


def test_parse_json_direct_object():
    data = LLMSummarizer._parse_json_safely(_VALID)
    assert data["summary"] == "一句话摘要"
    assert data["key_points"] == ["要点 1"]


def test_parse_json_from_markdown_fence():
    raw = "```json\n" + _VALID + "\n```"
    data = LLMSummarizer._parse_json_safely(raw)
    assert data["todos"] == ["待办 1"]


def test_parse_json_from_surrounding_text():
    raw = "这是模型前言\n" + _VALID + "\n结束"
    data = LLMSummarizer._parse_json_safely(raw)
    assert data["summary"] == "一句话摘要"


def test_parse_json_invalid_raises():
    with pytest.raises(LLMParseError):
        LLMSummarizer._parse_json_safely("not-json-at-all")


@pytest.mark.asyncio
async def test_summarize_model_rejects_empty_transcript():
    with pytest.raises(ValueError, match="transcript"):
        await _summarizer().summarize_model("  ")


@pytest.mark.asyncio
async def test_summarize_retries_invalid_json_then_succeeds():
    svc = _summarizer(max_retries=3)
    svc._call_llm = AsyncMock(
        side_effect=["<<<not json>>>", _VALID],
    )

    result = await svc.summarize_model("会议转写文本")

    assert isinstance(result, SummaryResult)
    assert result.summary == "一句话摘要"
    assert svc._call_llm.await_count == 2


@pytest.mark.asyncio
async def test_summarize_retries_timeout_then_succeeds():
    svc = _summarizer(max_retries=3)
    timeout = APITimeoutError(
        request=httpx.Request("POST", "https://example.invalid/v1/chat")
    )
    svc._call_llm = AsyncMock(side_effect=[timeout, _VALID])

    result = await svc.summarize_model("会议转写文本")

    assert result.summary == "一句话摘要"
    assert svc._call_llm.await_count == 2


@pytest.mark.asyncio
async def test_summarize_raises_after_retries_exhausted():
    svc = _summarizer(max_retries=2)
    svc._call_llm = AsyncMock(return_value="still not json")

    with pytest.raises(RuntimeError, match="failed after 2 retries"):
        await svc.summarize_model("会议转写文本")
    assert svc._call_llm.await_count == 2


@pytest.mark.asyncio
async def test_mock_summarize_stream_emits_delta_and_done():
    events = [
        item
        async for item in LLMSummarizer.mock_summarize_stream(
            chunk_size=16,
            delay_seconds=0,
        )
    ]
    names = [name for name, _ in events]
    assert "delta" in names
    assert names[-1] == "done"
    _, payload = events[-1]
    assert payload["summary"] == "模拟摘要"
    assert payload["key_points"] == ["要点1"]
    assert payload["todos"] == ["待办1"]
