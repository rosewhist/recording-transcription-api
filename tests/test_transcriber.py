from __future__ import annotations

import pytest

from api.service.mock_transcripts import TRANSCRIPT_TEMPLATES
from api.service.transcriber import TranscriberService, _pick_transcript


def test_transcriber_rejects_invalid_delay_range():
    with pytest.raises(ValueError, match="delay"):
        TranscriberService(min_delay_seconds=2, max_delay_seconds=1)


def test_transcriber_rejects_invalid_fail_rate():
    with pytest.raises(ValueError, match="fail_rate"):
        TranscriberService(fail_rate=1.5)


def test_transcript_template_pool_has_fifty_unique_entries():
    assert len(TRANSCRIPT_TEMPLATES) == 50
    assert len(set(TRANSCRIPT_TEMPLATES)) == 50


def test_pick_transcript_stable_for_same_seed():
    a = _pick_transcript(file_path="/tmp/demo.wav", task_id="t1")
    b = _pick_transcript(file_path="/tmp/demo.wav", task_id="t1")
    assert a == b
    assert "模拟转写" in a
    assert "demo.wav" in a


def test_pick_transcript_differs_across_paths_or_tasks():
    base = _pick_transcript(file_path="/tmp/a.wav", task_id="t1")
    by_path = _pick_transcript(file_path="/tmp/b.wav", task_id="t1")
    by_task = _pick_transcript(file_path="/tmp/a.wav", task_id="t2")
    assert base != by_path
    assert base != by_task


@pytest.mark.asyncio
async def test_transcribe_success_without_waiting():
    svc = TranscriberService(
        min_delay_seconds=0,
        max_delay_seconds=0,
        fail_rate=0.0,
    )
    text = await svc.transcribe(file_path="/tmp/demo.wav", task_id="t1")
    assert "模拟转写" in text
    assert "demo.wav" in text
    assert text == _pick_transcript(file_path="/tmp/demo.wav", task_id="t1")


@pytest.mark.asyncio
async def test_transcribe_always_fails_when_fail_rate_is_one():
    svc = TranscriberService(
        min_delay_seconds=0,
        max_delay_seconds=0,
        fail_rate=1.0,
    )
    with pytest.raises(RuntimeError, match="ASR Mock 失败"):
        await svc.transcribe(file_path="/tmp/demo.wav", task_id="t1")
