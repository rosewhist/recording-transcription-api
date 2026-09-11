from __future__ import annotations

import pytest

from api.service.transcriber import TranscriberService


def test_transcriber_rejects_invalid_delay_range():
    with pytest.raises(ValueError, match="delay"):
        TranscriberService(min_delay_seconds=2, max_delay_seconds=1)


def test_transcriber_rejects_invalid_fail_rate():
    with pytest.raises(ValueError, match="fail_rate"):
        TranscriberService(fail_rate=1.5)


@pytest.mark.asyncio
async def test_transcribe_success_without_waiting():
    svc = TranscriberService(
        min_delay_seconds=0,
        max_delay_seconds=0,
        fail_rate=0.0,
    )
    text = await svc.transcribe(file_path="/tmp/demo.wav", task_id="t1")
    assert "模拟转写" in text
    assert "/tmp/demo.wav" in text


@pytest.mark.asyncio
async def test_transcribe_always_fails_when_fail_rate_is_one():
    svc = TranscriberService(
        min_delay_seconds=0,
        max_delay_seconds=0,
        fail_rate=1.0,
    )
    with pytest.raises(RuntimeError, match="ASR Mock 失败"):
        await svc.transcribe(file_path="/tmp/demo.wav", task_id="t1")
