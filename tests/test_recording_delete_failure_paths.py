"""删除录音时「DB 已删但磁盘清理失败」的分支。

该分支刻意吞掉异常（录音记录已经删了，卡在这里对调用方毫无意义），但必须留下
可检索的告警，否则磁盘上的残留文件会无声无息。
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from httpx import AsyncClient
from loguru import logger as loguru_logger


@pytest.fixture
def captured_logs():
    """挂一个 loguru sink，取回记录列表以便断言事件名。"""
    records: list[dict] = []
    sink_id = loguru_logger.add(
        lambda message: records.append(message.record), level=0, format="{message}"
    )
    try:
        yield records
    finally:
        loguru_logger.remove(sink_id)


def _events(records: list[dict]) -> list[str]:
    return [r["extra"].get("event") for r in records]


@pytest.mark.asyncio
async def test_file_cleanup_failure_still_returns_204_with_warning(
    client: AsyncClient,
    mock_repo: MagicMock,
    captured_logs: list[dict],
):
    recording_id = uuid4()
    mock_repo.delete = AsyncMock(return_value="/tmp/locked.mp3")

    with patch(
        "api.service.recording.file_utils.remove_file_async",
        new_callable=AsyncMock,
        side_effect=PermissionError("file is locked"),
    ):
        resp = await client.delete(f"/v1/recordings/{recording_id}")

    assert resp.status_code == 204
    events = _events(captured_logs)
    assert "recording.delete.file_cleanup_failed" in events
    assert "recording.deleted" not in events

    failure = next(
        r
        for r in captured_logs
        if r["extra"].get("event") == "recording.delete.file_cleanup_failed"
    )
    assert failure["extra"]["path"] == "/tmp/locked.mp3"
    assert failure["level"].name == "WARNING"


@pytest.mark.asyncio
async def test_successful_cleanup_logs_deleted(
    client: AsyncClient,
    mock_repo: MagicMock,
    captured_logs: list[dict],
):
    """对照用例：清理成功走 else 分支，事件名可区分。"""
    recording_id = uuid4()
    mock_repo.delete = AsyncMock(return_value="/tmp/demo.mp3")

    with patch(
        "api.service.recording.file_utils.remove_file_async",
        new_callable=AsyncMock,
    ):
        resp = await client.delete(f"/v1/recordings/{recording_id}")

    assert resp.status_code == 204
    events = _events(captured_logs)
    assert "recording.deleted" in events
    assert "recording.delete.file_cleanup_failed" not in events
