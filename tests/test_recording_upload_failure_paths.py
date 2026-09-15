"""上传失败与竞态路径：暂存件、孤儿文件与 .part 的清理。

这些分支都在 ``RecordingService.upload`` 的失败/竞态出口上，旧测试只覆盖了成功
路径与「上传前就命中幂等」两种情况。
"""
from __future__ import annotations

import asyncio
from io import BytesIO
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import UploadFile

from api.domain.exceptions import InvalidUploadError
from api.repository.dao.recording import Recording
from api.repository.dao.task import Task
from api.repository.dao.task_types import TaskStatus
from api.service.recording import RecordingService


def _upload(content: bytes = b"fake-audio-bytes", name: str = "demo.mp3") -> UploadFile:
    return UploadFile(file=BytesIO(content), filename=name)


def _existing() -> tuple[Recording, Task]:
    recording = Recording(
        id=uuid4(),
        file_name="demo.mp3",
        file_path="/tmp/already-there.mp3",
        file_size=12,
        file_hash="a" * 64,
    )
    task = Task(
        id=uuid4(),
        recording_id=recording.id,
        status=TaskStatus.PENDING.value,
    )
    return recording, task


def _stored_files(upload_dir) -> list[str]:
    return sorted(p.name for p in upload_dir.iterdir())


@pytest.mark.asyncio
async def test_commit_failure_removes_staging_file(
    recording_service: RecordingService,
    upload_dir,
):
    """落盘重命名失败：抛错且不残留 ``.part``（否则失败一次就留一份垃圾）。"""
    with patch("api.utils.file.os.replace", side_effect=OSError("disk full")):
        with pytest.raises(OSError):
            await recording_service.upload(_upload())

    assert _stored_files(upload_dir) == []


@pytest.mark.asyncio
async def test_cancel_during_commit_removes_staging_file(
    recording_service: RecordingService,
    upload_dir,
):
    """请求在提交阶段被取消（客户端断连）：清理后原样抛出 CancelledError。"""
    with patch("api.utils.file.os.replace", side_effect=asyncio.CancelledError):
        with pytest.raises(asyncio.CancelledError):
            await recording_service.upload(_upload())

    assert _stored_files(upload_dir) == []


@pytest.mark.asyncio
async def test_db_failure_removes_committed_file(
    recording_service: RecordingService,
    mock_repo: MagicMock,
    mock_worker: MagicMock,
    upload_dir,
):
    """文件已提交但落库异常：回滚磁盘文件，不留下无记录引用的孤儿文件。"""
    mock_repo.create_recording_and_task = AsyncMock(
        side_effect=RuntimeError("db down")
    )

    with pytest.raises(RuntimeError, match="db down"):
        await recording_service.upload(_upload())

    assert _stored_files(upload_dir) == []
    mock_worker.submit.assert_not_awaited()


@pytest.mark.asyncio
async def test_lost_idempotency_race_removes_orphan_file(
    recording_service: RecordingService,
    mock_repo: MagicMock,
    mock_worker: MagicMock,
    upload_dir,
):
    """并发同哈希竞态落败：仓储未插入新记录，本请求提交的文件必须删掉。"""
    existing_recording, existing_task = _existing()
    mock_repo.get_by_hash = AsyncMock(return_value=(None, None))  # 先查未命中
    mock_repo.create_recording_and_task = AsyncMock(
        return_value=(existing_recording, existing_task, False)  # 随后撞上唯一约束
    )

    recording, task, created = await recording_service.upload(_upload())

    assert created is False
    assert recording is existing_recording
    assert task is existing_task
    assert _stored_files(upload_dir) == []
    mock_worker.submit.assert_not_awaited()


@pytest.mark.asyncio
async def test_idempotent_hit_leaves_no_file(
    recording_service: RecordingService,
    mock_repo: MagicMock,
    mock_worker: MagicMock,
    upload_dir,
):
    """上传前命中幂等：暂存件直接丢弃，不产生任何文件。"""
    existing_recording, existing_task = _existing()
    mock_repo.get_by_hash = AsyncMock(return_value=(existing_recording, existing_task))

    recording, task, created = await recording_service.upload(_upload())

    assert created is False
    assert recording is existing_recording
    assert task is existing_task
    assert _stored_files(upload_dir) == []
    mock_repo.create_recording_and_task.assert_not_awaited()
    mock_worker.submit.assert_not_awaited()


@pytest.mark.asyncio
async def test_oversized_upload_leaves_no_part_file(
    recording_service: RecordingService,
    upload_dir,
):
    """超大文件在流式读取中被拒：同样不得残留 ``.part``。"""
    original = recording_service.settings.MAX_FILE_SIZE_MB
    recording_service.settings.MAX_FILE_SIZE_MB = 1
    try:
        with pytest.raises(InvalidUploadError, match="1MB"):
            await recording_service.upload(_upload(b"x" * (1024 * 1024 + 1)))
    finally:
        recording_service.settings.MAX_FILE_SIZE_MB = original

    assert _stored_files(upload_dir) == []


@pytest.mark.asyncio
async def test_successful_upload_keeps_exactly_one_file(
    recording_service: RecordingService,
    mock_repo: MagicMock,
    upload_dir,
):
    """成功路径保留且仅保留最终文件（无 ``.part`` 残留）。"""
    recording, task = _existing()
    mock_repo.create_recording_and_task = AsyncMock(return_value=(recording, task, True))

    _, _, created = await recording_service.upload(_upload())

    assert created is True
    stored = _stored_files(upload_dir)
    assert len(stored) == 1
    assert not stored[0].endswith(".part")
