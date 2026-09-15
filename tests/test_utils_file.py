from __future__ import annotations

import asyncio
import hashlib
from io import BytesIO
from pathlib import Path

import pytest
from fastapi import UploadFile

from api.utils.file import (
    stage_upload,
    validate_extension,
)
from api.domain.exceptions import InvalidUploadError


def _upload(content: bytes, name: str = "demo.mp3") -> UploadFile:
    return UploadFile(file=BytesIO(content), filename=name)


def test_validate_extension_accepts_and_normalizes():
    assert validate_extension("Demo.MP3", {".mp3"}) == ".mp3"


@pytest.mark.parametrize("name", ["", "notes.txt", "noext"])
def test_validate_extension_rejects_bad_names(name: str):
    with pytest.raises(InvalidUploadError):
        validate_extension(name, {".mp3"})


@pytest.mark.asyncio
async def test_stage_upload_hashes_across_chunks(tmp_path: Path):
    content = b"abcdefghij" * 5  # 50 bytes
    staged = await stage_upload(
        _upload(content),
        ext=".mp3",
        upload_dir=tmp_path,
        max_size_bytes=1000,
        max_size_mb=1,
        chunk_size=7,  # 强制多轮读取
    )

    assert staged.size == len(content)
    assert staged.sha256 == hashlib.sha256(content).hexdigest()
    # 未 commit 前最终路径不存在
    assert not staged.final_path.exists()

    final = Path(staged.commit())
    assert final.parent == tmp_path
    assert final.read_bytes() == content
    assert not staged.tmp_path.exists()


@pytest.mark.asyncio
async def test_stage_upload_discard_leaves_no_file(tmp_path: Path):
    staged = await stage_upload(
        _upload(b"abc"),
        ext=".mp3",
        upload_dir=tmp_path,
        max_size_bytes=100,
        max_size_mb=1,
    )
    assert list(tmp_path.iterdir())  # .part 存在

    await asyncio.to_thread(staged.discard)

    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_stage_upload_rejects_oversized_and_cleans_up(tmp_path: Path):
    with pytest.raises(InvalidUploadError):
        await stage_upload(
            _upload(b"x" * 50),
            ext=".mp3",
            upload_dir=tmp_path,
            max_size_bytes=10,
            max_size_mb=1,
            chunk_size=8,
        )

    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_stage_upload_rejects_empty_and_cleans_up(tmp_path: Path):
    with pytest.raises(InvalidUploadError):
        await stage_upload(
            _upload(b""),
            ext=".mp3",
            upload_dir=tmp_path,
            max_size_bytes=10,
            max_size_mb=1,
        )

    assert list(tmp_path.iterdir()) == []


class _CancellingUpload:
    """首轮返回数据，随后抛 ``CancelledError``（模拟客户端中途断连）。"""

    def __init__(self, first: bytes) -> None:
        self._first = first
        self.calls = 0

    async def read(self, size: int = -1) -> bytes:
        self.calls += 1
        if self.calls == 1:
            return self._first
        raise asyncio.CancelledError


@pytest.mark.asyncio
async def test_stage_upload_cancelled_midway_cleans_up_part_file(tmp_path: Path):
    """取消（而非普通异常）也必须清理 ``.part``：断连是这里最常见的失败。"""
    upload = _CancellingUpload(b"x" * 8)

    with pytest.raises(asyncio.CancelledError):
        await stage_upload(
            upload,
            ext=".mp3",
            upload_dir=tmp_path,
            max_size_bytes=1000,
            max_size_mb=1,
            chunk_size=8,
        )

    assert upload.calls == 2  # 确实是在写入之后被取消的
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_discard_is_idempotent(tmp_path: Path):
    """``discard`` 可重复调用（删除是 cleanup 路径，不该因文件已删而报错）。"""
    staged = await stage_upload(
        _upload(b"abc"),
        ext=".mp3",
        upload_dir=tmp_path,
        max_size_bytes=100,
        max_size_mb=1,
    )

    await asyncio.to_thread(staged.discard)
    await asyncio.to_thread(staged.discard)

    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_discard_after_commit_keeps_committed_file(tmp_path: Path):
    """commit 之后 discard 是空操作，不能误删已经对外可见的最终文件。"""
    staged = await stage_upload(
        _upload(b"abc"),
        ext=".mp3",
        upload_dir=tmp_path,
        max_size_bytes=100,
        max_size_mb=1,
    )
    final = Path(staged.commit())

    await asyncio.to_thread(staged.discard)

    assert final.exists()
    assert final.read_bytes() == b"abc"
