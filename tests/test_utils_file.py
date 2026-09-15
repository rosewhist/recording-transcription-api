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
