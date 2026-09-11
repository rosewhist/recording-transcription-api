"""上传文件读写等工具（后续实现）。"""
import asyncio
import hashlib
import os
from pathlib import Path
from typing import Union
from uuid import uuid4
from fastapi import UploadFile
from api.domain.exceptions import InvalidUploadError


async def validate_upload(
        upload_file: UploadFile,
        *,
        allowed_ext: set[str],
        max_size_bytes: int,
        max_size_mb: int,
) -> tuple[str, str, bytes]:
    """校验上传文件：扩展名、非空、大小上限。返回 (filename, ext, content)。"""
    filename = upload_file.filename or ""
    ext = ext_of(filename)
    if not filename or ext not in allowed_ext:
        raise InvalidUploadError(
            f"仅支持音频扩展名 {sorted(allowed_ext)}，收到: {filename or '(空)'}"
        )

    # 限制读取量，超限立即拒绝（避免超大文件占用内存）
    content = await upload_file.read(max_size_bytes + 1)
    if not content:
        raise InvalidUploadError("文件内容为空")
    if len(content) > max_size_bytes:
        raise InvalidUploadError(f"文件大小不能超过 {max_size_mb}MB")
    return filename, ext, content


def ext_of(filename: str) -> str:
    """返回小写扩展名（含点），如 'demo.MP3' -> '.mp3'；无扩展名返回 ''。"""
    return Path(filename or "").suffix.lower()


def compute_sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def save_bytes(content: bytes, ext: str, upload_dir: Path) -> str:
    """原子写入文件，返回绝对路径字符串。"""
    upload_dir.mkdir(parents=True, exist_ok=True)
    path = upload_dir / f"{uuid4().hex}{ext}"
    tmp = path.with_name(path.name + ".part")
    tmp.write_bytes(content)
    os.replace(tmp, path)
    return str(path)


def remove_file(path: Union[str, Path]) -> None:
    try:
        Path(path).unlink(missing_ok=True)
    except OSError:  # pragma: no cover
        pass


async def save_bytes_async(content: bytes, ext: str, upload_dir: Path) -> str:
    return await asyncio.to_thread(save_bytes, content, ext, upload_dir)


async def remove_file_async(path: Union[str, Path]) -> None:
    await asyncio.to_thread(remove_file, path)
