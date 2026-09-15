"""上传文件校验、分块落盘与增量哈希。"""
from __future__ import annotations

import asyncio
import hashlib
import os
from pathlib import Path
from typing import Union
from uuid import uuid4

from fastapi import UploadFile

from api.domain.exceptions import InvalidUploadError

# 1 MiB / read：无论上限多高，进程峰值内存都被限制在一个 chunk 量级。
_UPLOAD_CHUNK_SIZE = 1024 * 1024


def ext_of(filename: str) -> str:
    """返回小写扩展名（含点），如 'demo.MP3' -> '.mp3'；无扩展名返回 ''。"""
    return Path(filename or "").suffix.lower()


def validate_extension(filename: str, allowed_ext: set[str]) -> str:
    """校验扩展名并返回规范化 ext；不合法抛 ``InvalidUploadError``。"""
    ext = ext_of(filename)
    if not filename or ext not in allowed_ext:
        raise InvalidUploadError(
            f"仅支持音频扩展名 {sorted(allowed_ext)}，收到: {filename or '(空)'}"
        )
    return ext


class _ChunkSink:
    """增量写盘 + 增量哈希。方法均为阻塞 I/O，需放在线程中调用。"""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._fh = open(path, "wb")
        self._hash = hashlib.sha256()
        self.size = 0

    def write(self, chunk: bytes) -> None:
        self._fh.write(chunk)
        self._hash.update(chunk)
        self.size += len(chunk)

    def finalize(self) -> str:
        self._fh.flush()
        self._fh.close()
        return self._hash.hexdigest()

    def abort(self) -> None:
        try:
            self._fh.close()
        except OSError:  # pragma: no cover
            pass
        remove_file(self.path)


class StagedUpload:
    """已落盘并算好 SHA-256 的上传暂存件（位于 ``upload_dir`` 的 ``.part``）。

    未 commit 前不出现在最终路径；重复上传可直接 ``discard``，不产生垃圾文件。
    """

    def __init__(
        self, tmp_path: Path, final_path: Path, sha256: str, size: int
    ) -> None:
        self.tmp_path = tmp_path
        self.final_path = final_path
        self.sha256 = sha256
        self.size = size

    def commit(self) -> str:
        """原子重命名为最终路径，返回绝对路径字符串。"""
        os.replace(self.tmp_path, self.final_path)
        return str(self.final_path)

    def discard(self) -> None:
        """删除暂存文件（幂等：commit 后再调用为空操作）。"""
        remove_file(self.tmp_path)


async def stage_upload(
    upload_file: UploadFile,
    *,
    ext: str,
    upload_dir: Path,
    max_size_bytes: int,
    max_size_mb: int,
    chunk_size: int = _UPLOAD_CHUNK_SIZE,
) -> StagedUpload:
    """分块读取 ``upload_file`` 落盘并计算 SHA-256，阻塞 I/O 走 ``to_thread``。

    返回尚未 commit 的 ``StagedUpload``；内容为空或超限抛 ``InvalidUploadError``。
    失败路径会清理已写入的 ``.part`` 文件。
    """
    upload_dir.mkdir(parents=True, exist_ok=True)
    final_path = upload_dir / f"{uuid4().hex}{ext}"
    tmp_path = final_path.with_name(final_path.name + ".part")

    sink = await asyncio.to_thread(_ChunkSink, tmp_path)
    try:
        while True:
            chunk = await upload_file.read(chunk_size)
            if not chunk:
                break
            if sink.size + len(chunk) > max_size_bytes:
                raise InvalidUploadError(f"文件大小不能超过 {max_size_mb}MB")
            await asyncio.to_thread(sink.write, chunk)
        if sink.size == 0:
            raise InvalidUploadError("文件内容为空")
        digest = await asyncio.to_thread(sink.finalize)
    except BaseException:  # 含 CancelledError：客户端断连/取消时也要清理 .part 文件
        await asyncio.to_thread(sink.abort)
        raise
    return StagedUpload(tmp_path, final_path, digest, sink.size)


def remove_file(path: Union[str, Path]) -> None:
    try:
        Path(path).unlink(missing_ok=True)
    except OSError:  # pragma: no cover
        pass


async def remove_file_async(path: Union[str, Path]) -> None:
    await asyncio.to_thread(remove_file, path)
