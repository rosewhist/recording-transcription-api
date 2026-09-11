"""Transcription service (Mock ASR for P0; swap for a real provider later)."""
from __future__ import annotations

import asyncio
import random
import time
from typing import Optional, Union
from uuid import UUID

from api.core.logger import get_logger

logger = get_logger(__name__)

TaskId = Union[UUID, str]


class TranscriberService:
    """Produce a transcript from an audio file path.

    Current implementation is Mock ASR per need.txt:
    random 5~15s latency and ~20% failure rate.
    """

    def __init__(
        self,
        *,
        min_delay_seconds: float = 5.0,
        max_delay_seconds: float = 15.0,
        fail_rate: float = 0.2,
    ) -> None:
        if min_delay_seconds < 0 or max_delay_seconds < min_delay_seconds:
            raise ValueError("invalid ASR delay range")
        if not 0.0 <= fail_rate <= 1.0:
            raise ValueError("fail_rate must be in [0, 1]")
        self.min_delay_seconds = min_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        self.fail_rate = fail_rate

    async def transcribe(
        self,
        *,
        file_path: str,
        task_id: Optional[TaskId] = None,
    ) -> str:
        """Return transcript text for ``file_path`` (Mock)."""
        t0 = time.perf_counter()
        await asyncio.sleep(
            random.uniform(self.min_delay_seconds, self.max_delay_seconds)
        )
        if random.random() < self.fail_rate:
            raise RuntimeError(
                f"ASR Mock 失败 (约 {int(self.fail_rate * 100)}% 概率)"
            )

        transcript = (
            f"【模拟转写】录音文件 {file_path}。"
            "会议讨论了项目进度、风险与下一步待办，参会人确认了时间节点。"
        )
        elapsed = time.perf_counter() - t0
        logger.info(
            "[task=%s] ASR 转写完成，file=%s，文本长度: %d 字，耗时: %.1fs",
            task_id if task_id is not None else "-",
            file_path,
            len(transcript),
            elapsed,
        )
        return transcript
