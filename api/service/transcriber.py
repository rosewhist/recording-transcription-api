"""转写服务（P0 使用 Mock ASR；后续可替换为真实提供商）。"""
from __future__ import annotations

import asyncio
import hashlib
import random
import time
from pathlib import Path
from typing import Optional, Union
from uuid import UUID

from api.core.logger import get_logger
from api.service.mock_transcripts import TRANSCRIPT_TEMPLATES

logger = get_logger(__name__)

TaskId = Union[UUID, str]


def _pick_transcript(*, file_path: str, task_id: Optional[TaskId]) -> str:
    """按 task_id+file_path 稳定选择模板，保证同任务重试文案一致。"""
    seed = f"{task_id or ''}:{file_path}".encode("utf-8")
    idx = int(hashlib.sha256(seed).hexdigest(), 16) % len(TRANSCRIPT_TEMPLATES)
    body = TRANSCRIPT_TEMPLATES[idx]
    return f"【模拟转写】{body}（来源: {Path(file_path).name}）"


class TranscriberService:
    """根据音频文件路径生成转写文本。

    当前为 need.txt 要求的 Mock ASR：
    随机耗时 5~15 秒，约 20% 失败率；文案见 ``mock_transcripts``。
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
        """对 ``file_path`` 返回转写文本（Mock）。"""
        t0 = time.perf_counter()
        await asyncio.sleep(
            random.uniform(self.min_delay_seconds, self.max_delay_seconds)
        )
        if random.random() < self.fail_rate:
            raise RuntimeError(
                f"ASR Mock 失败 (约 {int(self.fail_rate * 100)}% 概率)"
            )

        transcript = _pick_transcript(file_path=file_path, task_id=task_id)
        elapsed = time.perf_counter() - t0
        logger.info(
            "[task=%s] ASR 转写完成，file=%s，文本长度: %d 字，耗时: %.1fs",
            task_id if task_id is not None else "-",
            file_path,
            len(transcript),
            elapsed,
        )
        return transcript
