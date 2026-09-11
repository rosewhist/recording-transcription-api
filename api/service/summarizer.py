"""对 ``LLMSummarizer`` 的薄封装，供 worker 与业务层调用。"""
from __future__ import annotations

from typing import Any, AsyncIterator, Optional

from api.config.settings import Settings, get_settings
from api.core.llm import LLMSummarizer, SummaryResult


class SummarizerService:
    """按需从 Settings 构建 ``LLMSummarizer``，并对外提供摘要 API。

    建议在应用 lifespan 中传入 ``settings=...`` 以共享配置。
    仅在测试或复用已有 client 时注入 ``summarizer=...``。
    """

    def __init__(
        self,
        summarizer: Optional[LLMSummarizer] = None,
        settings: Optional[Settings] = None,
    ) -> None:
        self.settings = settings or get_settings()
        self._summarizer = summarizer
        # 仅关闭由本服务创建的 client（不关闭外部注入的）。
        self._owns_summarizer = summarizer is None

    @property
    def summarizer(self) -> LLMSummarizer:
        if self._summarizer is None:
            self._summarizer = LLMSummarizer.from_settings(self.settings)
            self._owns_summarizer = True
        return self._summarizer

    @property
    def can_stream(self) -> bool:
        """真实 LLM 或 mock 流可用时返回 True。"""
        if self._summarizer is not None or self.settings.LLM_API_KEY:
            return True
        return bool(self.settings.WORKER_ALLOW_MOCK_LLM)

    async def summarize(self, transcript: str) -> dict[str, Any]:
        """返回普通 dict 形式的摘要。"""
        return await self.summarizer.summarize(transcript)

    async def summarize_model(self, transcript: str) -> SummaryResult:
        """返回校验后的 ``SummaryResult`` 模型。"""
        return await self.summarizer.summarize_model(transcript)

    async def summarize_stream(
        self, transcript: str
    ) -> AsyncIterator[tuple[str, Any]]:
        """产出 SSE 用的 ``(事件, 载荷)``：delta / done / error。"""
        if self._summarizer is not None or self.settings.LLM_API_KEY:
            async for item in self.summarizer.summarize_stream(transcript):
                yield item
            return
        if self.settings.WORKER_ALLOW_MOCK_LLM:
            async for item in LLMSummarizer.mock_summarize_stream():
                yield item
            return
        yield ("error", "LLM is not configured")

    async def aclose(self) -> None:
        """若本服务拥有底层 HTTP client，则关闭它。"""
        if self._summarizer is not None and self._owns_summarizer:
            await self._summarizer.aclose()
            self._summarizer = None
