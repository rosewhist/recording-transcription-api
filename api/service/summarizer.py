"""Thin service wrapper around ``LLMSummarizer`` for workers and business logic."""
from __future__ import annotations

from typing import Any, AsyncIterator, Optional

from api.config.settings import Settings, get_settings
from api.core.llm import LLMSummarizer, SummaryResult


class SummarizerService:
    """Lazy-build an ``LLMSummarizer`` from Settings and expose summarize APIs.

    Prefer constructing with ``settings=...`` from app lifespan so config is
    shared. Inject ``summarizer=...`` only in tests or when reusing a client.
    """

    def __init__(
        self,
        summarizer: Optional[LLMSummarizer] = None,
        settings: Optional[Settings] = None,
    ) -> None:
        self.settings = settings or get_settings()
        self._summarizer = summarizer
        # Only close clients this service created (not injected ones).
        self._owns_summarizer = summarizer is None

    @property
    def summarizer(self) -> LLMSummarizer:
        if self._summarizer is None:
            self._summarizer = LLMSummarizer.from_settings(self.settings)
            self._owns_summarizer = True
        return self._summarizer

    @property
    def can_stream(self) -> bool:
        """True if real LLM or mock stream is available."""
        if self._summarizer is not None or self.settings.LLM_API_KEY:
            return True
        return bool(self.settings.WORKER_ALLOW_MOCK_LLM)

    async def summarize(self, transcript: str) -> dict[str, Any]:
        """Return summary as a plain dict."""
        return await self.summarizer.summarize(transcript)

    async def summarize_model(self, transcript: str) -> SummaryResult:
        """Return a validated ``SummaryResult`` model."""
        return await self.summarizer.summarize_model(transcript)

    async def summarize_stream(
        self, transcript: str
    ) -> AsyncIterator[tuple[str, Any]]:
        """Yield (event, payload) for SSE: delta / done / error."""
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
        """Close the underlying HTTP client if this service owns it."""
        if self._summarizer is not None and self._owns_summarizer:
            await self._summarizer.aclose()
            self._summarizer = None
