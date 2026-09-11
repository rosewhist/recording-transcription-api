"""Thin service wrapper around ``LLMSummarizer`` for workers and business logic."""
from __future__ import annotations

from typing import Any, Optional

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

    async def summarize(self, transcript: str) -> dict[str, Any]:
        """Return summary as a plain dict."""
        return await self.summarizer.summarize(transcript)

    async def summarize_model(self, transcript: str) -> SummaryResult:
        """Return a validated ``SummaryResult`` model."""
        return await self.summarizer.summarize_model(transcript)

    async def aclose(self) -> None:
        """Close the underlying HTTP client if this service owns it."""
        if self._summarizer is not None and self._owns_summarizer:
            await self._summarizer.aclose()
            self._summarizer = None
