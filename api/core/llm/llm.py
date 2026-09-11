"""LLM-backed meeting/recording summarizer (OpenAI-compatible APIs)."""
from __future__ import annotations

import asyncio
import json
import re
from typing import Any, AsyncIterator, Optional

from openai import (
    APIConnectionError,
    APIStatusError,
    APITimeoutError,
    AsyncOpenAI,
    RateLimitError,
)
from pydantic import BaseModel, Field, ValidationError

from api.core.logger import get_logger

logger = get_logger(__name__)


class SummaryResult(BaseModel):
    """Structured summary payload returned to callers."""

    summary: str = Field(..., description="One-sentence summary")
    key_points: list[str] = Field(default_factory=list, description="Key points")
    todos: list[str] = Field(default_factory=list, description="Action items")


class LLMParseError(ValueError):
    """Raised when the model output is empty, truncated, or not valid JSON.

    Treated as retriable by ``summarize_model``.
    """


def _is_retriable_api_status(exc: APIStatusError) -> bool:
    """Return True for transient HTTP failures (5xx and 429)."""
    return exc.status_code >= 500 or exc.status_code == 429


class LLMSummarizer:
    """Async summarizer with JSON enforcement, validation, and bounded retries.

    Features:
      1. Prefer ``response_format=json_object``, then Pydantic validation
      2. Exponential backoff for network / rate-limit / 5xx / parse errors
      3. Three-layer JSON extraction fallback
      4. SDK retries disabled to avoid stacking with this class

    Prefer one instance per worker process to reuse the HTTP connection pool.
    """

    def __init__(
        self,
        api_key: str,
        base_url: str,
        model: str = "deepseek-chat",
        timeout: float = 30.0,
        max_tokens: int = 512,
        max_retries: int = 3,
        base_backoff: float = 1.0,
        client: Optional[AsyncOpenAI] = None,
    ) -> None:
        if max_retries < 1:
            raise ValueError("max_retries must be >= 1")

        # Disable SDK retries; this class owns retry policy.
        self.client = client or AsyncOpenAI(
            api_key=api_key,
            base_url=base_url,
            timeout=timeout,
            max_retries=0,
        )
        self._owns_client = client is None
        self.model = model
        self.max_tokens = max_tokens
        self.max_retries = max_retries
        self.base_backoff = base_backoff

    @classmethod
    def from_settings(cls, settings: Any = None) -> "LLMSummarizer":
        """Build an instance from application Settings (loads .env via Settings)."""
        if settings is None:
            from api.config.settings import get_settings

            settings = get_settings()
        if not settings.LLM_API_KEY:
            raise ValueError("LLM_API_KEY is not configured")
        return cls(
            api_key=settings.LLM_API_KEY,
            base_url=settings.LLM_BASE_URL,
            model=settings.LLM_MODEL,
            timeout=settings.LLM_TIMEOUT_SECONDS,
            max_tokens=settings.LLM_MAX_TOKENS,
            max_retries=getattr(settings, "LLM_MAX_RETRIES", 3),
        )

    async def aclose(self) -> None:
        """Close the client only if this instance created it."""
        if self._owns_client:
            await self.client.close()

    async def summarize(self, transcript: str) -> dict[str, Any]:
        """Return a plain dict summary (``SummaryResult.model_dump()``)."""
        result = await self.summarize_model(transcript)
        return result.model_dump()

    async def summarize_model(self, transcript: str) -> SummaryResult:
        """Call the LLM and return a validated ``SummaryResult``."""
        if not transcript or not transcript.strip():
            raise ValueError("transcript must not be empty")

        # Prompts stay Chinese: product targets Chinese meeting transcripts.
        system_prompt, user_prompt = self._summary_prompts(transcript)

        last_exception: Optional[BaseException] = None

        for attempt in range(1, self.max_retries + 1):
            try:
                raw_content = await self._call_llm(system_prompt, user_prompt)
                result_dict = self._parse_json_safely(raw_content)
                validated = SummaryResult.model_validate(result_dict)
                logger.debug(
                    "LLM summarize ok attempt=%d chars=%d",
                    attempt,
                    len(raw_content),
                )
                return validated

            except (LLMParseError, ValidationError) as e:
                last_exception = e
                logger.warning(
                    "LLM summarize attempt %d/%d failed (parse/validate): %s",
                    attempt,
                    self.max_retries,
                    e,
                )
                if attempt < self.max_retries:
                    await asyncio.sleep(self._backoff_delay(attempt))

            except RateLimitError as e:
                last_exception = e
                delay = self._rate_limit_delay(e, attempt)
                logger.warning(
                    "LLM summarize attempt %d/%d rate-limited, sleep %.1fs: %s",
                    attempt,
                    self.max_retries,
                    delay,
                    e,
                )
                if attempt < self.max_retries:
                    await asyncio.sleep(delay)

            except (APITimeoutError, APIConnectionError) as e:
                last_exception = e
                logger.warning(
                    "LLM summarize attempt %d/%d failed: %s",
                    attempt,
                    self.max_retries,
                    e,
                )
                if attempt < self.max_retries:
                    await asyncio.sleep(self._backoff_delay(attempt))

            except APIStatusError as e:
                if not _is_retriable_api_status(e):
                    logger.exception("LLM summarize non-retriable HTTP error")
                    raise RuntimeError(f"LLM summarize failed: {e}") from e
                last_exception = e
                logger.warning(
                    "LLM summarize attempt %d/%d failed (HTTP %s): %s",
                    attempt,
                    self.max_retries,
                    e.status_code,
                    e,
                )
                if attempt < self.max_retries:
                    await asyncio.sleep(self._backoff_delay(attempt))

            except Exception as e:  # noqa: BLE001
                logger.exception("LLM summarize unexpected non-retriable error")
                raise RuntimeError(f"LLM summarize failed: {e}") from e

        logger.error("LLM summarize retries exhausted")
        raise RuntimeError(
            f"LLM summarize failed after {self.max_retries} retries: {last_exception}"
        ) from last_exception

    async def summarize_stream(
        self, transcript: str
    ) -> AsyncIterator[tuple[str, Any]]:
        """Stream summary generation as (event, payload) pairs.

        Yields:
          - ``("delta", text_chunk)``
          - ``("done", summary_dict)`` on success
          - ``("error", message)`` on failure (then stops)
        """
        if not transcript or not transcript.strip():
            yield ("error", "transcript must not be empty")
            return

        system_prompt, user_prompt = self._summary_prompts(transcript)
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        kwargs: dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": 0.1,
            "max_tokens": self.max_tokens,
            "stream": True,
        }

        parts: list[str] = []
        try:
            stream = await self.client.chat.completions.create(**kwargs)
            async for chunk in stream:
                if not chunk.choices:
                    continue
                delta = chunk.choices[0].delta
                text = getattr(delta, "content", None) or ""
                if not text:
                    continue
                parts.append(text)
                yield ("delta", text)
        except Exception as e:  # noqa: BLE001
            logger.exception("LLM summarize_stream failed")
            yield ("error", f"LLM stream failed: {e}")
            return

        raw = "".join(parts).strip()
        if not raw:
            yield ("error", "LLM returned empty stream content")
            return
        try:
            result_dict = self._parse_json_safely(raw)
            validated = SummaryResult.model_validate(result_dict)
            yield ("done", validated.model_dump())
        except (LLMParseError, ValidationError) as e:
            yield ("error", f"Failed to parse streamed summary: {e}")

    @staticmethod
    def _summary_prompts(transcript: str) -> tuple[str, str]:
        system_prompt = (
            "你是一个专业的会议/录音摘要助手。"
            "你必须以纯 JSON 格式返回结果，不要包含任何其他文字（如 Markdown 代码块）。"
            "JSON 结构必须严格遵循："
            '{"summary": "一句话摘要", "key_points": ["要点1", "要点2"], "todos": ["待办1"]}'
        )
        user_prompt = f"请对以下录音转写文本进行摘要和待办提取：\n\n{transcript}"
        return system_prompt, user_prompt

    @staticmethod
    async def mock_summarize_stream(
        *,
        chunk_size: int = 8,
        delay_seconds: float = 0.02,
    ) -> AsyncIterator[tuple[str, Any]]:
        """Pseudo-stream a fixed JSON summary (for WORKER_ALLOW_MOCK_LLM)."""
        payload = {
            "summary": "模拟摘要",
            "key_points": ["要点1"],
            "todos": ["待办1"],
        }
        raw = json.dumps(payload, ensure_ascii=False)
        for i in range(0, len(raw), chunk_size):
            piece = raw[i : i + chunk_size]
            yield ("delta", piece)
            if delay_seconds > 0:
                await asyncio.sleep(delay_seconds)
        yield ("done", payload)

    def _backoff_delay(self, attempt: int) -> float:
        """Exponential backoff: base, 2*base, 4*base, ..."""
        return self.base_backoff * (2 ** (attempt - 1))

    def _rate_limit_delay(self, exc: RateLimitError, attempt: int) -> float:
        """Prefer Retry-After header; otherwise fall back to exponential backoff."""
        header_delay: Optional[float] = None
        response = getattr(exc, "response", None)
        if response is not None:
            raw = response.headers.get("retry-after") or response.headers.get(
                "Retry-After"
            )
            if raw:
                try:
                    header_delay = float(raw)
                except ValueError:
                    header_delay = None
        return header_delay if header_delay is not None else self._backoff_delay(attempt)

    async def _call_llm(self, system_prompt: str, user_prompt: str) -> str:
        """Invoke chat.completions; fall back if ``response_format`` is unsupported."""
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        kwargs: dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": 0.1,
            "max_tokens": self.max_tokens,
        }

        try:
            response = await self.client.chat.completions.create(
                **kwargs,
                response_format={"type": "json_object"},
            )
        except APIStatusError as e:
            # Some gateways reject response_format with HTTP 400; retry once without it.
            if e.status_code == 400:
                logger.warning(
                    "Gateway may not support response_format=json_object; "
                    "retrying without it: %s",
                    e,
                )
                response = await self.client.chat.completions.create(**kwargs)
            else:
                raise

        if not response.choices:
            raise LLMParseError(f"LLM returned empty choices: {response!r}")

        choice = response.choices[0]
        if getattr(choice, "finish_reason", None) == "length":
            raise LLMParseError(
                "LLM output truncated by max_tokens "
                f"(finish_reason=length): {(choice.message.content or '')[:200]!r}"
            )

        raw = choice.message.content
        if not raw or not raw.strip():
            raise LLMParseError(f"LLM returned empty content: {choice!r}")
        return raw.strip()

    @staticmethod
    def _parse_json_safely(raw_text: str) -> dict[str, Any]:
        """Extract a JSON object with three fallbacks.

        1. ``json.loads`` on the whole string
        2. Strip a Markdown `` ```json `` fence
        3. Regex from the first ``{`` to the last ``}``

        Raises ``LLMParseError`` if none succeed (retriable upstream).
        """
        try:
            data = json.loads(raw_text)
            if isinstance(data, dict):
                return data
        except json.JSONDecodeError:
            pass

        block_match = re.search(
            r"```(?:json)?\s*(.*?)\s*```", raw_text, flags=re.DOTALL
        )
        if block_match:
            try:
                data = json.loads(block_match.group(1))
                if isinstance(data, dict):
                    return data
            except json.JSONDecodeError:
                pass

        obj_match = re.search(r"\{.*\}", raw_text, flags=re.DOTALL)
        if obj_match:
            try:
                data = json.loads(obj_match.group(0))
                if isinstance(data, dict):
                    return data
            except json.JSONDecodeError:
                pass

        raise LLMParseError(
            f"Could not parse valid JSON from LLM output: {raw_text[:200]}..."
        )
