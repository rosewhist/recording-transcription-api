"""基于 LLM 的会议/录音摘要器（兼容 OpenAI API）。"""
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
    """返回给调用方的结构化摘要结果。"""

    summary: str = Field(..., description="一句话摘要")
    key_points: list[str] = Field(default_factory=list, description="要点列表")
    todos: list[str] = Field(default_factory=list, description="待办事项")


class LLMParseError(ValueError):
    """模型输出为空、被截断或不是合法 JSON 时抛出。

    在 ``summarize_model`` 中视为可重试错误。
    """


def _is_retriable_api_status(exc: APIStatusError) -> bool:
    """判断是否为可重试的瞬时 HTTP 失败（5xx 与 429）。"""
    return exc.status_code >= 500 or exc.status_code == 429


class LLMSummarizer:
    """异步摘要器：强制 JSON、结果校验，并带有限次重试。

    能力：
      1. 优先使用 ``response_format=json_object``，再用 Pydantic 校验
      2. 对网络 / 限流 / 5xx / 解析错误做指数退避
      3. 三层 JSON 提取兜底
      4. 关闭 SDK 自带重试，避免与本类策略叠加

    建议每个 worker 进程共用一个实例，以便复用 HTTP 连接池。
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

        # 关闭 SDK 重试，由本类自行控制重试策略。
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
        """从应用 Settings 构建实例（通过 Settings 加载 .env）。"""
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
        """仅在本实例创建了 client 时关闭连接。"""
        if self._owns_client:
            await self.client.close()

    async def summarize(self, transcript: str) -> dict[str, Any]:
        """返回普通 dict 形式的摘要（``SummaryResult.model_dump()``）。"""
        result = await self.summarize_model(transcript)
        return result.model_dump()

    async def summarize_model(self, transcript: str) -> SummaryResult:
        """调用 LLM 并返回校验后的 ``SummaryResult``。"""
        if not transcript or not transcript.strip():
            raise ValueError("transcript must not be empty")

        # 提示词保持中文：产品面向中文会议转写文本。
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
        """以流式方式生成摘要，产出 ``(事件名, 载荷)``。

        产出：
          - ``("delta", 文本片段)``
          - 成功时 ``("done", 摘要字典)``
          - 失败时 ``("error", 错误信息)``（随后结束）
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
        """伪流式输出固定 JSON 摘要（供 WORKER_ALLOW_MOCK_LLM 使用）。"""
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
        """指数退避：base、2*base、4*base……"""
        return self.base_backoff * (2 ** (attempt - 1))

    def _rate_limit_delay(self, exc: RateLimitError, attempt: int) -> float:
        """优先使用 Retry-After 响应头；否则回退到指数退避。"""
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
        """调用 chat.completions；若网关不支持 ``response_format`` 则降级重试。"""
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
            # 部分网关会以 HTTP 400 拒绝 response_format；去掉后再试一次。
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
        """用三层兜底从模型输出中提取 JSON 对象。

        1. 对整段文本直接 ``json.loads``
        2. 去掉 Markdown 的 `` ```json `` 代码块围栏后再解析
        3. 用正则截取第一个 ``{`` 到最后一个 ``}``

        全部失败时抛出 ``LLMParseError``（上层可重试）。
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
