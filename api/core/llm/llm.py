from typing import AsyncIterator, Optional

import httpx
import json


class LLMError(Exception):
    """LLM 调用失败（可重试）。"""


class LLMTimeoutError(LLMError):
    """LLM 调用超时。"""


class LLMAPIError(LLMError):
    """LLM 返回非 2xx。"""


class OpenAICompatClient:
    """OpenAI 兼容 Chat Completions 客户端（仅负责传输，不解析业务内容）。"""

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        model: str,
        timeout: float = 30.0,
        max_tokens: int = 512,
        temperature: float = 0.2,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.timeout = timeout
        self.max_tokens = max_tokens
        self.temperature = temperature

    def _payload(self, prompt: str, *, stream: bool) -> dict:
        return {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": self.max_tokens,
            "temperature": self.temperature,
            "stream": stream,
        }

    async def complete(self, prompt: str) -> str:
        """发送单轮对话，返回助手回复文本。超时 / 非 2xx 抛 LLMError。"""
        url = f"{self.base_url}/chat/completions"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.post(url, headers=headers, json=self._payload(prompt, stream=False))
        except httpx.TimeoutException as exc:
            raise LLMTimeoutError(f"LLM 请求超时（>{self.timeout}s）: {self.base_url}") from exc
        except httpx.HTTPError as exc:
            raise LLMAPIError(f"LLM 请求失败: {exc}") from exc

        if resp.status_code != 200:
            raise LLMAPIError(f"LLM 返回 HTTP {resp.status_code}: {resp.text[:200]}")

        try:
            data = resp.json()
            return data["choices"][0]["message"]["content"]
        except (ValueError, KeyError, IndexError, TypeError) as exc:
            raise LLMAPIError(f"LLM 响应格式异常: {resp.text[:200]}") from exc

    async def stream_complete(self, prompt: str) -> AsyncIterator[str]:
        """流式单轮对话：逐段 yield content delta。"""
        url = f"{self.base_url}/chat/completions"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                async with client.stream(
                    "POST", url, headers=headers, json=self._payload(prompt, stream=True)
                ) as resp:
                    if resp.status_code != 200:
                        body = (await resp.aread())[:200]
                        raise LLMAPIError(f"LLM 返回 HTTP {resp.status_code}: {body!r}")
                    async for line in resp.aiter_lines():
                        if not line:
                            continue
                        if not line.startswith("data:"):
                            continue
                        data = line[5:].strip()
                        if not data or data == "[DONE]":
                            if data == "[DONE]":
                                break
                            continue
                        try:
                            obj = json.loads(data)
                            delta = obj["choices"][0].get("delta") or {}
                            text = delta.get("content")
                            if text:
                                yield text
                        except (ValueError, KeyError, IndexError, TypeError):
                            continue
        except LLMError:
            raise
        except httpx.TimeoutException as exc:
            raise LLMTimeoutError(f"LLM 流式请求超时（>{self.timeout}s）: {self.base_url}") from exc
        except httpx.HTTPError as exc:
            raise LLMAPIError(f"LLM 流式请求失败: {exc}") from exc


def build_llm_client(settings) -> Optional[OpenAICompatClient]:
    """provider=openai_compatible 且配置齐全时返回真实客户端，否则返回 None（走 mock）。"""
    if settings.LLM_PROVIDER != "openai_compatible":
        return None
    if not (settings.LLM_BASE_URL and settings.LLM_API_KEY and settings.LLM_MODEL):
        return None
    return OpenAICompatClient(
        base_url=settings.LLM_BASE_URL,
        api_key=settings.LLM_API_KEY,
        model=settings.LLM_MODEL,
        timeout=settings.LLM_TIMEOUT_SECONDS,
        max_tokens=settings.LLM_MAX_TOKENS,
    )
