from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict

_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    """应用配置。

    优先顺序：进程环境变量 > 仓库根 ``.env`` > 下方兜底默认值。
    推荐配置见 ``.env.example``（复制为 ``.env`` 后修改）；密钥勿提交仓库。
    """

    # ---- App（路径类兜底相对仓库根，避免写死本机绝对路径）----
    API_HOST: str = "127.0.0.1"
    API_PORT: int = 8000
    DEBUG: bool = False
    LOG_DIR: str = str(_ROOT / "logs")
    LOG_LEVEL: str = "INFO"
    LOG_JSON: bool = True
    LOG_TO_FILE: bool = True
    LOG_MAX_BYTES: int = 10_485_760
    LOG_BACKUP_COUNT: int = 5

    # ---- Database（与 compose 宿主机映射 5433 对齐；容器内由 compose 覆盖）----
    DATABASE_URL: str = (
        "postgresql+asyncpg://postgres:postgres@127.0.0.1:5433/recording_transcription"
    )
    POOL_SIZE: int = 10
    POOL_TIMEOUT: float = 10.0

    # ---- 上传 ----
    UPLOAD_DIR: str = str(_ROOT / "uploads")
    MAX_FILE_SIZE_MB: int = 50
    ALLOWED_EXTENSIONS: str = "wav,mp3,m4a,aac"

    # ---- LLM（Key 默认空；模型 URL 可由 .env 覆盖）----
    LLM_PROVIDER: str = "openai_compatible"
    LLM_BASE_URL: str = "https://api.deepseek.com/v1"
    LLM_API_KEY: str = ""
    LLM_MODEL: str = "deepseek-chat"
    LLM_TIMEOUT_SECONDS: float = 30.0
    LLM_MAX_TOKENS: int = 512
    LLM_MAX_RETRIES: int = 3

    # ---- Worker（无 Key 时请在 .env 设 WORKER_ALLOW_MOCK_LLM=true）----
    WORKER_MAX_CONCURRENCY: int = 3
    WORKER_MAX_BATCH_SIZE: int = 50
    WORKER_LEASE_SECONDS: int = 300
    WORKER_MAX_RETRIES: int = 3
    WORKER_POLL_IDLE_SECONDS: float = 1.0
    WORKER_ALLOW_MOCK_LLM: bool = False
    WORKER_ID: Optional[str] = None
    # 持久化 worker 标识（重启复用，使“重置自身锁”立即生效）
    WORKER_STATE_DIR: str = str(_ROOT / ".worker")

    model_config = SettingsConfigDict(
        env_file=_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ---- 派生配置 ----
    @property
    def allowed_ext_set(self) -> set[str]:
        return {
            f".{e.strip().lower().lstrip('.')}"
            for e in self.ALLOWED_EXTENSIONS.split(",")
            if e.strip()
        }

    @property
    def max_file_size_bytes(self) -> int:
        return self.MAX_FILE_SIZE_MB * 1024 * 1024

    @property
    def log_file_path(self) -> Optional[str]:
        if not self.LOG_TO_FILE:
            return None
        return str(Path(self.LOG_DIR) / "app.log")


@lru_cache
def get_settings() -> Settings:
    return Settings()
