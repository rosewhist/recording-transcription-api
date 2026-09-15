"""真实 Postgres 集成测试的基础设施。

设计约束：

- **默认离线套件不受影响**：连不上数据库时整组 ``skip``，``pytest`` 依然全绿。
- **拒绝误伤**：目标库名必须含 ``test``，否则直接退出——本目录的 teardown 会 DROP
  所有表。
- **schema 由 Alembic 建立**：顺带覆盖此前完全没被测过的迁移（含 ``downgrade``）。

指定目标库：``TEST_DATABASE_URL=postgresql+asyncpg://user:pw@host:port/db_test``
"""
from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Iterator
from urllib.parse import urlsplit, urlunsplit

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool
from sqlmodel.ext.asyncio.session import AsyncSession

DEFAULT_TEST_DATABASE_URL = (
    "postgresql+asyncpg://postgres:postgres@127.0.0.1:5433/recording_transcription_test"
)

_SKIP_HINT = (
    "需要真实 Postgres 才能运行集成测试（默认套件不含数据库依赖）。\n"
    "启动一个临时实例：\n"
    "  docker run -d --rm -p 55432:5432 -e POSTGRES_PASSWORD=postgres postgres:16-alpine\n"
    "指向它：\n"
    "  TEST_DATABASE_URL=postgresql+asyncpg://postgres:postgres@127.0.0.1:55432/"
    "recording_transcription_test pytest tests/integration\n"
)


def _test_database_url() -> str:
    return os.environ.get("TEST_DATABASE_URL") or DEFAULT_TEST_DATABASE_URL


def _database_name(url: str) -> str:
    return urlsplit(url).path.lstrip("/")


def _guard_database_name(url: str) -> str:
    name = _database_name(url)
    if "test" not in name.lower():
        pytest.exit(
            f"TEST_DATABASE_URL 指向的库名 {name!r} 不含 'test'；"
            "本目录 teardown 会 DROP 所有表，拒绝执行。",
            returncode=1,
        )
    return name


def _admin_url(url: str) -> str:
    """同实例的 ``postgres`` 库，用于 CREATE DATABASE。"""
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, "/postgres", parts.query, ""))


async def _ensure_database(url: str) -> None:
    name = _guard_database_name(url)
    engine = create_async_engine(
        _admin_url(url), isolation_level="AUTOCOMMIT", poolclass=NullPool
    )
    try:
        async with engine.connect() as conn:
            exists = await conn.scalar(
                text("SELECT 1 FROM pg_database WHERE datname = :name"), {"name": name}
            )
            if not exists:
                await conn.execute(text(f'CREATE DATABASE "{name}"'))
    finally:
        await engine.dispose()


def _alembic_config():
    from alembic.config import Config

    root = Path(__file__).resolve().parents[2]
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "alembic"))
    return cfg


def _migration_url_env(url: str) -> None:
    """``alembic/env.py`` 读 ``Settings.DATABASE_URL``，故经环境变量注入测试库。"""
    from api.config.settings import get_settings

    os.environ["DATABASE_URL"] = url
    get_settings.cache_clear()


# pytest 会为每个测试重新执行「setup 失败」的 session fixture，故把探测结果缓存，
# 避免没有数据库时每个用例都重试一次连接。
_PROBE: dict[str, Optional[str]] = {}


def _probe_database(url: str) -> Optional[str]:
    """可用则返回 ``None``，否则返回失败原因（供 skip 展示）。"""
    if url not in _PROBE:
        try:
            _guard_database_name(url)
            asyncio.run(_ensure_database(url))
            _PROBE[url] = None
        except Exception as exc:  # noqa: BLE001 — 任何连接类失败都视为「没有可用数据库」
            _PROBE[url] = f"{type(exc).__name__}: {exc}"
    return _PROBE[url]


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    url = _test_database_url()
    reason = _probe_database(url)
    if reason is not None:
        pytest.skip(f"{_SKIP_HINT}\n实际错误：{reason}")

    from alembic import command

    _migration_url_env(url)
    cfg = _alembic_config()
    command.upgrade(cfg, "head")
    try:
        yield url
    finally:
        # 走 downgrade 而不是 DROP：顺带验证迁移的 downgrade 路径可用。
        command.downgrade(cfg, "base")


# 语句级超时（毫秒）。并发用例依赖 ``SKIP LOCKED``：一旦有人把它改回普通
# ``FOR UPDATE``，被锁的查询会一直等下去——有了这个超时，回归表现为 10s 内失败
# 而不是把 CI 挂死。
_STATEMENT_TIMEOUT_MS = 10_000


def _engine_kwargs() -> dict:
    return {
        "poolclass": NullPool,
        "connect_args": {
            "server_settings": {"statement_timeout": str(_STATEMENT_TIMEOUT_MS)}
        },
    }


@pytest.fixture
async def db_engine(postgres_url: str):
    engine = create_async_engine(postgres_url, **_engine_kwargs())
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def session_factory(db_engine) -> async_sessionmaker:
    """每个测试前清空数据；可多次调用以开独立会话（并发用例需要）。"""
    factory = async_sessionmaker(
        db_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )
    async with db_engine.begin() as conn:
        await conn.execute(text("TRUNCATE tasks, recordings RESTART IDENTITY CASCADE"))
    return factory


@pytest.fixture
async def session(session_factory):
    """单个会话；事务边界由测试自行控制（与生产 DAO 契约一致）。"""
    async with session_factory() as s:
        yield s
