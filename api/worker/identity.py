"""解析稳定的 worker 标识。

``tasks.locked_by`` 记录占用任务的 worker。若标识每次启动都随机，
``reclaim_in_flight(include_own_locks=True)`` 的「本 worker 仍持有的任务」分支
永远匹配不到上一进程的锁，服务重启后只能等租约（``WORKER_LEASE_SECONDS``，默认
300s）过期才恢复。这里把标识持久化到 ``WORKER_STATE_DIR/worker_id``，使同一实例
重启后复用同一标识，从而**立即**回收自身 in-flight 任务。
"""
from __future__ import annotations

import os
import socket
import uuid
from pathlib import Path
from typing import Optional

from api.config.settings import Settings
from api.core.logger import get_logger

logger = get_logger(__name__)

# 与 tasks.locked_by VARCHAR(64) 对齐
_MAX_WORKER_ID_LEN = 64
_STATE_FILENAME = "worker_id"


def _hostname() -> str:
    try:
        return socket.gethostname() or "unknown"
    except OSError:  # pragma: no cover - gethostname 极少失败
        return "unknown"


def _sanitize(value: str) -> str:
    cleaned = "".join(
        ch if ch.isalnum() or ch in "-_." else "-" for ch in value
    ).strip("-.")
    return cleaned or "worker"


def _new_worker_id() -> str:
    """构造可读且长度受限的新标识，如 ``worker-myhost-1a2b3c4d``。"""
    token = uuid.uuid4().hex[:8]
    prefix = _sanitize(f"worker-{_hostname()}")
    room = max(1, _MAX_WORKER_ID_LEN - len(token) - 1)
    return f"{prefix[:room]}-{token}"


def _load_or_create(state_path: Path) -> Optional[str]:
    """读取持久化标识；不存在则生成并写入。无法读写时返回 ``None``。"""
    try:
        existing = state_path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        existing = ""
    except OSError:
        logger.warning("读取 worker 标识文件失败: %s", state_path, exc_info=True)
        return None

    if existing:
        return existing[:_MAX_WORKER_ID_LEN]

    new_id = _new_worker_id()
    try:
        state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = state_path.with_name(state_path.name + ".tmp")
        tmp_path.write_text(new_id, encoding="utf-8")
        os.replace(tmp_path, state_path)
    except OSError:
        logger.warning(
            "写入 worker 标识文件失败: %s（重启后需等租约过期才能回收自身任务）",
            state_path,
            exc_info=True,
        )
        return None
    return new_id


def resolve_worker_id(settings: Settings) -> str:
    """确定 worker 标识。

    优先级：显式 ``WORKER_ID`` > ``WORKER_STATE_DIR/worker_id`` 持久化标识 >
    临时 ``worker-{hostname}-{hex}``（此时重启恢复只能依赖租约过期）。
    """
    if settings.WORKER_ID:
        return settings.WORKER_ID[:_MAX_WORKER_ID_LEN]

    state_path = Path(settings.WORKER_STATE_DIR) / _STATE_FILENAME
    persisted = _load_or_create(state_path)
    if persisted:
        logger.info("worker 标识已持久化: %s（%s）", persisted, state_path)
        return persisted

    fallback = _new_worker_id()
    logger.warning(
        "无法持久化 worker 标识，使用临时标识: %s（重启后需等租约过期）", fallback
    )
    return fallback
