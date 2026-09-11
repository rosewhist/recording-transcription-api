"""
全局日志配置。
- 使用 dictConfig 统一接管 root / uvicorn / 业务 logger
- ContextVar 传递 trace_id，协程安全
- 支持 JSON（生产）与可读文本（开发）两种格式
- 单进程可用 RotatingFileHandler；多 worker 请改用 ConcurrentRotatingFileHandler
"""
from __future__ import annotations

import contextvars
import logging
import logging.config
from pathlib import Path
from typing import Any, Optional

try:
    from pythonjsonlogger.json import JsonFormatter  # python-json-logger >= 2
except ImportError:  # 兼容旧版本
    from pythonjsonlogger import jsonlogger as _jl  # type: ignore

    JsonFormatter = _jl.JsonFormatter  # type: ignore


# ---------------------------------------------------------------------------
# ContextVar & Filter
# ---------------------------------------------------------------------------
trace_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "trace_id", default="-"
)


def task_trace_id(task_id: Any) -> str:
    """Canonical per-task correlation id used in logs (upload + worker)."""
    return f"task-{task_id}"


def bind_task_trace(task_id: Any) -> contextvars.Token:
    """Switch current ContextVar to ``task-{id}``; caller should ``reset`` the token."""
    return trace_id_var.set(task_trace_id(task_id))


class ContextFilter(logging.Filter):
    """把 ContextVar 中的 trace_id 注入到每条 LogRecord。"""

    def filter(self, record: logging.LogRecord) -> bool:
        record.trace_id = trace_id_var.get()
        return True


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """获取业务 logger；默认名为 app。"""
    return logging.getLogger(name or "app")


# ---------------------------------------------------------------------------
# dictConfig
# ---------------------------------------------------------------------------
def _build_config(
    *,
    level: str,
    log_json: bool,
    log_file: Optional[str],
    max_bytes: int,
    backup_count: int,
) -> dict[str, Any]:
    level = level.upper()
    formatter_name = "json" if log_json else "plain"

    formatters: dict[str, Any] = {
        "plain": {
            "()": logging.Formatter,
            "fmt": "%(asctime)s | %(levelname)-8s | [%(trace_id)s] | %(name)s | %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "json": {
            "()": JsonFormatter,
            "fmt": "%(asctime)s %(levelname)s %(trace_id)s %(name)s %(message)s",
            "rename_fields": {
                "asctime": "ts",
                "levelname": "level",
                "name": "logger",
            },
        },
    }

    handlers: dict[str, Any] = {
        "console": {
            "class": "logging.StreamHandler",
            "level": level,
            "formatter": formatter_name,
            "stream": "ext://sys.stdout",
            "filters": ["context"],
        },
    }

    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        handlers["file"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "level": level,
            "formatter": formatter_name,
            "filename": log_file,
            "maxBytes": max_bytes,
            "backupCount": backup_count,
            "encoding": "utf-8",
            "filters": ["context"],
        }

    root_handlers = list(handlers.keys())

    return {
        "version": 1,
        "disable_existing_loggers": False,
        "filters": {
            "context": {"()": ContextFilter},
        },
        "formatters": formatters,
        "handlers": handlers,
        "loggers": {
            "uvicorn": {
                "handlers": root_handlers,
                "level": level,
                "propagate": False,
            },
            "uvicorn.error": {
                "handlers": root_handlers,
                "level": level,
                "propagate": False,
            },
            "uvicorn.access": {
                "handlers": root_handlers,
                "level": "WARNING",
                "propagate": False,
            },
            "asyncio": {"level": "WARNING", "handlers": [], "propagate": True},
        },
        "root": {
            "handlers": root_handlers,
            "level": level,
        },
    }


def setup_logging(
    *,
    level: str = "INFO",
    log_json: bool = True,
    log_file: Optional[str] = None,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
) -> None:
    """进程启动时显式调用一次（建议在 create_app 内、路由注册前）。"""
    logging.config.dictConfig(
        _build_config(
            level=level,
            log_json=log_json,
            log_file=log_file,
            max_bytes=max_bytes,
            backup_count=backup_count,
        )
    )


logger = get_logger("app")
