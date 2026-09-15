"""全局日志：loguru 原生 + 扁平结构化输出（JSON / 彩色文本）。

设计要点
--------
- 调用点直接用 loguru：``logger = get_logger(__name__)``，
  占位符用 ``{}``，异常用 ``logger.opt(exception=True)`` / ``logger.exception``。
- 结构化字段作为「未消费的 kwargs」自动进入 ``record["extra"]``，例如::

      logger.info("任务已创建", event="task.created", file_size=n, file_hash=h)

- 链路标识由 ContextVar 注入（协程安全）：``request_id`` / ``task_id`` /
  ``recording_id``；``worker_id`` 仅在 worker 上下文绑定。
- 标准库 logging（uvicorn / sqlalchemy / openai 等）经 ``InterceptHandler``
  转发到同一 schema。
- 输出为**扁平**单行 JSON（便于 ELK/Loki 按字段检索）：

      {"ts":"...Z","level":"INFO","logger":"api.service.recording",
       "event":"task.created","message":"任务已创建","request_id":"...",
       "task_id":"...","recording_id":"...","worker_id":null,
       "extra":{"file_size":48213}}

实现说明：loguru 的 ``format`` 若传可调用对象，其返回值会被当作**模板**再做一次
``str.format_map``，因此不能直接返回含 ``{}`` 的 JSON/文本。这里改由 patcher 把
``_LazyRender`` 写入 ``extra['_r_*']``，sink 仅用 ``format="{extra[_r_*]}"`` 取值；
``_LazyRender`` 在 sink 实际取值（``str()``）时才渲染并缓存，因此通过级别/过滤器
的日志不会付出渲染开销，多 sink 时各自渲染所需格式。
"""
from __future__ import annotations

import contextvars
import inspect
import json
import logging
import sys
import traceback
from contextlib import contextmanager
from datetime import timezone
from pathlib import Path
from typing import Any, Iterator, Optional

from loguru import logger as _logger

# ---------------------------------------------------------------------------
# 上下文（ContextVar，协程安全）与 worker 标识
# ---------------------------------------------------------------------------
request_id_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "request_id", default=None
)
task_id_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "task_id", default=None
)
recording_id_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "recording_id", default=None
)
worker_id_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "worker_id", default=None
)


def bind_request_id(value: Any) -> contextvars.Token:
    """绑定本次 HTTP 请求的 request_id；返回 token 供 ``reset``。"""
    return request_id_var.set(None if value is None else str(value))


def bind_task_id(value: Any) -> contextvars.Token:
    """绑定任务 id；worker 全程与相关 HTTP 请求都应绑定，便于按 task_id 串联。"""
    return task_id_var.set(None if value is None else str(value))


def bind_recording_id(value: Any) -> contextvars.Token:
    """绑定录音 id。"""
    return recording_id_var.set(None if value is None else str(value))


def set_worker_id(value: Optional[str]) -> contextvars.Token:
    """在当前（worker）上下文绑定标识；HTTP 请求上下文不同步，故仍为 null。"""
    return worker_id_var.set(None if value is None else str(value))


def get_worker_id() -> Optional[str]:
    return worker_id_var.get()


# 进程级实例标识（全局，非 ContextVar）：每条日志都带上，用于多副本/进程归因
_instance_id: Optional[str] = None


def set_instance_id(value: Optional[str]) -> None:
    """设置进程级实例标识（启动时一次）；会注入到每条日志。"""
    global _instance_id
    _instance_id = None if value is None else str(value)


def get_instance_id() -> Optional[str]:
    return _instance_id


def get_logger(name: Optional[str] = None) -> Any:
    """获取绑定了 ``name`` 的 loguru logger；默认名为 app。"""
    return _logger.bind(name=name or "app")


@contextmanager
def log_context(
    *,
    request_id: Any = None,
    task_id: Any = None,
    recording_id: Any = None,
) -> Iterator[None]:
    """临时绑定链路标识，退出时自动恢复（异步函数内亦可使用）。"""
    tokens: list[tuple[contextvars.ContextVar, contextvars.Token]] = []
    for var, value in (
        (request_id_var, request_id),
        (task_id_var, task_id),
        (recording_id_var, recording_id),
    ):
        if value is not None:
            tokens.append((var, var.set(str(value))))
    try:
        yield
    finally:
        for var, token in reversed(tokens):
            var.reset(token)


# ---------------------------------------------------------------------------
# 字段布局
# ---------------------------------------------------------------------------
# 渲染结果键（存于 extra，供 sink 模板取值，不进 JSON 输出）
_RENDER_KEYS = ("_r_json", "_r_console", "_r_plain")
# 顶层字段；extra 中的其余键进入嵌套 "extra"
_RESERVED = frozenset(
    {
        "name",
        "event",
        "request_id",
        "task_id",
        "recording_id",
        "worker_id",
        "instance_id",
        "duration_ms",
        "error",
        "_exc",
        *_RENDER_KEYS,
    }
)

_DIM = "\x1b[2m"
_RESET = "\x1b[0m"
_MAGENTA = "\x1b[35m"
_LEVEL_COLOR = {
    "TRACE": "\x1b[35m",
    "DEBUG": "\x1b[34m",
    "INFO": "\x1b[32m",
    "SUCCESS": "\x1b[32m",
    "WARNING": "\x1b[33m",
    "ERROR": "\x1b[31m",
    "CRITICAL": "\x1b[1;31m",
}


def _dominant_error(record: dict) -> Any:
    """优先用调用方传入的 error（dict/str），否则由异常构造。"""
    extra_error = record["extra"].get("error")
    if extra_error is not None:
        return extra_error
    exc = record["extra"].get("_exc")
    if exc is None:
        return None
    return {
        "type": getattr(exc.type, "__name__", str(exc.type)),
        "message": str(exc.value),
        "traceback": "".join(
            traceback.format_exception(exc.type, exc.value, exc.traceback)
        ),
    }


def _domain_extra(extra: dict) -> dict:
    return {k: v for k, v in extra.items() if k not in _RESERVED}


def _context_parts(extra: dict, *, colored: bool) -> str:
    parts = []
    if extra.get("task_id"):
        parts.append(f"task={_short(extra['task_id'])}")
    if extra.get("request_id"):
        parts.append(f"req={_short(extra['request_id'])}")
    if extra.get("recording_id"):
        parts.append(f"rec={_short(extra['recording_id'])}")
    if extra.get("worker_id"):
        parts.append(f"w={_short(extra['worker_id'])}")
    if extra.get("instance_id"):
        parts.append(f"inst={_short(extra['instance_id'])}")
    text = " ".join(parts) or "-"
    return f"{_DIM}{text}{_RESET}" if colored else text


def _short(value: Any, length: int = 8) -> str:
    if value is None or value == "":
        return "-"
    text = str(value)
    return text if len(text) <= length else text[:length] + "…"


def json_formatter(record: dict) -> str:
    """扁平单行 JSON（顶层 schema 固定，域字段归入 extra）。"""
    extra = record["extra"]
    doc: dict[str, Any] = {
        "ts": record["time"]
        .astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z"),
        "level": record["level"].name,
        "logger": extra.get("name") or record["name"],
        "event": extra.get("event"),
        "message": record["message"],
        "request_id": extra.get("request_id"),
        "task_id": extra.get("task_id"),
        "recording_id": extra.get("recording_id"),
        "worker_id": extra.get("worker_id"),
        "instance_id": extra.get("instance_id"),
    }
    duration = extra.get("duration_ms")
    if duration is not None:
        doc["duration_ms"] = duration
    error = _dominant_error(record)
    if error is not None:
        doc["error"] = error
    domain = _domain_extra(extra)
    if domain:
        doc["extra"] = domain
    return json.dumps(doc, ensure_ascii=False, default=str)


def _text_formatter(record: dict, *, colored: bool) -> str:
    extra = record["extra"]
    level = record["level"].name
    ctx = _context_parts(extra, colored=colored)
    if colored:
        color = _LEVEL_COLOR.get(level, "")
        ts = record["time"].strftime("%H:%M:%S.%f")[:-3]
        head = f"{_DIM}{ts}{_RESET} | {color}{level: <7}{_RESET} | {ctx} | "
        tail = f"{_MAGENTA}{extra.get('name') or record['name']}{_RESET}"
    else:
        ts = record["time"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        head = f"{ts} | {level: <7} | {ctx} | "
        tail = str(extra.get("name") or record["name"])
    line = f"{head}{extra.get('event') or '-'} | {tail} | {record['message']}"
    if extra.get("duration_ms") is not None:
        suffix = f"({extra['duration_ms']}ms)"
        line += f" {_DIM}{suffix}{_RESET}" if colored else f" {suffix}"
    domain = _domain_extra(extra)
    if domain:
        rendered = " ".join(f"{k}={v}" for k, v in domain.items())
        line += f"  {_DIM}{rendered}{_RESET}" if colored else f"  {rendered}"
    error = _dominant_error(record)
    if isinstance(error, dict) and error.get("traceback"):
        line += "\n" + str(error["traceback"]).rstrip()
    return line


def plain_formatter(record: dict) -> str:
    """无颜色的可读单行（文件文本模式）。"""
    return _text_formatter(record, colored=False)


def console_formatter(record: dict) -> str:
    """带颜色的可读单行（开发控制台）。"""
    return _text_formatter(record, colored=True)


# 需要为每条记录准备取值入口的渲染器（由 setup_logging 设定）
_ACTIVE_RENDERERS: tuple[str, ...] = ()
_RENDERERS = {
    "json": json_formatter,
    "console": console_formatter,
    "plain": plain_formatter,
}
_EXTRA_KEY = {"json": "_r_json", "console": "_r_console", "plain": "_r_plain"}


class _LazyRender:
    """延迟渲染：sink 取值（``str()``）时才渲染成品字符串并缓存。

    patcher 对每条记录只运行一次、先于 sink 的级别/过滤器，预渲染会让被过滤
    的日志白白付出开销；多 sink（控制台+文件）时也只需各自渲染用到的格式。
    """

    __slots__ = ("_fn", "_record", "_cache")

    def __init__(self, fn, record: dict) -> None:
        self._fn = fn
        self._record = record
        self._cache: Optional[str] = None

    def __str__(self) -> str:
        if self._cache is None:
            self._cache = self._fn(self._record)
        return self._cache


def _patcher(record: dict) -> None:
    """注入链路标识，并挂上各 sink 需要的延迟渲染器。"""
    extra = record["extra"]
    extra.setdefault("name", "app")
    extra.setdefault("request_id", request_id_var.get())
    extra.setdefault("task_id", task_id_var.get())
    extra.setdefault("recording_id", recording_id_var.get())
    extra.setdefault("worker_id", worker_id_var.get())
    extra.setdefault("instance_id", _instance_id)
    for name in _ACTIVE_RENDERERS:
        extra[_EXTRA_KEY[name]] = _LazyRender(_RENDERERS[name], record)
    # loguru 会对不含 {exception} 占位符的模板自动追加 ``"\n{exception}"``，
    # 故把异常暂存进 extra（渲染器经 ``_exc`` 读取）并清除原字段，防止
    # traceback 被附加到成品字符串之外重复输出。
    if record["exception"] is not None:
        extra["_exc"] = record["exception"]
        record["exception"] = None


# ---------------------------------------------------------------------------
# 标准库 -> loguru
# ---------------------------------------------------------------------------
class InterceptHandler(logging.Handler):
    """把标准库 LogRecord 转发给 loguru（uvicorn / sqlalchemy / openai）。"""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level: Any = _logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        frame, depth = inspect.currentframe(), 0
        while frame is not None and (
            depth == 0 or frame.f_code.co_filename == logging.__file__
        ):
            frame = frame.f_back
            depth += 1

        _logger.opt(depth=depth, exception=record.exc_info).bind(
            name=record.name
        ).log(level, record.getMessage())


def setup_logging(
    *,
    level: str = "INFO",
    log_json: bool = True,
    log_file: Optional[str] = None,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
) -> None:
    """进程启动时显式调用一次（建议在 create_app 内、路由注册前）。"""
    global _ACTIVE_RENDERERS
    level = level.upper()

    # 幂等：重复调用（--reload / 多入口）不叠加 sink
    _logger.remove()

    if log_json:
        _ACTIVE_RENDERERS = ("json",)
        console_format = file_format = "{extra[_r_json]}"
    else:
        _ACTIVE_RENDERERS = ("console", "plain")
        console_format = "{extra[_r_console]}"
        file_format = "{extra[_r_plain]}"

    _logger.configure(patcher=_patcher)

    _logger.add(
        sys.stdout,
        level=level,
        format=console_format,
        colorize=False,
        backtrace=False,
        diagnose=False,
        enqueue=False,
    )

    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        _logger.add(
            log_file,
            level=level,
            format=file_format,
            rotation=max_bytes,
            retention=backup_count,
            encoding="utf-8",
            backtrace=False,
            diagnose=False,
            enqueue=False,
        )

    # 标准库 -> loguru：接管 root 与 uvicorn 系 logger
    intercept = InterceptHandler()
    logging.root.handlers = [intercept]
    logging.root.setLevel(level)

    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        std_logger = logging.getLogger(name)
        std_logger.handlers = [intercept]
        std_logger.setLevel("WARNING" if name == "uvicorn.access" else level)
        std_logger.propagate = False

    logging.getLogger("asyncio").setLevel("WARNING")


logger = get_logger("app")
