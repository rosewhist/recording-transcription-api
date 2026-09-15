from __future__ import annotations

import json
import logging

import pytest
from loguru import logger as loguru_logger

from api.core.logger import (
    get_instance_id,
    get_logger,
    log_context,
    set_instance_id,
    set_worker_id,
    setup_logging,
    worker_id_var,
)


@pytest.fixture(autouse=True)
def _clean_logging():
    """恢复 logging/loguru/context 状态，避免污染其它测试。"""
    root = logging.root
    saved_handlers = root.handlers[:]
    saved_level = root.level
    saved_worker_id = worker_id_var.get()
    saved_instance_id = get_instance_id()
    yield
    loguru_logger.remove()
    root.handlers = saved_handlers
    root.setLevel(saved_level)
    worker_id_var.set(saved_worker_id)
    set_instance_id(saved_instance_id)


@pytest.fixture
def capture():
    """setup_logging 配好 patcher 后挂一个捕获 sink，返回 record 列表。"""
    setup_logging(level="INFO", log_json=True, log_file=None)
    loguru_logger.remove()  # 去掉默认 sink，保留 patcher + 标准库拦截器

    records: list[dict] = []
    sink_id = loguru_logger.add(
        lambda message: records.append(message.record), level=0, format="{message}"
    )
    try:
        yield records
    finally:
        loguru_logger.remove(sink_id)


def _doc(record: dict) -> dict:
    # sink 实际输出的成品 JSON（patcher 挂在 extra[_r_json] 的懒渲染器，str() 时渲染）
    return json.loads(str(record["extra"]["_r_json"]))


def test_get_logger_binds_name(capture):
    get_logger("api.demo").info("hello")

    assert _doc(capture[-1])["logger"] == "api.demo"


def test_json_schema_matches_target(capture):
    with log_context(request_id="req-1", task_id="task-1", recording_id="rec-1"):
        get_logger("api.service.recording").info(
            "任务已创建", event="task.created", file_size=48213, file_hash="a1b2"
        )

    doc = _doc(capture[-1])
    assert list(doc) == [
        "ts",
        "level",
        "logger",
        "event",
        "message",
        "request_id",
        "task_id",
        "recording_id",
        "worker_id",
        "instance_id",
        "extra",
    ]
    assert doc["level"] == "INFO"
    assert doc["logger"] == "api.service.recording"
    assert doc["event"] == "task.created"
    assert doc["message"] == "任务已创建"
    assert doc["request_id"] == "req-1"
    assert doc["task_id"] == "task-1"
    assert doc["recording_id"] == "rec-1"
    assert doc["worker_id"] is None
    assert doc["instance_id"] is None
    assert doc["extra"] == {"file_size": 48213, "file_hash": "a1b2"}
    assert doc["ts"].endswith("Z")


def test_log_context_restores_previous_ids(capture):
    with log_context(task_id="outer"):
        with log_context(task_id="inner"):
            get_logger("api.demo").info("inner", event="probe")
            assert _doc(capture[-1])["task_id"] == "inner"
        get_logger("api.demo").info("outer", event="probe")
        assert _doc(capture[-1])["task_id"] == "outer"

    get_logger("api.demo").info("none", event="probe")
    assert _doc(capture[-1])["task_id"] is None


def test_worker_id_duration_and_error_fields(capture):
    set_worker_id("worker-host-abc")
    with log_context(task_id="t1"):
        get_logger("api.worker.executor").info(
            "任务执行成功", event="task.done", status="done", duration_ms=12.5
        )

    doc = _doc(capture[-1])
    assert doc["worker_id"] == "worker-host-abc"
    assert doc["duration_ms"] == 12.5
    assert doc["extra"] == {"status": "done"}


def test_instance_id_on_every_record_but_worker_id_only_in_worker_context(capture):
    set_instance_id("worker-host1-7f3c9a01")

    # HTTP 侧：无 worker 上下文
    get_logger("api.service.recording").info("HTTP 请求完成", event="http.request")
    http_doc = _doc(capture[-1])
    assert http_doc["instance_id"] == "worker-host1-7f3c9a01"
    assert http_doc["worker_id"] is None

    # worker 侧：额外带 worker_id
    set_worker_id("worker-host1-7f3c9a01")
    get_logger("api.worker.executor").info("任务执行成功", event="task.done")
    worker_doc = _doc(capture[-1])
    assert worker_doc["instance_id"] == "worker-host1-7f3c9a01"
    assert worker_doc["worker_id"] == "worker-host1-7f3c9a01"


def test_exception_becomes_error_object(capture):
    try:
        raise ValueError("boom")
    except ValueError:
        loguru_logger.opt(exception=True).error("处理失败", event="task.failed")

    error = _doc(capture[-1])["error"]
    assert error["type"] == "ValueError"
    assert error["message"] == "boom"
    assert "Traceback" in error["traceback"]


def test_intercept_handler_forwards_stdlib_into_schema(capture):
    with log_context(request_id="req-9"):
        logging.getLogger("uvicorn.error").warning("third-party %s", "log")

    doc = _doc(capture[-1])
    assert doc["message"] == "third-party log"
    assert doc["logger"] == "uvicorn.error"
    assert doc["level"] == "WARNING"
    assert doc["request_id"] == "req-9"


def test_setup_logging_is_idempotent(tmp_path):
    log_file = tmp_path / "app.log"
    setup_logging(level="INFO", log_json=True, log_file=str(log_file))
    setup_logging(level="INFO", log_json=True, log_file=str(log_file))

    get_logger("api.demo").info("only-once")

    assert log_file.read_text(encoding="utf-8").count("only-once") == 1
