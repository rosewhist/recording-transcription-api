"""统一错误响应与异常映射。"""
from __future__ import annotations

from typing import Any, Optional

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from api.core.logger import get_logger
from api.domain.exceptions import (
    AppError,
    InvalidRequestError,
    InvalidUploadError,
    RecordingNotFoundError,
    ServiceUnavailableError,
    TaskNotFoundError,
    TaskNotRetryableError,
)

logger = get_logger(__name__)


class ErrorBody(BaseModel):
    code: str
    message: str
    details: Optional[Any] = None


class ErrorResponse(BaseModel):
    error: ErrorBody = Field(..., description="统一错误结构")


def _error_json(
    status_code: int,
    code: str,
    message: str,
    details: Any = None,
) -> JSONResponse:
    payload = ErrorResponse(
        error=ErrorBody(code=code, message=message, details=details)
    )
    return JSONResponse(status_code=status_code, content=payload.model_dump())


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(InvalidUploadError)
    async def invalid_upload(_request: Request, exc: InvalidUploadError):
        return _error_json(400, "invalid_upload", str(exc))

    @app.exception_handler(InvalidRequestError)
    async def invalid_request(_request: Request, exc: InvalidRequestError):
        return _error_json(400, "invalid_request", str(exc))

    @app.exception_handler(RecordingNotFoundError)
    async def recording_not_found(_request: Request, exc: RecordingNotFoundError):
        return _error_json(404, "recording_not_found", str(exc))

    @app.exception_handler(TaskNotFoundError)
    async def task_not_found(_request: Request, exc: TaskNotFoundError):
        return _error_json(404, "task_not_found", str(exc))

    @app.exception_handler(TaskNotRetryableError)
    async def task_not_retryable(_request: Request, exc: TaskNotRetryableError):
        return _error_json(409, "task_not_retryable", str(exc))

    @app.exception_handler(ServiceUnavailableError)
    async def service_unavailable(_request: Request, exc: ServiceUnavailableError):
        return _error_json(503, "service_unavailable", str(exc))

    @app.exception_handler(AppError)
    async def app_error(_request: Request, exc: AppError):
        return _error_json(400, "app_error", str(exc))

    @app.exception_handler(RequestValidationError)
    async def validation_error(_request: Request, exc: RequestValidationError):
        return _error_json(400, "validation_error", "请求参数不合法", details=exc.errors())

    @app.exception_handler(Exception)
    async def unhandled(_request: Request, exc: Exception):
        logger.exception("未处理异常")
        return _error_json(500, "internal_error", "服务器内部错误")
