"""领域异常（HTTP 映射在 controller / exception handler 中处理）。"""


class AppError(Exception):
    """业务异常基类。"""


class InvalidRequestError(AppError):
    """请求参数不合法。"""


class InvalidUploadError(AppError):
    """上传文件不合法。"""


class RecordingNotFoundError(AppError):
    """录音不存在。"""


class TaskNotFoundError(AppError):
    """任务不存在。"""


class TaskNotRetryableError(AppError):
    """任务当前状态不可重试。"""


class ServiceUnavailableError(AppError):
    """依赖服务不可用（如未配置 LLM）。"""
