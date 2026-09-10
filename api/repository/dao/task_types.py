import sys

if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from enum import Enum

    class StrEnum(str, Enum):
        """Python 3.9/3.10 兼容的 StrEnum。"""


class TaskStatus(StrEnum):
    """任务状态。API 与数据库统一使用小写值。"""

    PENDING = "pending"
    TRANSCRIBING = "transcribing"
    SUMMARIZING = "summarizing"
    DONE = "done"
    FAILED = "failed"


# 仍在流转、服务重启后需要恢复的状态
RECOVERABLE_STATUSES = (TaskStatus.PENDING, TaskStatus.TRANSCRIBING, TaskStatus.SUMMARIZING)
# 终态
TERMINAL_STATUSES = (TaskStatus.DONE, TaskStatus.FAILED)
