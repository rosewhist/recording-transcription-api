"""Task status enums and status-set constants shared by API / worker / DAO."""
from __future__ import annotations

import sys

if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from enum import Enum

    class StrEnum(str, Enum):
        """StrEnum for Python 3.9/3.10."""


class TaskStatus(StrEnum):
    """Task lifecycle status; DB and API use lowercase string values."""

    PENDING = "pending"
    TRANSCRIBING = "transcribing"
    SUMMARIZING = "summarizing"
    DONE = "done"
    FAILED = "failed"


# Actively processed (has / should have a lease).
IN_FLIGHT_STATUSES = (TaskStatus.TRANSCRIBING, TaskStatus.SUMMARIZING)
# Still in the pipeline (queue + in-flight); restart-safe recoverable set.
RECOVERABLE_STATUSES = (TaskStatus.PENDING,) + IN_FLIGHT_STATUSES
# Terminal outcomes.
TERMINAL_STATUSES = (TaskStatus.DONE, TaskStatus.FAILED)

# Convenience string tuples for SQL ``IN (...)`` / ORM comparisons.
IN_FLIGHT_STATUS_VALUES = tuple(s.value for s in IN_FLIGHT_STATUSES)
RECOVERABLE_STATUS_VALUES = tuple(s.value for s in RECOVERABLE_STATUSES)
TERMINAL_STATUS_VALUES = tuple(s.value for s in TERMINAL_STATUSES)
