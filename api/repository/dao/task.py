"""Task table model and DAO (PostgreSQL via SQLModel)."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional, Sequence
from uuid import UUID, uuid4

from sqlalchemy import Column, Index, bindparam, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel, select
from sqlmodel.ext.asyncio.session import AsyncSession

from api.repository.dao.task_types import IN_FLIGHT_STATUS_VALUES, TaskStatus

__all__ = [
    "Task",
    "TaskDao",
    "TaskStatus",
]

# Prefer SQL-side UTC so lease / retry clocks match worker dispatch SQL.
_SQL_UTC_NOW = "timezone('utc', now())"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Task(SQLModel, table=True):
    """Processing task for one recording (1:1)."""

    __tablename__ = "tasks"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    recording_id: UUID = Field(
        foreign_key="recordings.id",
        unique=True,
        index=True,
        nullable=False,
    )

    status: str = Field(
        default=TaskStatus.PENDING.value,
        max_length=32,
        index=True,
    )
    retry_count: int = Field(default=0)
    locked_by: Optional[str] = Field(default=None, max_length=64)
    next_retry_at: datetime = Field(default_factory=_utcnow)
    lease_expires_at: Optional[datetime] = Field(default=None)
    transcript: Optional[str] = Field(default=None)
    error_msg: Optional[str] = Field(default=None)
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)

    # PG JSONB only — Recording has no equivalent simple Field mapping.
    summary: Optional[dict[str, Any]] = Field(
        default=None,
        sa_column=Column(JSONB, nullable=True),
    )

    __table_args__ = (
        Index(
            "ix_tasks_dispatch",
            "next_retry_at",
            "created_at",
            postgresql_where=text("status = 'pending'"),
        ),
        Index("ix_tasks_lease", "status", "lease_expires_at"),
    )


class TaskDao:
    """Task persistence. Caller owns the session transaction (commit/rollback)."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, task: Task) -> Task:
        self.session.add(task)
        await self.session.flush()
        await self.session.refresh(task)
        return task

    async def get_by_id(self, task_id: UUID) -> Optional[Task]:
        return await self.session.get(Task, task_id)

    async def get_by_recording(self, recording_id: UUID) -> Optional[Task]:
        stmt = select(Task).where(Task.recording_id == recording_id)
        return (await self.session.scalars(stmt)).first()

    async def claim_pending(
        self,
        *,
        limit: int,
        worker_id: str,
        lease_seconds: int,
    ) -> list[UUID]:
        """Atomically claim pending tasks (SKIP LOCKED) into transcribing."""
        if limit <= 0:
            return []

        sql = text(
            f"""
            WITH locked AS (
                SELECT id
                FROM tasks
                WHERE status = 'pending'
                  AND next_retry_at <= {_SQL_UTC_NOW}
                ORDER BY created_at ASC
                LIMIT :limit
                FOR UPDATE SKIP LOCKED
            )
            UPDATE tasks t
            SET status           = 'transcribing',
                locked_by        = :worker_id,
                lease_expires_at = {_SQL_UTC_NOW}
                    + (:lease_seconds || ' seconds')::interval,
                updated_at       = {_SQL_UTC_NOW}
            FROM locked
            WHERE t.id = locked.id
            RETURNING t.id
            """
        )
        result = await self.session.execute(
            sql,
            {
                "limit": limit,
                "worker_id": worker_id,
                "lease_seconds": lease_seconds,
            },
        )
        return [row[0] for row in result.fetchall()]

    async def renew_lease(
        self,
        task_id: UUID,
        *,
        worker_id: str,
        lease_seconds: int,
    ) -> bool:
        """Extend lease with SQL UTC clock. Returns False if lock is gone."""
        sql = text(
            f"""
            UPDATE tasks
            SET lease_expires_at = {_SQL_UTC_NOW}
                    + (:lease_seconds || ' seconds')::interval,
                updated_at = {_SQL_UTC_NOW}
            WHERE id = :task_id
              AND locked_by = :worker_id
            """
        )
        result = await self.session.execute(
            sql,
            {
                "task_id": task_id,
                "worker_id": worker_id,
                "lease_seconds": lease_seconds,
            },
        )
        return bool(result.rowcount)

    async def mark_summarizing(
        self,
        task_id: UUID,
        *,
        worker_id: str,
        transcript: str,
    ) -> bool:
        sql = text(
            f"""
            UPDATE tasks
            SET status = 'summarizing',
                transcript = :transcript,
                updated_at = {_SQL_UTC_NOW}
            WHERE id = :task_id
              AND locked_by = :worker_id
            """
        )
        result = await self.session.execute(
            sql,
            {
                "task_id": task_id,
                "worker_id": worker_id,
                "transcript": transcript,
            },
        )
        return bool(result.rowcount)

    async def mark_done(
        self,
        task_id: UUID,
        *,
        worker_id: str,
        summary: dict[str, Any],
    ) -> bool:
        sql = text(
            f"""
            UPDATE tasks
            SET status = 'done',
                summary = CAST(:summary AS jsonb),
                locked_by = NULL,
                lease_expires_at = NULL,
                error_msg = NULL,
                updated_at = {_SQL_UTC_NOW}
            WHERE id = :task_id
              AND locked_by = :worker_id
            """
        )
        result = await self.session.execute(
            sql,
            {
                "task_id": task_id,
                "worker_id": worker_id,
                "summary": json.dumps(summary, ensure_ascii=False),
            },
        )
        return bool(result.rowcount)

    async def mark_failure(
        self,
        task_id: UUID,
        *,
        worker_id: str,
        error_msg: str,
        max_retries: int,
    ) -> Optional[tuple[int, str]]:
        """Bump retry_count; return (retry_count, status) or None if not locked."""
        sql = text(
            f"""
            UPDATE tasks AS t
            SET
                retry_count = x.new_count,
                error_msg = :error_msg,
                locked_by = NULL,
                lease_expires_at = NULL,
                updated_at = {_SQL_UTC_NOW},
                status = CASE
                    WHEN x.new_count <= :max_retries THEN 'pending'
                    ELSE 'failed'
                END,
                next_retry_at = CASE
                    WHEN x.new_count <= :max_retries THEN
                        {_SQL_UTC_NOW}
                        + (power(2, x.new_count)::text || ' seconds')::interval
                    ELSE t.next_retry_at
                END
            FROM (
                SELECT id, retry_count + 1 AS new_count
                FROM tasks
                WHERE id = :task_id
                  AND locked_by = :worker_id
            ) AS x
            WHERE t.id = x.id
            RETURNING t.retry_count, t.status
            """
        )
        result = await self.session.execute(
            sql,
            {
                "task_id": task_id,
                "worker_id": worker_id,
                "error_msg": (error_msg or "")[:1000],
                "max_retries": max_retries,
            },
        )
        row = result.fetchone()
        if row is None:
            return None
        return int(row[0]), str(row[1])

    async def reclaim_in_flight(
        self,
        *,
        worker_id: str,
        include_own_locks: bool,
        error_msg: str,
    ) -> int:
        """Reset expired (and optionally this worker's) in-flight tasks to pending."""
        if include_own_locks:
            sql = text(
                f"""
                UPDATE tasks
                SET status = 'pending',
                    error_msg = :error_msg,
                    next_retry_at = {_SQL_UTC_NOW},
                    locked_by = NULL,
                    lease_expires_at = NULL,
                    updated_at = {_SQL_UTC_NOW}
                WHERE status IN :statuses
                  AND (
                    locked_by = :worker_id
                    OR (
                        lease_expires_at IS NOT NULL
                        AND lease_expires_at < {_SQL_UTC_NOW}
                    )
                  )
                """
            ).bindparams(bindparam("statuses", expanding=True))
            params: dict[str, Any] = {
                "error_msg": error_msg,
                "worker_id": worker_id,
                "statuses": list(IN_FLIGHT_STATUS_VALUES),
            }
        else:
            sql = text(
                f"""
                UPDATE tasks
                SET status = 'pending',
                    error_msg = :error_msg,
                    next_retry_at = {_SQL_UTC_NOW},
                    locked_by = NULL,
                    lease_expires_at = NULL,
                    updated_at = {_SQL_UTC_NOW}
                WHERE status IN :statuses
                  AND lease_expires_at IS NOT NULL
                  AND lease_expires_at < {_SQL_UTC_NOW}
                """
            ).bindparams(bindparam("statuses", expanding=True))
            params = {
                "error_msg": error_msg,
                "statuses": list(IN_FLIGHT_STATUS_VALUES),
            }

        result = await self.session.execute(sql, params)
        return int(result.rowcount or 0)

    async def reset_aborted(
        self,
        task_ids: Sequence[UUID],
        *,
        worker_id: str,
        error_msg: str,
    ) -> int:
        if not task_ids:
            return 0
        sql = text(
            f"""
            UPDATE tasks
            SET status = 'pending',
                error_msg = :error_msg,
                next_retry_at = {_SQL_UTC_NOW},
                locked_by = NULL,
                lease_expires_at = NULL,
                updated_at = {_SQL_UTC_NOW}
            WHERE id IN :task_ids
              AND status IN :statuses
              AND locked_by = :worker_id
            """
        ).bindparams(
            bindparam("task_ids", expanding=True),
            bindparam("statuses", expanding=True),
        )
        result = await self.session.execute(
            sql,
            {
                "task_ids": list(task_ids),
                "statuses": list(IN_FLIGHT_STATUS_VALUES),
                "worker_id": worker_id,
                "error_msg": error_msg,
            },
        )
        return int(result.rowcount or 0)
