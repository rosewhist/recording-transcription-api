"""Task table model and DAO (PostgreSQL via SQLModel)."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional, Sequence
from uuid import UUID, uuid4

from sqlalchemy import Column, DateTime, Index, and_, case, func, or_, text, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel, delete, select
from sqlmodel.ext.asyncio.session import AsyncSession

from api.repository.dao.task_types import IN_FLIGHT_STATUS_VALUES, TaskStatus

__all__ = [
    "Task",
    "TaskDao",
    "TaskStatus",
]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _sql_utc_now():
    """SQL-side UTC so lease / retry clocks match across worker paths."""
    return func.timezone("utc", func.now())


def _make_interval_secs(seconds):
    # SQLAlchemy 2.0.x Function does not accept make_interval(secs=...); use positionals.
    return func.make_interval(0, 0, 0, 0, 0, 0, seconds)


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
    next_retry_at: datetime = Field(
        default_factory=_utcnow,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
    lease_expires_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True), nullable=True),
    )
    transcript: Optional[str] = Field(default=None)
    error_msg: Optional[str] = Field(default=None)
    created_at: datetime = Field(
        default_factory=_utcnow,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
    updated_at: datetime = Field(
        default_factory=_utcnow,
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )

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

    async def list_by_recording_ids(self, recording_ids: Sequence[UUID]) -> list[Task]:
        if not recording_ids:
            return []
        stmt = select(Task).where(Task.recording_id.in_(recording_ids))
        return list((await self.session.scalars(stmt)).all())

    async def delete_by_recording(self, recording_id: UUID) -> int:
        result = await self.session.execute(
            delete(Task).where(Task.recording_id == recording_id)
        )
        return int(result.rowcount or 0)

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

        utc_now = _sql_utc_now()

        # Hold row locks until the surrounding transaction commits/rolls back.
        pick_stmt = (
            select(Task.id)
            .where(
                Task.status == TaskStatus.PENDING.value,
                Task.next_retry_at <= utc_now,
            )
            .order_by(Task.created_at.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        task_ids = list(await self.session.scalars(pick_stmt))
        if not task_ids:
            return []

        claim_stmt = (
            update(Task)
            .where(Task.id.in_(task_ids))
            .values(
                status=TaskStatus.TRANSCRIBING.value,
                locked_by=worker_id,
                lease_expires_at=utc_now + _make_interval_secs(lease_seconds),
                updated_at=utc_now,
            )
            .returning(Task.id)
            .execution_options(synchronize_session=False)
        )
        result = await self.session.execute(claim_stmt)
        return list(result.scalars().all())

    async def renew_lease(
        self,
        task_id: UUID,
        *,
        worker_id: str,
        lease_seconds: int,
    ) -> bool:
        """Extend lease with SQL UTC clock. Returns False if lock is gone."""
        utc_now = _sql_utc_now()
        stmt = (
            update(Task)
            .where(Task.id == task_id, Task.locked_by == worker_id)
            .values(
                lease_expires_at=utc_now + _make_interval_secs(lease_seconds),
                updated_at=utc_now,
            )
            .execution_options(synchronize_session=False)
        )
        result = await self.session.execute(stmt)
        return bool(result.rowcount)

    async def mark_summarizing(
        self,
        task_id: UUID,
        *,
        worker_id: str,
        transcript: str,
    ) -> bool:
        stmt = (
            update(Task)
            .where(Task.id == task_id, Task.locked_by == worker_id)
            .values(
                status=TaskStatus.SUMMARIZING.value,
                transcript=transcript,
                updated_at=_sql_utc_now(),
            )
            .execution_options(synchronize_session=False)
        )
        result = await self.session.execute(stmt)
        return bool(result.rowcount)

    async def mark_done(
        self,
        task_id: UUID,
        *,
        worker_id: str,
        summary: dict[str, Any],
    ) -> bool:
        stmt = (
            update(Task)
            .where(Task.id == task_id, Task.locked_by == worker_id)
            .values(
                status=TaskStatus.DONE.value,
                summary=summary,
                locked_by=None,
                lease_expires_at=None,
                error_msg=None,
                updated_at=_sql_utc_now(),
            )
            .execution_options(synchronize_session=False)
        )
        result = await self.session.execute(stmt)
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
        utc_now = _sql_utc_now()
        new_count = Task.retry_count + 1
        can_retry = new_count <= max_retries
        stmt = (
            update(Task)
            .where(Task.id == task_id, Task.locked_by == worker_id)
            .values(
                retry_count=new_count,
                error_msg=(error_msg or "")[:1000],
                locked_by=None,
                lease_expires_at=None,
                updated_at=utc_now,
                status=case(
                    (can_retry, TaskStatus.PENDING.value),
                    else_=TaskStatus.FAILED.value,
                ),
                next_retry_at=case(
                    (
                        can_retry,
                        utc_now + _make_interval_secs(func.power(2, new_count)),
                    ),
                    else_=Task.next_retry_at,
                ),
            )
            .returning(Task.retry_count, Task.status)
            .execution_options(synchronize_session=False)
        )
        result = await self.session.execute(stmt)
        row = result.first()
        if row is None:
            return None
        return int(row[0]), str(row[1])

    async def requeue_failed(self, task_id: UUID) -> Optional[Task]:
        """Atomically move failed → pending for manual retry; None if not failed."""
        utc_now = _sql_utc_now()
        stmt = (
            update(Task)
            .where(Task.id == task_id, Task.status == TaskStatus.FAILED.value)
            .values(
                status=TaskStatus.PENDING.value,
                retry_count=0,
                error_msg=None,
                transcript=None,
                summary=None,
                locked_by=None,
                lease_expires_at=None,
                next_retry_at=utc_now,
                updated_at=utc_now,
            )
            .returning(Task.id)
            .execution_options(synchronize_session=False)
        )
        result = await self.session.execute(stmt)
        row = result.first()
        if row is None:
            return None
        return await self.get_by_id(row[0])

    async def reclaim_in_flight(
        self,
        *,
        worker_id: str,
        include_own_locks: bool,
        error_msg: str,
    ) -> int:
        """Reset expired (and optionally this worker's) in-flight tasks to pending."""
        utc_now = _sql_utc_now()
        lease_expired = and_(
            Task.lease_expires_at.is_not(None),
            Task.lease_expires_at < utc_now,
        )
        if include_own_locks:
            owner_filter = or_(Task.locked_by == worker_id, lease_expired)
        else:
            owner_filter = lease_expired

        stmt = (
            update(Task)
            .where(
                Task.status.in_(IN_FLIGHT_STATUS_VALUES),
                owner_filter,
            )
            .values(
                status=TaskStatus.PENDING.value,
                error_msg=error_msg,
                next_retry_at=utc_now,
                locked_by=None,
                lease_expires_at=None,
                updated_at=utc_now,
            )
            .execution_options(synchronize_session=False)
        )
        result = await self.session.execute(stmt)
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
        utc_now = _sql_utc_now()
        stmt = (
            update(Task)
            .where(
                Task.id.in_(list(task_ids)),
                Task.status.in_(IN_FLIGHT_STATUS_VALUES),
                Task.locked_by == worker_id,
            )
            .values(
                status=TaskStatus.PENDING.value,
                error_msg=error_msg,
                next_retry_at=utc_now,
                locked_by=None,
                lease_expires_at=None,
                updated_at=utc_now,
            )
            .execution_options(synchronize_session=False)
        )
        result = await self.session.execute(stmt)
        return int(result.rowcount or 0)
