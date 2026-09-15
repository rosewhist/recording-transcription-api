from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, Optional
from uuid import UUID

from sqlalchemy.exc import IntegrityError
from sqlmodel.ext.asyncio.session import AsyncSession

from api.core.db.db_connector import AsyncSessionFactory
from api.repository.dao import Recording, Task
from api.repository.dao.recording import RecordingDao
from api.repository.dao.task import TaskDao


@asynccontextmanager
async def _session():
    async with AsyncSessionFactory() as s:
        yield s


async def _by_hash(
    session: AsyncSession, file_hash: str
) -> tuple[Optional[Recording], Optional[Task]]:
    recording = await RecordingDao(session).get_by_hash(file_hash)
    if recording is None:
        return None, None
    task = await TaskDao(session).get_by_recording(recording.id)
    return recording, task


class RecordingRepository:
    async def create_recording_and_task(
        self,
        *,
        file_name: str,
        file_path: str,
        file_size: int,
        file_hash: str,
    ) -> tuple[Recording, Optional[Task], bool]:
        async with _session() as session:
            recording_dao = RecordingDao(session)
            task_dao = TaskDao(session)

            try:
                recording = await recording_dao.get_by_hash(file_hash)
                if recording is not None:
                    return recording, await task_dao.get_by_recording(recording.id), False

                recording = Recording(
                    file_name=file_name,
                    file_path=file_path,
                    file_size=file_size,
                    file_hash=file_hash,
                )
                await recording_dao.create(recording)
                task = Task(recording_id=recording.id)
                await task_dao.create(task)
                await session.commit()
                return recording, task, True
            except IntegrityError:
                await session.rollback()
                existing = await recording_dao.get_by_hash(file_hash)
                if existing is None:
                    raise
                return existing, await task_dao.get_by_recording(existing.id), False

    async def get_by_hash(
        self, file_hash: str
    ) -> tuple[Optional[Recording], Optional[Task]]:
        async with _session() as session:
            return await _by_hash(session, file_hash)

    async def get_by_id(
        self, recording_id: UUID
    ) -> tuple[Optional[Recording], Optional[Task]]:
        async with _session() as session:
            recording = await RecordingDao(session).get_by_id(recording_id)
            if recording is None:
                return None, None
            task = await TaskDao(session).get_by_recording(recording.id)
            return recording, task

    async def set_task_summary_if_absent(
        self, task_id: UUID, *, summary: dict[str, Any]
    ) -> bool:
        """Best-effort writeback of an SSE-generated summary (never overwrites)."""
        async with _session() as session:
            saved = await TaskDao(session).set_summary_if_absent(
                task_id, summary=summary
            )
            if saved:
                await session.commit()
            return saved

    async def list_page(
        self, *, page: int, page_size: int
    ) -> tuple[list[tuple[Recording, Optional[Task]]], int]:
        offset = (page - 1) * page_size
        async with _session() as session:
            recording_dao = RecordingDao(session)
            task_dao = TaskDao(session)
            total = await recording_dao.count_all()
            recordings = list(await recording_dao.list_page(offset=offset, limit=page_size))
            if not recordings:
                return [], total
            tasks = await task_dao.list_by_recording_ids([r.id for r in recordings])
            by_recording = {t.recording_id: t for t in tasks}
            items = [(r, by_recording.get(r.id)) for r in recordings]
            return items, total

    async def delete(self, recording_id: UUID) -> Optional[str]:
        async with _session() as session:
            recording_dao = RecordingDao(session)
            task_dao = TaskDao(session)
            recording = await recording_dao.get_by_id(recording_id)
            if recording is None:
                return None
            file_path = recording.file_path
            await task_dao.delete_by_recording(recording.id)
            await recording_dao.delete(recording.id)
            await session.commit()
            return file_path
