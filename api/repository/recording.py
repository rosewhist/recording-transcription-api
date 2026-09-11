from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Optional

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
