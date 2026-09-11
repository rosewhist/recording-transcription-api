from datetime import datetime, timezone
from typing import Optional, Sequence
from uuid import UUID, uuid4

from sqlalchemy import func
from sqlmodel import Field, SQLModel, col, delete, select
from sqlmodel.ext.asyncio.session import AsyncSession


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Recording(SQLModel, table=True):
    __tablename__ = "recordings"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    file_name: str = Field(max_length=255)
    file_path: str = Field(max_length=1000)
    file_size: int = Field(default=0)
    file_hash: str = Field(max_length=64, unique=True, index=True)
    created_at: datetime = Field(default_factory=_utcnow, index=True)
    updated_at: datetime = Field(default_factory=_utcnow)


class RecordingDao:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, recording: Recording) -> Recording:
        self.session.add(recording)
        await self.session.flush()
        await self.session.refresh(recording)
        return recording

    async def get_by_hash(self, file_hash: str) -> Optional[Recording]:
        stmt = select(Recording).where(Recording.file_hash == file_hash)
        return (await self.session.scalars(stmt)).first()

    async def get_by_id(self, recording_id: UUID) -> Optional[Recording]:
        return await self.session.get(Recording, recording_id)

    async def count_all(self) -> int:
        stmt = select(func.count()).select_from(Recording)
        return int((await self.session.scalar(stmt)) or 0)

    async def list_page(self, *, offset: int, limit: int) -> Sequence[Recording]:
        stmt = (
            select(Recording)
            .order_by(col(Recording.created_at).desc())
            .offset(offset)
            .limit(limit)
        )
        return (await self.session.scalars(stmt)).all()

    async def delete(self, recording_id: UUID) -> bool:
        result = await self.session.execute(
            delete(Recording).where(Recording.id == recording_id)
        )
        return bool(result.rowcount)
