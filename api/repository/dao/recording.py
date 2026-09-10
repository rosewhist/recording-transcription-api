from datetime import datetime, timezone
from uuid import UUID, uuid4
from sqlmodel import Field, SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

class Recording(SQLModel, table=True):
    __tablename__ = "recordings"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    file_name: str = Field(max_length=255)
    file_path: str = Field(max_length=1000)
    file_size: int = Field(default=0)
    file_hash: str = Field(max_length=64, unique=True, index=True)  # sha256，用于上传幂等
    created_at: datetime = Field(default_factory=_utcnow, index=True)
    updated_at: datetime = Field(default_factory=_utcnow)
class RecordingDao:
    """录音表的基础查询封装。"""

    def __init__(self, session: AsyncSession):
        self.session = session
