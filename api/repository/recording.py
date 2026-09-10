from contextlib import asynccontextmanager
from api.core.db.db_connector import AsyncSessionFactory


@asynccontextmanager
async def _session():
    async with AsyncSessionFactory() as s:
        yield s


class RecordingRepository:
    """录音 + 任务（一对一）的聚合操作。"""
