from contextlib import asynccontextmanager
from api.core.db.db_connector import AsyncSessionFactory
@asynccontextmanager
async def _session():
    async with AsyncSessionFactory() as s:
        yield s


class TaskRepository:
    "任务"