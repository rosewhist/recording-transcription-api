import logging
from typing import Annotated, AsyncGenerator

from fastapi import Depends
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession

from api.config.settings import get_settings

logger = logging.getLogger(__name__)

settings = get_settings()


def _build_engine():
    return create_async_engine(
        url=settings.DATABASE_URL,
        pool_size=settings.POOL_SIZE,
        pool_timeout=settings.POOL_TIMEOUT,
        pool_pre_ping=True,
    )


engine = _build_engine()

AsyncSessionFactory = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """请求级会话：成功则提交，异常则回滚。

    注意：Repository 若自行用 AsyncSessionFactory 开短事务，应在内部显式 commit，
    不要与本依赖混用同一请求里的两套事务边界。
    """
    async with AsyncSessionFactory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


AsyncSessionDep = Annotated[AsyncSession, Depends(get_async_session)]
