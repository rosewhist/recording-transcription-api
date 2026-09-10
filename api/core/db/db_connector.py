import logging
from typing import Annotated, AsyncGenerator

from fastapi import Depends
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

from api.config.settings import get_settings

logger = logging.getLogger(__name__)

settings = get_settings()


def _build_engine():
    url = settings.DATABASE_URL
    if url.startswith("sqlite"):
        # SQLite 不支持连接池；NullPool 避免跨事件循环复用连接
        return create_async_engine(
            url=url,
            poolclass=NullPool,
            connect_args={"check_same_thread": False},
        )
    return create_async_engine(
        url=url,
        pool_size=settings.POOL_SIZE,
        pool_timeout=settings.POOL_TIMEOUT,
        pool_pre_ping=True,  # 避免 PostgreSQL 重启后连接池里的死连接
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


async def create_db_and_tables():
    # 确保模型已注册到 SQLModel.metadata（避免空库建表）
    import api.repository.dao  # noqa: F401

    try:
        table_names = list(SQLModel.metadata.tables.keys())
        logger.info("待创建的表: %s", table_names)
        if not table_names:
            logger.warning("SQLModel.metadata 为空，跳过 create_all")
            return
        async with engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.create_all)
        logger.info("数据库表创建/检查完成")
    except Exception as e:
        logger.error("数据库初始化失败: %s", e)
        raise
