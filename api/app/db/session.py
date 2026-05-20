from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

# ---------------------------------------------------------------------------
# Async engine
# pool_size / max_overflow tuned for a typical API workload.
# Use NullPool in tests to avoid connection leaks across test isolation.
# ---------------------------------------------------------------------------
engine = create_async_engine(
    settings.database_url,
    echo=settings.is_development,   # log SQL in dev
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,             # verify connections before use (handles DB restarts)
    pool_recycle=3600,              # recycle connections after 1hr to avoid stale state
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,         # don't expire attributes after commit (avoids lazy-load errors in async)
    autocommit=False,
    autoflush=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency — provides a database session per request.
    Commits on success, rolls back on any exception.
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


@asynccontextmanager
async def get_db_context() -> AsyncGenerator[AsyncSession, None]:
    """
    Context manager version for use outside FastAPI dependency injection
    (e.g. background tasks, scripts).
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()