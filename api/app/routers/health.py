from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db

router = APIRouter(tags=["health"])


@router.get("/health", summary="Health check")
async def health():
    return {"status": "ok"}


@router.get("/health/db", summary="Database connectivity check")
async def health_db(db: AsyncSession = Depends(get_db)):
    await db.execute(text("SELECT 1"))
    return {"status": "ok", "database": "connected"}


# JUST INFORMATIONAL ENDPOINTS -> after debezium implementation these are not the right way to check lag between database and debezium

@router.get("/health/outbox", summary="Outbox backlog monitor")
async def health_outbox(db: AsyncSession = Depends(get_db)):
    """
    Returns outbox event counts by status.
    Large PENDING counts or old PENDING events indicate CDC lag.
    """
    result = await db.execute(text("SELECT * FROM outbox_monitor"))
    rows = result.mappings().all()
    return {"outbox": [dict(r) for r in rows]}


@router.get("/health/outbox/lag", summary="Detect stuck outbox events")
async def health_outbox_lag(db: AsyncSession = Depends(get_db)):
    """Events pending for >60s — should be empty in a healthy system."""
    result = await db.execute(text("SELECT * FROM outbox_pending_lag LIMIT 20"))
    rows = result.mappings().all()
    return {
        "stuck_events": [dict(r) for r in rows],
        "count": len(rows),
        "healthy": len(rows) == 0,
    }