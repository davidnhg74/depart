"""Queue-enqueue helpers.

Encapsulates the arq Redis pool so callers don't have to poke at arq
internals. Two reasons to isolate this:

  1. Testability — tests monkeypatch `enqueue_migration` with a noop
     or a synchronous stand-in instead of requiring a real Redis.
  2. Graceful fallback — if Redis is unreachable at enqueue time we
     fall back to FastAPI's in-process BackgroundTasks so the
     self-hosted app still works when an operator hasn't started the
     worker container yet. A warning gets logged.

The API endpoint passes a BackgroundTasks alongside the migration id;
if Redis fails, we register the runner there instead.
"""

from __future__ import annotations

import logging
from typing import Optional
from urllib.parse import urlparse

from arq import create_pool
from arq.connections import ArqRedis, RedisSettings
from fastapi import BackgroundTasks

from ..config import settings


logger = logging.getLogger(__name__)


_pool: Optional[ArqRedis] = None


async def _get_pool() -> ArqRedis:
    """Lazily construct the arq Redis pool. Cached for process
    lifetime — arq's pool is connection-pooled underneath, so we
    don't need one per request."""
    global _pool
    if _pool is not None:
        return _pool
    parsed = urlparse(settings.redis_url)
    _pool = await create_pool(
        RedisSettings(
            host=parsed.hostname or "localhost",
            port=parsed.port or 6379,
            database=int(parsed.path.lstrip("/") or "0"),
            password=parsed.password,
        )
    )
    return _pool


async def enqueue_migration(
    migration_id: str, background: Optional[BackgroundTasks] = None
) -> str:
    """Enqueue the migration runner onto the arq queue, or fall back
    to FastAPI BackgroundTasks if Redis is unavailable.

    Returns the arq job id on success, or "inline:<migration_id>" on
    fallback (useful for audit log details so operators can tell
    which path ran)."""
    try:
        pool = await _get_pool()
        job = await pool.enqueue_job("run_migration_job", migration_id)
        if job:
            return job.job_id
    except Exception as exc:  # noqa: BLE001 — Redis can fail many ways
        logger.warning(
            "arq enqueue failed (%s: %s); falling back to in-process BackgroundTasks",
            type(exc).__name__,
            exc,
        )

    # Fallback: register the sync runner on the FastAPI task queue.
    # We import here to avoid a circular import at module load.
    if background is None:
        raise RuntimeError(
            "arq enqueue failed and no BackgroundTasks fallback was provided"
        )
    from ..db import get_session_factory
    from .migration_runner import run_migration

    def _run_inline() -> None:
        db = get_session_factory()()
        try:
            run_migration(db, migration_id)
        finally:
            db.close()

    background.add_task(_run_inline)
    return f"inline:{migration_id}"
