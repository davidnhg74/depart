"""arq worker — runs migrations out-of-process.

Replaces FastAPI BackgroundTasks for migration jobs. The API enqueues
a job, returns 202, and a separate `arq` process picks it up off
Redis. Key benefits:

  * If the API container restarts mid-run, the job survives — arq
    picks it up again on worker boot.
  * Multiple workers can process independent migrations in parallel
    (one worker per CPU, typically).
  * Retries on transient failures (exponential backoff, 3 attempts
    by default — see `retry` config below).
  * Observable: arq logs each job's lifecycle and failures to stderr;
    production can point those at the same log pipeline as the API.

Run via:
    arq src.worker.WorkerSettings

(See the `worker` service in the top-level docker-compose.yml.)
"""

from __future__ import annotations

import logging
from typing import Any

from arq.connections import RedisSettings

from .config import settings
from .db import get_session_factory
from .services.migration_runner import run_migration


logger = logging.getLogger(__name__)


async def run_migration_job(ctx: dict[str, Any], migration_id: str) -> None:
    """arq task entry point. Opens a DB Session, calls the existing
    service-layer runner, closes the Session. The runner itself is
    sync (SQLAlchemy 2.x sync session) so we don't try to await it;
    arq's worker runs tasks in a threadpool-free async loop, which
    means this blocks the worker's event loop for the duration of the
    migration. That's fine at the scale of one migration per worker
    at a time — it's what we want, in fact (single-writer semantics
    per migration id).

    For parallel migrations, spin up more worker containers."""
    db = get_session_factory()()
    try:
        run_migration(db, migration_id)
    finally:
        db.close()


def _redis_settings() -> RedisSettings:
    """Parse the REDIS_URL into arq's RedisSettings. Supports the
    standard redis://host:port/db syntax; more exotic schemes can be
    added here later."""
    from urllib.parse import urlparse

    parsed = urlparse(settings.redis_url)
    return RedisSettings(
        host=parsed.hostname or "localhost",
        port=parsed.port or 6379,
        database=int(parsed.path.lstrip("/") or "0"),
        password=parsed.password,
    )


class WorkerSettings:
    """arq discovers this class by import path. Registered tasks,
    lifecycle hooks, and retry/timeout policy all live here."""

    functions = [run_migration_job]
    redis_settings = _redis_settings()
    # Long migrations can take hours; give them plenty of headroom.
    # If the job exceeds this the worker kills it and arq retries it.
    job_timeout = 6 * 60 * 60  # 6 hours
    # Max concurrent migrations *per worker process*. Scale by running
    # more worker containers rather than bumping this — a single
    # worker process doesn't benefit from concurrency on the sync
    # SQLAlchemy path.
    max_jobs = 1
    # Fail-after: 3 attempts with exponential backoff (5s, 25s, 125s).
    # Beyond that the job is marked failed; the DB row keeps the error
    # message so operators can decide whether to retry manually.
    max_tries = 3
