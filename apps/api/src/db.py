from __future__ import annotations

from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from contextlib import contextmanager
from .config import settings

Base = declarative_base()

_engine: Engine | None = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(
            settings.database_url,
            echo=settings.environment == "development",
            pool_pre_ping=True,
        )
    return _engine


def get_session_factory() -> sessionmaker:
    return sessionmaker(autocommit=False, autoflush=False, bind=get_engine())


def get_db() -> Session:
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def get_db_context():
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Schema is owned by Alembic. Tests that need a fresh DB should run migrations,
# not call Base.metadata.create_all() — keeping the two paths in sync is a known
# trap that we choose not to inherit.
