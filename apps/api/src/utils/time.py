"""Naive-UTC timestamp helper.

`datetime.utcnow()` is deprecated on Python 3.12+ and will be removed.
The straightforward replacement `datetime.now(timezone.utc)` returns a
tz-aware value, which changes semantics: our SQLAlchemy columns are
`DateTime` (naive) and downstream comparisons assume naive — mixing
aware and naive raises `TypeError`.

`utc_now()` preserves the old naive-UTC behavior by stripping the
tzinfo, which lets us silence the deprecation without touching schemas
or migration-sensitive equality/ordering code.

Call sites that explicitly need an aware datetime (e.g. PyJWT payloads)
should keep using `datetime.now(timezone.utc)` directly."""

from __future__ import annotations

from datetime import datetime, timezone


def utc_now() -> datetime:
    """Return the current UTC time as a *naive* datetime — same shape
    the deprecated `datetime.utcnow()` produced."""
    return datetime.now(timezone.utc).replace(tzinfo=None)
