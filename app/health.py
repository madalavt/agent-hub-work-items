"""Health checks for the Work Item Manager.

``GET /health/live``  the process is up.
``GET /health``       database (critical) plus queue counts; the annotations API (linked
                      documents) only degrades. 503 when the database is down.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from importlib import metadata
from typing import Any

import httpx
from sqlalchemy import func, select, text

from app import models
from app.config import Settings
from app.db import session_scope
from app.db_read import read_status
from app.models import Item, Queue

SERVICE = "agent-hub-work-items"
STARTED_AT = time.monotonic()


def version() -> str:
    try:
        return metadata.version(SERVICE)
    except metadata.PackageNotFoundError:  # pragma: no cover
        return "dev"


def liveness() -> dict[str, Any]:
    return {
        "service": SERVICE,
        "status": "ok",
        "version": version(),
        "uptime_s": round(time.monotonic() - STARTED_AT, 1),
    }


def _error(exc: Exception) -> str:
    first = str(exc).splitlines()[0] if str(exc) else ""
    return f"{type(exc).__name__}: {first}"[:300]


def _database(settings: Settings) -> dict[str, Any]:
    started = time.monotonic()
    check: dict[str, Any] = {"critical": True, "details": {}, "error": None}
    try:
        with session_scope() as session:
            session.execute(text("SELECT 1"))
            stale = datetime.now(UTC) - timedelta(minutes=settings.stale_lock_minutes)
            check["details"] = {
                "dialect": session.get_bind().dialect.name,
                "queues": session.scalar(
                    select(func.count()).select_from(Queue).where(Queue.deleted_at.is_(None))
                ),
                "items": session.scalar(
                    select(func.count()).select_from(Item).where(Item.deleted_at.is_(None))
                ),
                "in_progress": session.scalar(
                    select(func.count())
                    .select_from(Item)
                    .where(Item.status == models.IN_PROGRESS, Item.deleted_at.is_(None))
                ),
                "stale_locks": session.scalar(
                    select(func.count())
                    .select_from(Item)
                    .where(
                        Item.locked.is_(True),
                        Item.last_locked_at < stale,
                        Item.deleted_at.is_(None),
                    )
                ),
            }
        check["status"] = "ok"
    except Exception as exc:  # noqa: BLE001 - report, don't raise
        check["status"] = "down"
        check["error"] = _error(exc)
    check["latency_ms"] = round((time.monotonic() - started) * 1000, 1)
    return check


def _annotations(settings: Settings) -> dict[str, Any]:
    started = time.monotonic()
    url = f"{settings.annotations_api_url.rstrip('/')}/health"
    check: dict[str, Any] = {"critical": False, "details": {"url": url}, "error": None}
    try:
        headers = (
            {"Ocp-Apim-Subscription-Key": settings.annotations_api_key}
            if settings.annotations_api_key
            else {}
        )
        r = httpx.get(url, headers=headers, timeout=3)
        check["status"] = "ok" if r.status_code == 200 else "degraded"
        if r.status_code != 200:
            check["error"] = f"HTTP {r.status_code}: linked documents unavailable"
    except httpx.HTTPError as exc:
        check["status"] = "degraded"
        check["error"] = f"{_error(exc)} (linked documents unavailable)"
    check["latency_ms"] = round((time.monotonic() - started) * 1000, 1)
    return check


def readiness(settings: Settings) -> tuple[int, dict[str, Any]]:
    checks = {"database": _database(settings), "annotations_api": _annotations(settings)}
    if checks["database"]["status"] == "down":
        status = "down"
    elif any(c["status"] != "ok" for c in checks.values()):
        status = "degraded"
    else:
        status = "ok"
    body = {
        **liveness(),
        "status": status,
        "checked_at": datetime.now(UTC).isoformat(),
        "checks": checks,
        # Informational: reads fall back to the primary, so this never fails readiness.
        "read_replica": read_status(),
    }
    return (503 if status == "down" else 200), body
