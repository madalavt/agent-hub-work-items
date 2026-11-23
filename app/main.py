"""Work Item Manager API. Run with: ``uvicorn app.main:app --port 8003``"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

from app import health
from app.config import get_settings
from app.db import init_db, lock_engine, startup_lock
from app.routes import router

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    init_db(
        settings.database_url,
        direct_url=settings.direct_database_url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        statement_timeout_ms=settings.db_statement_timeout_ms,
        lock_timeout_ms=settings.db_lock_timeout_ms,
    )
    # Several workers / replicas start together: seed one at a time.
    with startup_lock(lock_engine()):
        if settings.seed_default_queues:
            from app.seed import seed_default_queues

            seed_default_queues()
        if settings.seed_demo_data:
            from app.seed import seed_demo

            seed_demo()
    yield


class TrailingSlashMiddleware:
    """Route ``/items/`` and ``/items`` alike without a redirect (a 307 would make clients
    resend POST/PUT bodies, and some don't). RootsQueueClient mixes both forms."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            path = scope["path"]
            if len(path) > 1 and path.endswith("/"):
                scope = dict(scope)
                scope["path"] = path.rstrip("/") or "/"
                raw = scope.get("raw_path")
                if raw:
                    scope["raw_path"] = raw.rstrip(b"/") or b"/"
        await self.app(scope, receive, send)


app = FastAPI(
    title="Agent Hub · Work Item Manager",
    version="0.1.0",
    description=(
        "The work item queue every automation project uses. Paths and payloads match the "
        "Roots Queue / work_item_mgr API, so RootsQueueClient works by pointing `base_url` here. "
        "Auth: `Ocp-Apim-Subscription-Key` (or `X-API-Key`) when API_KEY is set. "
        "The `Resource` header names the caller."
    ),
    lifespan=lifespan,
    redirect_slashes=False,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=get_settings().cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(TrailingSlashMiddleware)

# Conflicts a retry resolves (Postgres SQLSTATE). Callers (and APIM) retry on 503.
_TRANSIENT = {
    "40P01": "deadlock",
    "40001": "serialization conflict",
    "55P03": "lock wait timeout",
    "57014": "statement timeout",
}


@app.exception_handler(DBAPIError)
async def database_error(request: Request, exc: DBAPIError) -> JSONResponse:
    reason = _TRANSIENT.get(getattr(exc.orig, "sqlstate", None) or "")
    if reason is None:
        logging.getLogger(__name__).exception("database error on %s", request.url.path)
        return JSONResponse({"detail": "Database error"}, status_code=500)
    logging.getLogger(__name__).warning("%s on %s %s", reason, request.method, request.url.path)
    return JSONResponse(
        {"detail": f"Temporary database {reason}; retry the request"},
        status_code=503,
        headers={"Retry-After": "1"},
    )


@app.exception_handler(PoolTimeoutError)
async def pool_exhausted(request: Request, exc: PoolTimeoutError) -> JSONResponse:
    logging.getLogger(__name__).warning("connection pool exhausted on %s", request.url.path)
    return JSONResponse(
        {"detail": "Server busy; retry the request"}, status_code=503, headers={"Retry-After": "1"}
    )


app.include_router(router)


@app.get("/health/live", tags=["health"], summary="Liveness: the process is up")
def health_live() -> dict:
    return health.liveness()


@app.get(
    "/health",
    tags=["health"],
    summary="Readiness: database (critical) and annotations API (degrades only)",
    responses={503: {"description": "Database is down"}},
)
def health_ready(response: Response) -> dict:
    code, body = health.readiness(get_settings())
    response.status_code = code
    return body


@app.get("/health_check", tags=["health"], summary="Health check (Roots Queue path)")
def health_check(response: Response) -> dict:
    code, body = health.readiness(get_settings())
    response.status_code = code
    return body
