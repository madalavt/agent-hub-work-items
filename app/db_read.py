"""Read-only queries on a Postgres read replica, falling back to the primary.

Off by default: without ``DB_READ_URL`` every read uses the primary, exactly as before. With it
(e.g. the Azure read replica, through its built-in PgBouncer on port 6432):

* ``get_read_session`` (FastAPI dependency) / ``read_session()`` give a **read-only** session on
  the replica. Read-only is enforced on the primary too, so a route that would write fails the
  same way in development as it would on a replica.
* The primary is used instead when the replica is unreachable (then for
  ``DB_READ_RETRY_SECONDS``, default 30, before trying it again) or when it lags more than
  ``DB_READ_MAX_LAG_SECONDS`` (default 10; checked at most every ``DB_READ_CHECK_SECONDS``,
  default 10). An idle primary does not count as lag: lag is 0 once the replica has replayed
  everything it received.
* ``run_read(fn)`` also retries ``fn`` on the primary when the replica fails mid-query; a
  session from ``read_session`` can't be retried (the request fails once and later requests
  use the primary).

Only lag-tolerant reads belong here (lists, searches, counts). Reads that must see a write just
made (read-after-write), queue claims and anything under a row lock stay on the primary.

This module is copied verbatim into each service (api, extraction, work-items, annotations).
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any, TypeVar

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import TimeoutError as PoolTimeout
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)
T = TypeVar("T")

# Seconds behind the primary; 0 on a primary and on a replica that has replayed all it received.
LAG_SQL = text(
    "SELECT CASE WHEN NOT pg_is_in_recovery() THEN 0"
    " WHEN pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn() THEN 0"
    " ELSE COALESCE(EXTRACT(EPOCH FROM now() - pg_last_xact_replay_timestamp()), 0) END"
)


# A replica cancels queries that conflict with WAL replay ("conflict with recovery").
_RECOVERY_CONFLICT = "40001"


def _sqlstate(exc: DBAPIError) -> str | None:
    return getattr(exc.orig, "sqlstate", None) or getattr(exc.orig, "pgcode", None)


def _retry_on_primary(exc: DBAPIError) -> bool:
    """Replica connection lost, or a recovery conflict: the same read can run on the primary."""
    return bool(exc.connection_invalidated) or _sqlstate(exc) == _RECOVERY_CONFLICT


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


class ReadRouter:
    def __init__(
        self,
        primary: Engine,
        replica: Engine | None = None,
        *,
        max_lag_seconds: float = 30.0,
        retry_seconds: float = 30.0,
        check_seconds: float = 10.0,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.primary = primary
        self.replica = replica
        self.max_lag_seconds = max_lag_seconds
        self.retry_seconds = retry_seconds
        self.check_seconds = check_seconds
        self._clock = clock
        self._lock = threading.Lock()
        self._down_until = 0.0
        self._checked_at: float | None = None
        self._lag_ok = True
        self.lag_seconds: float | None = None
        self.last_error: str | None = None

    # ---------------------------------------------------------------- replica health

    def mark_down(self, exc: BaseException) -> None:
        self._down_until = self._clock() + self.retry_seconds
        self._checked_at = None  # check lag again once it is back
        lines = str(exc).splitlines()
        first = lines[0][:200] if lines else ""
        self.last_error = f"{type(exc).__name__}: {first}" if first else type(exc).__name__
        logger.warning(
            "Read replica unavailable (%s); reads use the primary for %ss",
            self.last_error,
            self.retry_seconds,
        )

    def _check_lag(self) -> None:
        assert self.replica is not None
        try:
            with self.replica.connect() as conn:
                lag = float(conn.execute(LAG_SQL).scalar() or 0.0)
        except (DBAPIError, PoolTimeout) as exc:
            self.mark_down(exc)
            return
        self.lag_seconds = round(lag, 3)
        ok = lag <= self.max_lag_seconds
        if ok != self._lag_ok:
            logger.warning(
                "Read replica lag %.1fs %s %ss: reads use the %s",
                lag,
                "is within" if ok else "exceeds",
                self.max_lag_seconds,
                "replica" if ok else "primary",
            )
        self._lag_ok = ok
        self._checked_at = self._clock()

    def replica_usable(self) -> bool:
        if self.replica is None:
            return False
        now = self._clock()
        if now < self._down_until:
            return False
        if self._checked_at is None or now - self._checked_at >= self.check_seconds:
            with self._lock:  # one lag check at a time per process
                now = self._clock()
                if now < self._down_until:
                    return False
                if self._checked_at is None or now - self._checked_at >= self.check_seconds:
                    self._check_lag()
        return self._lag_ok and self._clock() >= self._down_until

    def status(self) -> dict[str, Any]:
        """For /health: whether reads go to the replica now, and why not."""
        if self.replica is None:
            return {"configured": False, "target": "primary"}
        usable = self.replica_usable()
        return {
            "configured": True,
            "target": "replica" if usable else "primary",
            "lag_seconds": self.lag_seconds,
            "max_lag_seconds": self.max_lag_seconds,
            "last_error": None if usable else self.last_error,
        }

    # ---------------------------------------------------------------- connections

    def _readonly(self, conn: Connection) -> Connection:
        if conn.dialect.name == "postgresql":
            return conn.execution_options(postgresql_readonly=True)
        return conn

    def _connect(self, prefer_replica: bool) -> tuple[Connection, bool]:
        """(read-only connection, whether it is on the replica)."""
        if prefer_replica and self.replica_usable():
            try:
                return self._readonly(self.replica.connect()), True  # type: ignore[union-attr]
            except (DBAPIError, PoolTimeout) as exc:
                self.mark_down(exc)
        return self._readonly(self.primary.connect()), False

    @contextmanager
    def session(self, prefer_replica: bool = True) -> Iterator[Session]:
        conn, on_replica = self._connect(prefer_replica)
        session = Session(bind=conn, expire_on_commit=False)
        try:
            yield session
        except DBAPIError as exc:
            if on_replica and exc.connection_invalidated:
                self.mark_down(exc)
            raise
        finally:
            try:
                try:
                    session.rollback()  # read-only: nothing to commit
                except Exception:
                    pass
                session.close()
            finally:
                conn.close()

    def run(self, fn: Callable[[Session], T]) -> T:
        """``fn(session)`` on the replica; again on the primary if the replica fails."""
        conn, on_replica = self._connect(True)
        try:
            with Session(bind=conn, expire_on_commit=False) as session:
                try:
                    return fn(session)
                finally:
                    session.rollback()
        except DBAPIError as exc:
            if not (on_replica and _retry_on_primary(exc)):
                raise
            if exc.connection_invalidated:
                self.mark_down(exc)
            else:
                logger.info("Replica read cancelled (%s); retrying on the primary", _sqlstate(exc))
        finally:
            conn.close()
        with self.session(prefer_replica=False) as session:
            return fn(session)


_router: ReadRouter | None = None


def configure_reads(primary: Engine, read_url: str | None = None) -> ReadRouter:
    """Called by init_db with the primary engine. ``read_url`` defaults to ``DB_READ_URL``."""
    global _router
    url = read_url if read_url is not None else os.environ.get("DB_READ_URL", "").strip()
    replica = None
    if url and url != primary.url.render_as_string(hide_password=False) and url.startswith("postgresql"):
        pool_size = os.environ.get("DB_READ_POOL_SIZE") or os.environ.get("DB_POOL_SIZE", "5")
        replica = create_engine(
            url,
            pool_pre_ping=True,
            pool_size=int(pool_size),
            max_overflow=int(os.environ.get("DB_MAX_OVERFLOW", "5")),
            pool_timeout=5,  # fail over to the primary quickly when the replica's pool is stuck
            pool_recycle=1800,
            connect_args={"connect_timeout": int(_env_float("DB_READ_CONNECT_TIMEOUT", 3))},
        )
        safe_url = replica.url.render_as_string(hide_password=True)
        logger.info("Read replica configured: %s", safe_url)
    if _router is not None and _router.replica is not None:
        _router.replica.dispose()
    _router = ReadRouter(
        primary,
        replica,
        max_lag_seconds=_env_float("DB_READ_MAX_LAG_SECONDS", 10),
        retry_seconds=_env_float("DB_READ_RETRY_SECONDS", 30),
        check_seconds=_env_float("DB_READ_CHECK_SECONDS", 10),
    )
    return _router


def read_router() -> ReadRouter:
    if _router is None:
        raise RuntimeError("init_db() has not been called")
    return _router


@contextmanager
def read_session() -> Iterator[Session]:
    with read_router().session() as session:
        yield session


def get_read_session() -> Iterator[Session]:
    """FastAPI dependency: a read-only session (replica when usable, else the primary)."""
    with read_session() as session:
        yield session


def run_read(fn: Callable[[Session], T]) -> T:
    return read_router().run(fn)


def read_status() -> dict[str, Any]:
    return read_router().status() if _router is not None else {"configured": False}
