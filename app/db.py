from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC
from pathlib import Path

from sqlalchemy import DateTime, create_engine, event, inspect, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from sqlalchemy.pool import NullPool, StaticPool
from sqlalchemy.types import TypeDecorator

from app.db_read import configure_reads


class Base(DeclarativeBase):
    pass


class UTCDateTime(TypeDecorator):
    """Timezone-aware UTC datetimes on every backend (SQLite drops tzinfo on its own)."""

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    def process_result_value(self, value, dialect):
        if value is not None and value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value


_engine: Engine | None = None
_SessionLocal: sessionmaker[Session] | None = None
_lock_engine: Engine | None = None


logger = logging.getLogger(__name__)

# One startup (schema upgrade, seeding) at a time across workers and replicas (Postgres).
_STARTUP_LOCK_ID = 724117


def init_db(
    database_url: str,
    *,
    direct_url: str | None = None,
    pool_size: int = 5,
    max_overflow: int = 10,
    statement_timeout_ms: int = 0,
    lock_timeout_ms: int = 0,
) -> Engine:
    """Create the engine and bring the schema up to date. Safe to call again with the same URL.

    Every uvicorn worker (and every replica) has its own pool, so the database sees
    workers × replicas × (pool_size + max_overflow) connections at most.
    """
    global _engine, _SessionLocal, _lock_engine
    if _engine is not None and _engine.url.render_as_string(hide_password=False) == database_url:
        return _engine
    pooled = bool(direct_url) and direct_url != database_url
    kwargs: dict = {"pool_pre_ping": True}
    if database_url.startswith("postgresql"):
        kwargs.update(
            pool_size=pool_size, max_overflow=max_overflow, pool_timeout=10, pool_recycle=1800
        )
        # A runaway search can't hold a connection forever, and a request blocked on a row lock
        # gives up quickly (503, retryable) instead of piling up behind it.
        options = []
        if statement_timeout_ms:
            options.append(f"-c statement_timeout={int(statement_timeout_ms)}")
        if lock_timeout_ms:
            options.append(f"-c lock_timeout={int(lock_timeout_ms)}")
        # Through PgBouncer (transaction pooling) startup options are not passed on; the same
        # timeouts are then set as the database's defaults (below).
        if options and not pooled:
            kwargs["connect_args"] = {"options": " ".join(options)}
    if database_url.startswith("sqlite"):
        path = database_url.split("///", 1)[-1]
        if path and path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        else:  # one shared connection, or every session sees its own empty database
            kwargs["poolclass"] = StaticPool
        kwargs["connect_args"] = {"check_same_thread": False, "timeout": 30}
    engine = create_engine(database_url, **kwargs)
    if database_url.startswith("sqlite"):

        @event.listens_for(engine, "connect")
        def _sqlite_pragmas(dbapi_conn, _):  # pragma: no cover - trivial
            cur = dbapi_conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL")
            cur.execute("PRAGMA foreign_keys=ON")
            cur.close()

    from app import models  # noqa: F401  (register tables)

    schema = (
        create_engine(direct_url, poolclass=NullPool, pool_pre_ping=True) if pooled else engine
    )
    ensure_schema(schema)
    if pooled:
        with startup_lock(schema):
            _database_timeouts(schema, statement_timeout_ms, lock_timeout_ms)
        _lock_engine = schema
    _engine = engine
    _SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    configure_reads(engine)  # optional read replica (DB_READ_URL)
    return engine


@contextmanager
def startup_lock(engine: Engine) -> Iterator[None]:
    """Serialize startup work (seeding) across workers and replicas. No-op on SQLite."""
    if engine.dialect.name != "postgresql":
        yield
        return
    with engine.connect() as conn:
        conn.execute(text("SELECT pg_advisory_lock(:k)"), {"k": _STARTUP_LOCK_ID})
        try:
            yield
        finally:
            conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": _STARTUP_LOCK_ID})


def lock_engine() -> Engine:
    """Engine for startup locks (seeding): the direct connection when there is one."""
    if _lock_engine is not None:
        return _lock_engine
    if _engine is None:
        raise RuntimeError("init_db() has not been called")
    return _engine


def _database_timeouts(engine: Engine, statement_ms: int, lock_ms: int) -> None:
    """Request timeouts as the database's defaults (every new server session, so also the
    connections PgBouncer opens). Startup work clears them for its own session."""
    try:
        with engine.begin() as conn:
            db_name = conn.scalar(text("SELECT current_database()"))
            for name, ms in (("statement_timeout", statement_ms), ("lock_timeout", lock_ms)):
                value = f"{int(ms)}ms" if ms else "0"
                conn.execute(text(f"ALTER DATABASE \"{db_name}\" SET {name} = '{value}'"))
    except Exception as exc:  # pragma: no cover - non-owner DB roles
        logger.warning("Could not set database-level timeouts (%s); continuing with defaults", exc)


def ensure_schema(engine: Engine) -> None:
    """Create missing tables and indexes and apply in-place upgrades (idempotent).

    ``create_all`` only creates tables that don't exist, so indexes added to the models later
    are created here, and changed ones are fixed.
    """
    with engine.begin() as conn:
        if engine.dialect.name == "postgresql":
            # Table rewrites (bigint upgrades) must not hit the request timeouts.
            conn.execute(text("SET LOCAL statement_timeout = 0"))
            conn.execute(text("SET LOCAL lock_timeout = 0"))
            conn.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": _STARTUP_LOCK_ID})
        Base.metadata.create_all(conn)
        _upgrade(conn)


# Indexes earlier versions created that newer ones replace.
_OBSOLETE_INDEXES = {
    "items": [
        "ix_items_next",
        "ix_items_queue_status",
        # Redundant: composite indexes start with queue_id; status-only lookups are for
        # in-progress items (ix_items_in_progress). Fewer indexes = less work per update.
        "ix_items_queue_id",
        "ix_items_status",
    ],
    "item_tags": ["ix_item_tags_item_pk"],  # covered by the unique (item_pk, tag) index
}
# Columns that became 64-bit, with their sequences.
_BIGINT_COLUMNS = {"item_events": ["id"]}
# Columns earlier versions had: Roots Cockpit fields (Cockpit is no longer used by any project).
_OBSOLETE_COLUMNS = {
    "queues": ["cockpit_id"],
    "items": [
        "cockpit_id",
        "cockpit_retrying_count",
        "cockpit_payload",
        "is_in_roots_cockpit",
        "process_id",
        "subprocess_id",
    ],
}


# Columns added after a table was first created: (table, column, DDL type).
_NEW_COLUMNS = [("queues", "tenant_id", "VARCHAR(36)")]


def _upgrade(conn: Connection) -> None:
    insp = inspect(conn)
    for table, column, ddl in _NEW_COLUMNS:
        if column not in {c["name"] for c in insp.get_columns(table)}:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}"))
            logger.info("Added column %s.%s", table, column)
    insp = inspect(conn)
    for table, names in _OBSOLETE_COLUMNS.items():
        existing = {c["name"] for c in insp.get_columns(table)}
        for name in names:
            if name in existing:
                conn.execute(text(f"ALTER TABLE {table} DROP COLUMN {name}"))
                logger.info("Dropped column %s.%s (Roots Cockpit is not used)", table, name)
    insp = inspect(conn)
    for table, names in _OBSOLETE_INDEXES.items():
        existing = {ix["name"] for ix in insp.get_indexes(table)}
        for name in names:
            if name in existing:
                conn.execute(text(f"DROP INDEX {name}"))
                logger.info("Dropped index %s (replaced)", name)
    # Trace ids stopped being unique (force_retry adds a row with the same trace id).
    for ix in insp.get_indexes("items"):
        if ix["name"] == "ix_items_id" and ix.get("unique"):
            conn.execute(text("DROP INDEX ix_items_id"))
            logger.info("Dropped unique index ix_items_id (trace ids may repeat after retries)")
    for table in Base.metadata.sorted_tables:
        existing = {ix["name"] for ix in inspect(conn).get_indexes(table.name)}
        for index in table.indexes:
            if index.name not in existing:
                index.create(conn)
                logger.info("Created index %s", index.name)
    if conn.dialect.name == "postgresql":
        _add_lock_rule(conn)
        _widen_to_bigint(conn)
        # Substring search on client id ("search" box) without scanning a whole queue.
        try:
            with conn.begin_nested():
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
                for col in ("display_roots_id", "roots_id"):
                    conn.execute(
                        text(
                            f"CREATE INDEX IF NOT EXISTS ix_items_{col}_trgm "
                            f"ON items USING gin ({col} gin_trgm_ops)"
                        )
                    )
        except Exception as exc:  # extension not allowed: search still works, just slower
            logger.warning("Trigram indexes not created (%s); text search will scan", exc)


def _widen_to_bigint(conn: Connection) -> None:
    for table, cols in _BIGINT_COLUMNS.items():
        for col in cols:
            kind = conn.scalar(
                text(
                    "SELECT data_type FROM information_schema.columns"
                    " WHERE table_schema = current_schema()"
                    " AND table_name = :t AND column_name = :c"
                ),
                {"t": table, "c": col},
            )
            if kind == "integer":
                conn.execute(text(f'ALTER TABLE "{table}" ALTER COLUMN "{col}" TYPE bigint'))
                logger.info("Widened %s.%s to bigint", table, col)
            seq = conn.scalar(
                text("SELECT pg_get_serial_sequence(:t, :c)"), {"t": table, "c": col}
            )
            if seq:
                conn.execute(text(f"ALTER SEQUENCE {seq} AS bigint"))


def _add_lock_rule(conn: Connection) -> None:
    from app.models import LOCK_RULE

    names = {c["name"] for c in inspect(conn).get_check_constraints("items")}
    if "ck_items_locked_in_progress" in names:
        return
    # NOT VALID: enforced for every write from now on; then check the rows already there.
    conn.execute(
        text(
            "ALTER TABLE items ADD CONSTRAINT ck_items_locked_in_progress "
            f"CHECK ({LOCK_RULE}) NOT VALID"
        )
    )
    try:
        with conn.begin_nested():
            conn.execute(text("ALTER TABLE items VALIDATE CONSTRAINT ck_items_locked_in_progress"))
        logger.info("Added check constraint ck_items_locked_in_progress")
    except Exception:
        bad = conn.execute(text(f"SELECT count(*) FROM items WHERE NOT ({LOCK_RULE})")).scalar()
        logger.warning(
            "ck_items_locked_in_progress enforced for new writes; %s existing rows break it "
            "(fix them, e.g. PUT /items/{id}/unlock, then restart to validate)",
            bad,
        )


def get_engine() -> Engine:
    if _engine is None:
        raise RuntimeError("init_db() has not been called")
    return _engine


@contextmanager
def session_scope() -> Iterator[Session]:
    if _SessionLocal is None:
        raise RuntimeError("init_db() has not been called")
    session = _SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_session() -> Iterator[Session]:
    with session_scope() as session:
        yield session
