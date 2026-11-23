"""Work Item Manager settings (environment variables or .env)."""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "sqlite:///./data/work_items.db"
    # When DATABASE_URL goes through a transaction-pooling PgBouncer: a direct connection to
    # Postgres for startup work (schema upgrades, advisory locks). Empty = use DATABASE_URL.
    direct_database_url: str | None = None
    # When set, callers must send it as Ocp-Apim-Subscription-Key (what RootsQueueClient sends)
    # or X-API-Key (what the Agent Hub UI proxy sends).
    api_key: str | None = None
    cors_origins: list[str] = ["http://localhost:3000"]

    # Default retry budget for new queues (queue.maxAttempts).
    default_max_attempts: int = 3
    # Page size of POST /items/searches (v1). The v2 search pages with startRow/endRow.
    search_page_size: int = 1000
    # No search returns more than this many items in one page (v2, ui_get_items, docs).
    max_page_size: int = 1000
    # In-progress items locked longer than this are reported as stale by /health (stuck items
    # are released with /unlock, which returns them to queued).
    stale_lock_minutes: int = 30

    # Connection pool per worker process (Postgres). Database connections in use at most:
    # replicas × WEB_CONCURRENCY × (db_pool_size + db_max_overflow).
    db_pool_size: int = 5
    db_max_overflow: int = 5
    # Postgres statement timeout in ms (0 = none). Searches of large projects may take seconds.
    db_statement_timeout_ms: int = 30000
    # Postgres lock timeout in ms: waiting longer for a row lock fails with a retryable 503.
    db_lock_timeout_ms: int = 5000

    # Retention: POST /admin/retire_items (or `python -m app.retention`) deletes items whose
    # last update is older than this many days and whose status is one of retire_statuses.
    retention_days: int = 180
    retire_statuses: list[str] = [
        "automated",
        "completed",
        "manually_closed",
        "system_exception",
    ]
    retire_batch_size: int = 2000

    # Annotations API (services/annotations), used for an item's linked documents.
    annotations_api_url: str = "http://localhost:8004"
    annotations_api_key: str | None = None

    # Demo queues, users and items so the UI has something to show. Off in hosted environments.
    seed_demo_data: bool = False
    # The Demo organization's queues (Mortgage Change, Loss Runs), linked to the Demo projects
    # the decision-tree API seeds with fixed ids. Created on every start, in every environment.
    seed_default_queues: bool = True


@lru_cache
def get_settings() -> Settings:
    return Settings()
