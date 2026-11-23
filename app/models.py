"""Work Item Manager database.

    queues ─< items ─┬─< item_tags
                     ├─< item_files
                     ├─< form_elements      (encrypted by the caller; stored as-is)
                     └─< item_events        (audit trail: created, status, lock, assign, ...)
    resources                               (bots / callers, from the Resource header)
    users                                   (people work items can be assigned to)

An item has two ids, like the platform's work items:
* ``id``: a uuid4, stored as 32-char hex and returned dashed. This is the *trace id* and what
  every endpoint takes in its path (dashed or not, any case).
* ``display_id``: an incrementing integer (``incrementedDisplayId``), shown as *Item ID*.

A trace id is not unique: ``force_retry`` of a system exception adds a new row (new Item ID)
with the same trace id and leaves the failed row as history. Endpoints that take a trace id
act on its latest row (highest ``display_id``).

Lifecycle (what callers rely on):
* ``/items/next`` and ``/lock`` claim only ``queued`` or ``deferred`` items; a claim locks the
  item and sets ``in_progress``. ``locked`` and ``in_progress`` always go together.
* ``/status`` changes status only while the item is locked in ``in_progress`` (bots). People
  reviewing in the UI use a few explicit manual transitions instead (``manual: true``).
* ``/unlock`` (stuck items) always returns the item to ``queued``.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base, UTCDateTime


def new_id() -> str:
    return uuid.uuid4().hex


def now() -> datetime:
    return datetime.now(UTC)


# Statuses (stored lowercase with underscores; callers may send "Requires Review" etc.).
QUEUED = "queued"
IN_PROGRESS = "in_progress"
REQUIRES_REVIEW = "requires_review"
AUTOMATED = "automated"
SYSTEM_EXCEPTION = "system_exception"
MANUALLY_CLOSED = "manually_closed"
DEFERRED = "deferred"

STATUSES = [
    QUEUED,
    IN_PROGRESS,
    DEFERRED,
    REQUIRES_REVIEW,
    AUTOMATED,
    SYSTEM_EXCEPTION,
    MANUALLY_CLOSED,
]
# Older statuses that rows may still hold; new writes use STATUSES ("retrying" → queued,
# "completed" → automated, see app.util).
RETRYING = "retrying"
COMPLETED = "completed"

PENDING_STATUSES = {QUEUED, IN_PROGRESS, REQUIRES_REVIEW}
COMPLETED_STATUSES = {AUTOMATED, MANUALLY_CLOSED, COMPLETED}
DEFERRED_STATUSES = {DEFERRED}
TERMINATED_STATUSES = {SYSTEM_EXCEPTION}
# Statuses get_next_item / lock may pick up.
LOCKABLE_STATUSES = {QUEUED, DEFERRED}
# Status changes a person may make on an unlocked item (UI review actions), by current status.
MANUAL_TRANSITIONS: dict[str, set[str]] = {
    REQUIRES_REVIEW: {QUEUED, MANUALLY_CLOSED},
    SYSTEM_EXCEPTION: {MANUALLY_CLOSED},
    DEFERRED: {QUEUED, MANUALLY_CLOSED},
    QUEUED: {MANUALLY_CLOSED},
    RETRYING: {QUEUED, MANUALLY_CLOSED},
}


LOCK_RULE = "deleted_at IS NOT NULL OR (status = 'in_progress') = locked"


class Queue(Base):
    __tablename__ = "queues"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    name: Mapped[str] = mapped_column(String(255), unique=True)
    description: Mapped[str | None] = mapped_column(Text)
    max_attempts: Mapped[int] = mapped_column(Integer, default=3)
    # Name of the item-data key whose value is an item's roots id (client id), e.g. "KEY".
    roots_id: Mapped[str | None] = mapped_column(String(255))
    organization: Mapped[str | None] = mapped_column(String(255))
    # The org (Agent Hub customer / v1 tenant) this queue, a "project" in the UI, belongs to.
    # A project is not tied to a decision tree: automations upload a work item's file to a tree
    # configuration and link the package to the item (annotations /link), as in v1.
    tenant_id: Mapped[str | None] = mapped_column(String(36), index=True)
    # Deprecated: the tree configuration a queue was once tied to. Kept for old data only (it
    # gives the org of queues created before tenant_id, and a default for manual uploads).
    project_id: Mapped[str | None] = mapped_column(String(36))
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=now)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime, default=now, onupdate=now)
    deleted_at: Mapped[datetime | None] = mapped_column(UTCDateTime)


class Item(Base):
    __tablename__ = "items"
    __table_args__ = (
        # A live item is locked exactly when it is in_progress (no "in_progress but unlocked").
        CheckConstraint(LOCK_RULE, name="ck_items_locked_in_progress"),
        # Status counts of a queue (live items only, so Postgres can answer from the index).
        Index(
            "ix_items_queue_status_live",
            "queue_id",
            "status",
            postgresql_where=text("deleted_at IS NULL"),
            sqlite_where=text("deleted_at IS NULL"),
        ),
        # /items/next: the next unlocked item of one status, already in claim order, so a claim
        # reads one index entry instead of sorting every waiting item of a big queue.
        Index(
            "ix_items_claim",
            "queue_id",
            "status",
            "locked",
            "priority",
            "created_at",
            "display_id",
        ),
        # Latest row of a trace id (force_retry adds rows with the same trace id).
        Index("ix_items_trace", "id", "display_id"),
        # Work item list: newest first within a queue, optionally by status (scanned backwards
        # for DESC, so a project with 100k items pages without sorting the whole queue).
        Index("ix_items_queue_created", "queue_id", "created_at", "display_id"),
        Index("ix_items_queue_status_created", "queue_id", "status", "created_at", "display_id"),
        Index("ix_items_queue_updated", "queue_id", "updated_at"),
        # In-progress items across queues (health counts, stale locks, clean_up by resource).
        # Partial: other items never touch it.
        Index(
            "ix_items_in_progress",
            "locking_resource",
            postgresql_where=text("status = 'in_progress' AND deleted_at IS NULL"),
            sqlite_where=text("status = 'in_progress' AND deleted_at IS NULL"),
        ),
    )

    display_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    id: Mapped[str] = mapped_column(String(32), default=new_id)
    # Indexed through the composite indexes that start with queue_id.
    queue_id: Mapped[str] = mapped_column(ForeignKey("queues.id", ondelete="CASCADE"))

    roots_id: Mapped[str | None] = mapped_column(String(500), index=True)
    display_roots_id: Mapped[str | None] = mapped_column(String(500), index=True)
    data: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)

    status: Mapped[str] = mapped_column(String(32), default=QUEUED)
    previous_status: Mapped[str | None] = mapped_column(String(32))
    priority: Mapped[int] = mapped_column(Integer, default=50)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    locked: Mapped[bool] = mapped_column(Boolean, default=False)
    locking_resource: Mapped[str | None] = mapped_column(String(255))
    last_locked_at: Mapped[datetime | None] = mapped_column(UTCDateTime)

    next_review_date: Mapped[datetime | None] = mapped_column(UTCDateTime)  # deferred until
    defer_count: Mapped[int] = mapped_column(Integer, default=0)
    completed_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    work_time_seconds: Mapped[float] = mapped_column(Float, default=0.0)

    exception_date: Mapped[datetime | None] = mapped_column(UTCDateTime)
    exception_reason: Mapped[str | None] = mapped_column(Text)
    last_exception_type: Mapped[str | None] = mapped_column(String(64))

    reporting_json: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    form_search_text: Mapped[str] = mapped_column(Text, default="")
    notes: Mapped[str] = mapped_column(Text, default="")

    assignee_id: Mapped[str | None] = mapped_column(String(64), index=True)
    assigned_by: Mapped[str | None] = mapped_column(String(64))
    tenant_id: Mapped[str | None] = mapped_column(String(64))
    last_updated_by: Mapped[str | None] = mapped_column(String(255))

    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=now, index=True)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime, default=now, index=True)
    deleted_at: Mapped[datetime | None] = mapped_column(UTCDateTime)

    queue: Mapped[Queue] = relationship()
    tags: Mapped[list[ItemTag]] = relationship(
        cascade="all, delete-orphan", order_by="ItemTag.id", lazy="selectin"
    )
    files: Mapped[list[ItemFile]] = relationship(
        cascade="all, delete-orphan", order_by="ItemFile.created_at"
    )

    @property
    def tag_names(self) -> list[str]:
        return [t.tag for t in self.tags]


class ItemTag(Base):
    __tablename__ = "item_tags"
    # The unique (item_pk, tag) index also serves lookups by item.
    __table_args__ = (UniqueConstraint("item_pk", "tag"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    item_pk: Mapped[int] = mapped_column(ForeignKey("items.display_id", ondelete="CASCADE"))
    tag: Mapped[str] = mapped_column(String(255), index=True)


class ItemFile(Base):
    __tablename__ = "item_files"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    item_pk: Mapped[int] = mapped_column(
        ForeignKey("items.display_id", ondelete="CASCADE"), index=True
    )
    file_name: Mapped[str] = mapped_column(String(500))
    file_path: Mapped[str] = mapped_column(Text)
    # Extraction package (Agent Hub / platform) holding this document, when there is one.
    package_id: Mapped[int | None] = mapped_column(Integer, index=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=now)


class FormElement(Base):
    __tablename__ = "form_elements"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    item_pk: Mapped[int] = mapped_column(
        ForeignKey("items.display_id", ondelete="CASCADE"), index=True
    )
    encrypted_form_element: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=now)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime, default=now, onupdate=now)


class ItemEvent(Base):
    __tablename__ = "item_events"

    # Several events per item over years: 64-bit.
    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer(), "sqlite"), primary_key=True, autoincrement=True
    )
    item_pk: Mapped[int] = mapped_column(
        ForeignKey("items.display_id", ondelete="CASCADE"), index=True
    )
    at: Mapped[datetime] = mapped_column(UTCDateTime, default=now)
    event: Mapped[str] = mapped_column(String(50))
    from_status: Mapped[str | None] = mapped_column(String(32))
    to_status: Mapped[str | None] = mapped_column(String(32))
    actor: Mapped[str | None] = mapped_column(String(255))
    message: Mapped[str | None] = mapped_column(Text)


class Resource(Base):
    __tablename__ = "resources"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    name: Mapped[str] = mapped_column(String(255), unique=True)
    organization_id: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=now)


class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(
        String(64), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    name: Mapped[str] = mapped_column(String(255))
    email: Mapped[str | None] = mapped_column(String(255), unique=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=now)
