"""Work item lifecycle: add, claim (next / lock), status changes, unlock, retry.

Rules (what the automation projects rely on):

* ``/items/next`` claims the first unlocked item that is ``queued`` (or ``deferred`` whose date
  has passed), lowest ``priority`` value first, then oldest. With ``itemId`` it claims that item
  only when it is ``queued`` or ``deferred``. ``/lock`` does the same for one item. A claim locks
  the item, sets ``in_progress``, remembers ``previous_status`` and counts an attempt; the lock
  makes sure a single process works on an item.
* ``locked`` and ``in_progress`` always go together.
* ``/status`` changes status only while the item is locked in ``in_progress``; any status other
  than ``in_progress`` releases the lock. People in the UI use ``manual`` transitions instead
  (``models.MANUAL_TRANSITIONS``), only on unlocked items.
* ``/unlock`` releases a stuck lock (e.g. a bot lost its connection) and always moves the item
  to ``queued``.
* ``/force_retry`` applies only to ``system_exception``: it adds a new row (new Item ID) with
  the same trace id, ``queued``, and keeps the failed row as history.

Claims are race-free: on Postgres the candidate row is taken with ``FOR UPDATE SKIP LOCKED``
(many workers polling one queue don't wait on each other); everywhere the claim itself is a
conditional UPDATE (``... WHERE locked = false AND status = :seen``).
"""

from __future__ import annotations

import copy
from datetime import datetime
from typing import Any

from fastapi import HTTPException
from sqlalchemy import and_, exists, false, func, or_, select, true, update
from sqlalchemy.orm import Session

from app import models
from app.models import FormElement, Item, ItemEvent, ItemFile, ItemTag, Queue, now
from app.util import norm_id, norm_status, parse_dt

DEFAULT_RESOURCE = "default"


def _is_pg(session: Session) -> bool:
    return session.get_bind().dialect.name == "postgresql"


# ------------------------------------------------------------------------------ lookups


def get_queue(session: Session, queue_id: Any) -> Queue:
    if queue_id in (None, ""):
        raise HTTPException(400, "queueId is required")
    q = session.get(Queue, norm_id(str(queue_id)))
    if q is None or q.deleted_at is not None:
        # Callers sometimes pass a queue *name* from config.
        q = session.scalar(
            select(Queue).where(Queue.deleted_at.is_(None), Queue.name == str(queue_id))
        )
    if q is None:
        raise HTTPException(400, f"Queue {queue_id} cannot be found")
    return q


def get_item(session: Session, item_id: str, *, for_update: bool = False) -> Item:
    """The latest row of a trace id (force_retry adds rows with the same trace id)."""
    stmt = (
        select(Item)
        .where(Item.id == norm_id(item_id), Item.deleted_at.is_(None))
        .order_by(Item.display_id.desc())
        .limit(1)
    )
    if for_update and _is_pg(session):
        stmt = stmt.with_for_update()
    item = session.scalar(stmt)
    if item is None:
        # 400 + "cannot be found" is what existing callers check for.
        raise HTTPException(400, f"Item {item_id} cannot be found")
    return item


def latest_rows(
    session: Session, trace_ids: list[str], *, for_update: bool = False
) -> dict[str, Item]:
    """The latest row of each trace id (by normalized trace id). With ``for_update`` every row is
    locked in one statement, in ``display_id`` order, so concurrent multi-item writes take
    their locks in the same order and never deadlock."""
    wanted = {norm_id(t) for t in trace_ids if t}
    if not wanted:
        return {}
    latest = (
        select(func.max(Item.display_id))
        .where(Item.id.in_(wanted), Item.deleted_at.is_(None))
        .group_by(Item.id)
    )
    stmt = select(Item).where(Item.display_id.in_(latest)).order_by(Item.display_id)
    if for_update and _is_pg(session):
        stmt = stmt.with_for_update()
    return {row.id: row for row in session.scalars(stmt)}


def trace_rows(session: Session, item: Item) -> list[int]:
    """Display ids of every row (attempt) of the item's trace id, newest first."""
    return list(
        session.scalars(
            select(Item.display_id).where(Item.id == item.id).order_by(Item.display_id.desc())
        )
    )


def log(
    session: Session,
    item: Item,
    event: str,
    actor: str | None,
    *,
    from_status: str | None = None,
    to_status: str | None = None,
    message: str | None = None,
) -> None:
    session.add(
        ItemEvent(
            item_pk=item.display_id,
            event=event,
            actor=actor,
            from_status=from_status,
            to_status=to_status,
            message=(message or None) and str(message)[:4000],
        )
    )


def touch(item: Item, actor: str | None) -> None:
    item.updated_at = now()
    item.last_updated_by = actor or DEFAULT_RESOURCE


# ------------------------------------------------------------------------------ add


def roots_id_for(queue: Queue, data: dict[str, Any]) -> str | None:
    """The item's roots id (client id): the queue's rootsId key, else KEY, else first value."""
    for key in (queue.roots_id, "KEY", "roots_id", "rootsId"):
        if key and data.get(key) not in (None, ""):
            return str(data[key])
    for v in data.values():
        if isinstance(v, str | int | float) and not isinstance(v, bool) and v != "":
            return str(v)
    return None


def add_items(
    session: Session,
    queue: Queue,
    items_data: list[dict[str, Any]],
    *,
    actor: str,
    tags: list[str] | None = None,
    priority: int = 50,
    defer_until: datetime | None = None,
    assignee_id: str | None = None,
    unique: dict[str, Any] | None = None,
) -> tuple[list[Item], list[str]]:
    """Create items in input order. Returns (created, skipped roots ids)."""
    if not isinstance(items_data, list) or not all(isinstance(d, dict) for d in items_data):
        raise HTTPException(422, "items must be a list of JSON objects")
    blocked_statuses: set[str] = set()
    date_filter = None
    if unique is not None:
        if unique.get("includePendingItems"):
            blocked_statuses |= models.PENDING_STATUSES
        if unique.get("includeCompletedItems"):
            blocked_statuses |= models.COMPLETED_STATUSES
        if unique.get("includeDeferredItems"):
            blocked_statuses |= models.DEFERRED_STATUSES
        if unique.get("includeTerminatedItems"):
            blocked_statuses |= models.TERMINATED_STATUSES
        date_filter = parse_dt(unique.get("dateFilter"), "dateFilter")

    created: list[Item] = []
    skipped: list[str] = []
    seen: set[str] = set()
    for data in items_data:
        rid = roots_id_for(queue, data)
        if unique is not None and rid is not None:
            if rid in seen:
                skipped.append(rid)
                continue
            if blocked_statuses:
                conds = [
                    Item.queue_id == queue.id,
                    Item.roots_id == rid,
                    Item.deleted_at.is_(None),
                    Item.status.in_(blocked_statuses),
                ]
                if date_filter is not None:
                    conds.append(Item.created_at >= date_filter)
                if session.scalar(select(exists().where(*conds))):
                    skipped.append(rid)
                    continue
            seen.add(rid)
        ts = now()
        deferred = defer_until is not None and defer_until > ts
        item = Item(
            queue_id=queue.id,
            roots_id=rid,
            display_roots_id=rid,
            data=data,
            status=models.DEFERRED if deferred else models.QUEUED,
            next_review_date=defer_until if deferred else None,
            priority=priority,
            assignee_id=assignee_id,
            last_updated_by=actor,
            created_at=ts,
            updated_at=ts,
        )
        item.tags = [ItemTag(tag=t) for t in dict.fromkeys(tags or []) if t]
        created.append(item)
    if created:
        session.add_all(created)
        session.flush()  # one round trip for the batch; ids are assigned here
        for item in created:
            log(session, item, "created", actor, to_status=item.status)
    return created, skipped


# ------------------------------------------------------------------------------ claim


def _tag_filters(include: list[str], exclude: list[str]) -> list:
    conds = []
    for t in include:
        conds.append(exists().where(ItemTag.item_pk == Item.display_id, ItemTag.tag == t))
    if exclude:
        conds.append(~exists().where(ItemTag.item_pk == Item.display_id, ItemTag.tag.in_(exclude)))
    return conds


def _claim(session: Session, item: Item, actor: str) -> bool:
    """Conditional UPDATE: lock + in_progress only if nobody changed the item meanwhile."""
    # attempts goes up on every claim, so it doubles as the optimistic-lock version.
    seen_status, seen_attempts = item.status, item.attempts
    ts = now()
    res = session.execute(
        update(Item)
        .where(
            Item.display_id == item.display_id,
            Item.status == seen_status,
            Item.locked == false(),
            Item.attempts == seen_attempts,
        )
        .values(
            locked=True,
            status=models.IN_PROGRESS,
            previous_status=seen_status,
            attempts=Item.attempts + 1,
            last_locked_at=ts,
            locking_resource=actor,
            updated_at=ts,
            last_updated_by=actor,
        )
        .execution_options(synchronize_session=False)
    )
    if res.rowcount != 1:
        return False
    session.refresh(item)
    log(session, item, "locked", actor, from_status=seen_status, to_status=models.IN_PROGRESS)
    return True


def _claimable(item: Item) -> bool:
    return not item.locked and item.status in models.LOCKABLE_STATUSES


def get_next_item(
    session: Session,
    queue: Queue,
    *,
    actor: str,
    item_id: str | None = None,
    tags_include: list[str] | None = None,
    tags_exclude: list[str] | None = None,
) -> Item | None:
    ts = now()
    tag_conds = _tag_filters(tags_include or [], tags_exclude or [])
    if item_id:
        item = session.scalar(
            select(Item)
            .where(
                Item.id == norm_id(item_id),
                Item.queue_id == queue.id,
                Item.deleted_at.is_(None),
                *tag_conds,
            )
            .order_by(Item.display_id.desc())
            .limit(1)
        )
        if item is None or not _claimable(item):
            return None
        return item if _claim(session, item, actor) else None

    # Queued items and due deferred items are looked up separately: each lookup walks
    # ix_items_claim in claim order and reads only ids, instead of sorting every waiting item.
    due_deferred = and_(
        Item.status == models.DEFERRED,
        or_(Item.next_review_date.is_(None), Item.next_review_date <= ts),
    )
    pg = _is_pg(session)
    for _ in range(5):  # retry if another worker claimed the chosen candidate first
        found = [
            row
            for row in (
                _candidate(session, queue, Item.status == models.QUEUED, tag_conds, lock=False),
                _candidate(session, queue, due_deferred, tag_conds, lock=False),
            )
            if row is not None
        ]
        if not found:
            return None
        best = min(found, key=lambda r: (r.priority, r.created_at, r.display_id))
        if pg:
            locked = session.scalar(
                select(Item)
                .where(
                    Item.display_id == best.display_id,
                    Item.locked == false(),
                    Item.deleted_at.is_(None),
                    or_(Item.status == models.QUEUED, due_deferred),
                )
                .with_for_update(skip_locked=True)
            )
            if locked is not None and _claim(session, locked, actor):
                return locked
            continue
        item = session.get(Item, best.display_id)
        if item is not None and _claim(session, item, actor):
            return item
    if pg:
        # Fallback under heavy contention: single SKIP LOCKED query (locks at most one row).
        fallback = session.scalar(
            select(Item)
            .where(
                Item.queue_id == queue.id,
                or_(Item.status == models.QUEUED, due_deferred),
                Item.locked == false(),
                Item.deleted_at.is_(None),
                *tag_conds,
            )
            .order_by(Item.priority.asc(), Item.created_at.asc(), Item.display_id.asc())
            .limit(1)
            .with_for_update(skip_locked=True, of=Item)
        )
        if fallback is not None and _claim(session, fallback, actor):
            return fallback
    return None


def _candidate(session: Session, queue: Queue, status_cond, tag_conds: list, *, lock: bool):
    """The first unlocked item matching ``status_cond`` in claim order (ids only). On Postgres
    the row is locked with SKIP LOCKED: workers polling the same queue never wait on each other."""
    stmt = (
        select(Item.display_id, Item.priority, Item.created_at)
        .where(
            Item.queue_id == queue.id,
            status_cond,
            Item.locked == false(),
            Item.deleted_at.is_(None),
            *tag_conds,
        )
        .order_by(Item.priority.asc(), Item.created_at.asc(), Item.display_id.asc())
        .limit(1)
    )
    if lock:
        stmt = stmt.with_for_update(skip_locked=True, of=Item)
    return session.execute(stmt).first()


def lock_item(session: Session, item: Item, actor: str) -> str:
    if item.locked:
        if item.locking_resource == actor:
            return "Item already locked by current resource"
        raise HTTPException(400, f"Item is currently locked by {item.locking_resource}")
    if item.status not in models.LOCKABLE_STATUSES:
        raise HTTPException(
            400, f"Item status is {item.status}; only queued or deferred items can be locked"
        )
    if not _claim(session, item, actor):
        raise HTTPException(409, "Item changed while locking; try again")
    return "Item locked"


def unlock_item(session: Session, item: Item, actor: str, *, force: bool = False) -> str:
    """Release a stuck lock. Any caller may do it (that is the point); the item is queued."""
    del force  # accepted for compatibility: unlocking never depends on who holds the lock
    if not item.locked:
        raise HTTPException(400, "Item is not locked")
    before, holder = item.status, item.locking_resource
    _accumulate_work_time(item)
    item.previous_status = before
    item.status = models.QUEUED
    item.locked = False
    item.locking_resource = None
    touch(item, actor)
    log(
        session,
        item,
        "unlocked",
        actor,
        from_status=before,
        to_status=item.status,
        message=f"lock held by {holder}" if holder else None,
    )
    return "Item unlocked"


def _accumulate_work_time(item: Item) -> None:
    if item.locked and item.status == models.IN_PROGRESS and item.last_locked_at:
        item.work_time_seconds = (item.work_time_seconds or 0.0) + max(
            (now() - item.last_locked_at).total_seconds(), 0.0
        )


# ------------------------------------------------------------------------------ status


def set_status(
    session: Session,
    item: Item,
    status: str,
    *,
    actor: str,
    exception_message: str | None = None,
    defer_until: datetime | None = None,
    auto_retry: bool = False,
    user_assign: str | None = None,
    manual: bool = False,
) -> Item:
    """Change status. Bots: only while the item is locked in in_progress. ``manual`` (people in
    the UI): only the review transitions in ``models.MANUAL_TRANSITIONS``, on unlocked items."""
    new = norm_status(status)
    before = item.status
    if new == models.IN_PROGRESS:
        raise HTTPException(
            400, "in_progress is set by locking the item (/items/next or /lock), not by /status"
        )
    if manual:
        allowed = models.MANUAL_TRANSITIONS.get(before, set())
        if item.locked or new not in allowed:
            raise HTTPException(
                409,
                f"A person can't move an item from {before} to {new}"
                + ("; it is locked by " + str(item.locking_resource) if item.locked else ""),
            )
    elif not (item.locked and before == models.IN_PROGRESS):
        raise HTTPException(
            409,
            f"Item status is {before}; status can only be updated while the item is locked "
            "in in_progress (claim it with /items/next or /lock first)",
        )

    ts = now()
    _accumulate_work_time(item)
    if before != new:
        item.previous_status = before
    msg = exception_message or None

    if new == models.DEFERRED:
        item.next_review_date = defer_until or ts
        item.defer_count = (item.defer_count or 0) + 1
    if new == models.SYSTEM_EXCEPTION:
        item.exception_date = ts
        item.exception_reason = msg
        item.last_exception_type = "System_Exception"
    elif new == models.REQUIRES_REVIEW:
        item.exception_date = ts
        item.exception_reason = msg
        item.last_exception_type = "Business_Exception"
    elif msg:
        item.exception_reason = msg
    if new in models.COMPLETED_STATUSES:
        item.completed_at = ts
        # A finished item has no current exception (an earlier "deferred: awaiting extraction"
        # would read as the outcome). lastExceptionType / exceptionDate and the item's history
        # keep what happened before.
        if not msg:
            item.exception_reason = None

    item.status = new
    item.locked = False
    item.locking_resource = None
    if user_assign:
        item.assignee_id = user_assign
        item.assigned_by = actor
    touch(item, actor)
    log(
        session,
        item,
        "manual_status" if manual else "status",
        actor,
        from_status=before,
        to_status=new,
        message=msg,
    )

    if new == models.SYSTEM_EXCEPTION and auto_retry:
        max_attempts = item.queue.max_attempts if item.queue else 3
        if (item.attempts or 0) < max_attempts:
            force_retry(session, item, actor, note="auto_retry")
    return item


def force_retry(session: Session, item: Item, actor: str, *, note: str | None = None) -> Item:
    """New row (new Item ID), same trace id, queued. The failed row stays as history."""
    if _is_pg(session):
        from sqlalchemy import text as sql_text

        session.execute(sql_text("SELECT pg_advisory_xact_lock(hashtext(:tid))"), {"tid": item.id})
    newer = session.scalar(
        select(Item.display_id)
        .where(
            Item.id == item.id,
            Item.deleted_at.is_(None),
            Item.display_id > item.display_id,
        )
        .limit(1)
    )
    if newer is not None:
        raise HTTPException(
            409, f"Item has already been retried as Item ID {newer}"
        )
    if item.status != models.SYSTEM_EXCEPTION:
        raise HTTPException(
            400, f"Item status is {item.status}; force retry only applies to system_exception"
        )
    ts = now()
    retry = Item(
        id=item.id,
        queue_id=item.queue_id,
        roots_id=item.roots_id,
        display_roots_id=item.display_roots_id,
        data=copy.deepcopy(item.data or {}),
        status=models.QUEUED,
        previous_status=models.SYSTEM_EXCEPTION,
        priority=item.priority,
        attempts=item.attempts or 0,  # the retry budget (queue.maxAttempts) spans attempts
        defer_count=item.defer_count or 0,
        reporting_json=copy.deepcopy(item.reporting_json),
        form_search_text=item.form_search_text or "",
        notes=item.notes or "",
        assignee_id=item.assignee_id,
        assigned_by=item.assigned_by,
        tenant_id=item.tenant_id,
        last_updated_by=actor or DEFAULT_RESOURCE,
        created_at=ts,
        updated_at=ts,
    )
    retry.tags = [ItemTag(tag=t.tag) for t in item.tags]
    session.add(retry)
    session.flush()
    for f in session.scalars(select(ItemFile).where(ItemFile.item_pk == item.display_id)):
        session.add(
            ItemFile(
                item_pk=retry.display_id,
                file_name=f.file_name,
                file_path=f.file_path,
                package_id=f.package_id,
            )
        )
    for fe in session.scalars(select(FormElement).where(FormElement.item_pk == item.display_id)):
        session.add(
            FormElement(item_pk=retry.display_id, encrypted_form_element=fe.encrypted_form_element)
        )
    log(
        session,
        item,
        "force_retry",
        actor,
        from_status=item.status,
        message=f"retried as Item ID {retry.display_id}" + (f" ({note})" if note else ""),
    )
    log(
        session,
        retry,
        "created",
        actor,
        from_status=models.SYSTEM_EXCEPTION,
        to_status=models.QUEUED,
        message=f"retry of Item ID {item.display_id}" + (f" ({note})" if note else ""),
    )
    return retry


def clean_up(session: Session, actor: str) -> int:
    """In-progress items locked by ``actor`` become system exceptions and are unlocked."""
    stmt = (
        select(Item)
        .where(
            Item.locked == true(),
            Item.status == models.IN_PROGRESS,
            Item.locking_resource == actor,
            Item.deleted_at.is_(None),
        )
        .order_by(Item.display_id)
    )
    if _is_pg(session):
        stmt = stmt.with_for_update()
    items = session.scalars(stmt).all()
    for item in items:
        set_status(
            session,
            item,
            models.SYSTEM_EXCEPTION,
            actor=actor,
            exception_message="Cleaned up: in progress when its resource restarted",
        )
    return len(items)


def events_for_trace(session: Session, item: Item) -> list[ItemEvent]:
    """Audit trail of every attempt of the item's trace id, newest first."""
    return list(
        session.scalars(
            select(ItemEvent)
            .where(ItemEvent.item_pk.in_(trace_rows(session, item)))
            .order_by(ItemEvent.at.desc(), ItemEvent.id.desc())
        )
    )
