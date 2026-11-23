"""Work Item Manager REST API (paths match the Roots Queue / work_item_mgr API).

Point RootsQueueClient (``base_url``) or a Postman ``workItemManagerUrl`` at this service.
Every path works with or without a trailing slash. Ids may be dashed UUIDs or 32-char hex.
The ``Resource`` header names the caller (bot / function app); it owns locks and is recorded
as ``lastUpdatedBy``.
"""

from __future__ import annotations

import json
import logging
import math
import secrets
from typing import Annotated, Any

import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse, PlainTextResponse
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app import models, search, serialize, service
from app.config import Settings, get_settings
from app.db import get_session
from app.db_read import get_read_session
from app.models import FormElement, Item, ItemFile, ItemTag, Queue, Resource, User
from app.util import Body, json_body, norm_id, parse_dt

logger = logging.getLogger(__name__)


def require_api_key(
    settings: Annotated[Settings, Depends(get_settings)],
    ocp_apim_subscription_key: Annotated[str | None, Header()] = None,
    x_api_key: Annotated[str | None, Header()] = None,
) -> None:
    if not settings.api_key:
        return
    for given in (ocp_apim_subscription_key, x_api_key):
        if given and secrets.compare_digest(given, settings.api_key):
            return
    raise HTTPException(401, "invalid or missing Ocp-Apim-Subscription-Key")


def caller(resource: Annotated[str | None, Header()] = None) -> str:
    return (resource or "").strip() or service.DEFAULT_RESOURCE


router = APIRouter(dependencies=[Depends(require_api_key)])
# scope="function": the session commits when the endpoint returns, before the response is sent,
# so a caller never gets ids of rows that are not committed yet (e.g. add an item, then claim it).
SessionDep = Annotated[Session, Depends(get_session, scope="function")]
# Lag-tolerant reads (lists, searches): the read replica when DB_READ_URL is set.
ReadSessionDep = Annotated[Session, Depends(get_read_session)]
ActorDep = Annotated[str, Depends(caller)]
SettingsDep = Annotated[Settings, Depends(get_settings)]


def ok(message: str, **extra: Any) -> dict[str, Any]:
    return {"message": message, **extra}


async def body_of(request: Request) -> Body:
    return Body(await json_body(request))


BodyDep = Annotated[Body, Depends(body_of)]


# ============================================================================== queues


@router.get("/queues", tags=["queues"], summary="All queues (id + name); ?tenant_id= one org's")
def list_queues(
    session: SessionDep, details: bool = False, tenant_id: str | None = None
) -> list[dict]:
    stmt = select(Queue).where(Queue.deleted_at.is_(None)).order_by(Queue.name)
    if tenant_id:
        stmt = stmt.where(Queue.tenant_id == tenant_id.lower())
    queues = session.scalars(stmt).all()
    if not details:
        return [{"id": q.id, "name": q.name} for q in queues]
    counts = _counts_by_queue(session)
    return [serialize.queue_details(q, counts.get(q.id, {})) for q in queues]


@router.post("/queues", tags=["queues"], summary="Add a queue")
def add_queue(session: SessionDep, body: BodyDep, settings: SettingsDep) -> dict:
    name = str(body.get("queue_name", "name", default="")).strip()
    if not name:
        raise HTTPException(422, "queueName is required")
    if session.scalar(select(Queue).where(Queue.name == name, Queue.deleted_at.is_(None))):
        raise HTTPException(400, f"Queue {name!r} already exists")
    q = Queue(
        name=name,
        max_attempts=int(body.get("max_attempts", default=settings.default_max_attempts)),
        roots_id=body.get("roots_id") or None,
        organization=body.get("organization", "organization_id"),
        description=body.get("description"),
        tenant_id=_tenant(body.get("tenant_id", "tenantId")),
        project_id=body.get("project_id"),
    )
    session.add(q)
    session.flush()
    return {"queueId": q.id, **serialize.queue_details(q)}


@router.post("/queues/count_by_status", tags=["queues"], summary="Item counts by status")
def count_by_status(session: SessionDep, body: BodyDep) -> dict[str, int]:
    queue_id = body.get("queue_id")
    q = service.get_queue(session, queue_id) if queue_id else None
    return _status_counts(session, q.id if q else None)


@router.get("/queues/{queue_id}", tags=["queues"], summary="Queue by id")
def get_queue(queue_id: str, session: SessionDep) -> dict:
    return serialize.queue_strict(service.get_queue(session, queue_id))


@router.get("/queues/{queue_id}/details", tags=["queues"], summary="Queue + counts (UI)")
def get_queue_details(queue_id: str, session: SessionDep) -> dict:
    q = service.get_queue(session, queue_id)
    return serialize.queue_details(q, _status_counts(session, q.id))


@router.put("/queues/{queue_id}/update", tags=["queues"], summary="Update a queue")
def update_queue(queue_id: str, session: SessionDep, body: BodyDep) -> dict:
    q = service.get_queue(session, queue_id)
    if body.get("name"):
        new_name = str(body.get("name")).strip()
        if new_name != q.name and session.scalar(
            select(Queue).where(Queue.name == new_name, Queue.id != q.id)
        ):
            raise HTTPException(400, f"Queue {new_name!r} already exists")
        q.name = new_name
    for attr, key in (
        ("max_attempts", "max_attempts"),
        ("roots_id", "roots_id"),
        ("organization", "organization"),
    ):
        if body.get(key) not in (None, ""):
            setattr(q, attr, body.get(key))
    for attr in ("description", "project_id"):  # nullable from the UI
        if body.has(attr):
            setattr(q, attr, body.raw.get(attr, body.get(attr)) or None)
    if body.has("tenant_id"):
        q.tenant_id = _tenant(body.get("tenant_id"))
    session.flush()
    return serialize.queue_details(q)


def _tenant(value: object) -> str | None:
    """A tenant (org) id as stored: lowercase UUID; the v1 UI sends uppercase ones."""
    text = str(value or "").strip().lower()
    return text or None


@router.delete("/queues/{queue_id}", tags=["queues"], summary="Delete a queue (soft)")
def delete_queue(queue_id: str, session: SessionDep) -> dict:
    q = service.get_queue(session, queue_id)
    q.deleted_at = models.now()
    suffix = f" (deleted {q.id[:8]})"
    q.name = f"{q.name[: 255 - len(suffix)]}{suffix}"
    return ok("Queue deleted", queueId=q.id)


def _status_counts(session: Session, queue_id: str | None) -> dict[str, int]:
    stmt = select(Item.status, func.count()).where(Item.deleted_at.is_(None))
    if queue_id:
        stmt = stmt.where(Item.queue_id == queue_id)
    rows = dict(session.execute(stmt.group_by(Item.status)).all())
    out = {s: int(rows.get(s, 0)) for s in models.STATUSES}
    out["total"] = sum(out.values())
    return out


def _counts_by_queue(session: Session) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    rows = session.execute(
        select(Item.queue_id, Item.status, func.count())
        .where(Item.deleted_at.is_(None))
        .group_by(Item.queue_id, Item.status)
    ).all()
    for qid, st, n in rows:
        d = out.setdefault(qid, {s: 0 for s in models.STATUSES} | {"total": 0})
        d[st] = int(n)
        d["total"] += int(n)
    return out


# ============================================================================== resources & users


@router.post("/resources", tags=["resources"], summary="Add a resource (bot / caller)")
def add_resource(session: SessionDep, body: BodyDep) -> dict:
    name = str(body.get("resource_name", "name", default="")).strip()
    if not name:
        raise HTTPException(422, "resourceName is required")
    r = session.scalar(select(Resource).where(Resource.name == name))
    if r is None:
        r = Resource(name=name, organization_id=body.get("organization_id"))
        session.add(r)
        session.flush()
    return serialize.resource_out(r)


@router.get("/resources", tags=["resources"])
def list_resources(session: SessionDep) -> list[dict]:
    return [
        serialize.resource_out(r)
        for r in session.scalars(select(Resource).order_by(Resource.name))
    ]


@router.get("/users", tags=["users"], summary="People items can be assigned to")
def list_users(session: SessionDep) -> list[dict]:
    return [serialize.user_out(u) for u in session.scalars(select(User).order_by(User.name))]


@router.post("/users", tags=["users"])
def add_user(session: SessionDep, body: BodyDep) -> dict:
    name = str(body.get("name", default="")).strip()
    if not name:
        raise HTTPException(422, "name is required")
    email = body.get("email") or None
    if email and session.scalar(select(User).where(User.email == email)):
        raise HTTPException(400, f"a user with email {email} already exists")
    u = User(name=name, email=email)
    if body.get("id"):
        u.id = str(body.get("id"))
    session.add(u)
    session.flush()
    return serialize.user_out(u)


@router.delete("/users/{user_id}", tags=["users"])
def delete_user(user_id: str, session: SessionDep) -> dict:
    u = session.get(User, user_id)
    if u is None:
        raise HTTPException(404, "user not found")
    session.delete(u)
    return ok("User deleted")


def _user_names(session: Session) -> dict[str, str]:
    return dict(session.execute(select(User.id, User.name)).all())


# ============================================================================== items: add / claim


@router.post("/items", tags=["items"], summary="Add items to a queue")
def add_items(session: SessionDep, body: BodyDep, actor: ActorDep) -> dict:
    queue = service.get_queue(session, body.get("queue_id"))
    unique = body.raw.get("enforceUniqueRootsId", body.raw.get("enforce_unique_roots_id"))
    created, skipped = service.add_items(
        session,
        queue,
        body.get("items", default=[]),
        actor=actor,
        tags=body.get("tags") or [],
        priority=int(body.get("priority", default=50)),
        defer_until=parse_dt(body.get("defer_until"), "deferUntil"),
        assignee_id=body.get("assignee_id"),
        unique=unique if isinstance(unique, dict) else None,
    )
    return {
        "itemIds": [serialize.dashed(i.id) for i in created],
        "skippedRootsIds": skipped,
    }


@router.post(
    "/items/next",
    tags=["items"],
    summary="Lock and return the next item (204 when none is available)",
    responses={204: {"description": "No item available"}},
)
def next_item(session: SessionDep, body: BodyDep, actor: ActorDep) -> Response:
    queue = service.get_queue(session, body.get("queue_id"))
    item = service.get_next_item(
        session,
        queue,
        actor=actor,
        item_id=body.get("item_id"),
        tags_include=body.get("tags_include") or [],
        tags_exclude=body.get("tags_exclude") or [],
    )
    if item is None:
        return Response(status_code=204)
    return JSONResponse(serialize.next_item(item))


@router.put(
    "/items/clean_up", tags=["items"], summary="Fail + unlock the caller's in-progress items"
)
def clean_up(session: SessionDep, actor: ActorDep) -> dict:
    return ok("Clean up complete", count=service.clean_up(session, actor))


@router.put("/items/assign", tags=["items"], summary="Assign items to people")
def assign(session: SessionDep, body: BodyDep, actor: ActorDep) -> dict:
    entries = body.get("items", default=[])
    if not isinstance(entries, list):
        raise HTTPException(422, "items must be a list")
    entries = [Body(e) for e in entries if isinstance(e, dict)]
    traces = [str(b.get("work_item_id", "item_id", default="")) for b in entries]
    # All rows are locked up front in one ordered statement and written in one flush: locks are
    # held for milliseconds, and overlapping bulk assigns neither deadlock nor queue up.
    items = service.latest_rows(session, traces, for_update=True)
    done = []
    for b, trace in zip(entries, traces, strict=True):
        item = items.get(norm_id(trace))
        if item is None:
            raise HTTPException(400, f"Item {trace} cannot be found")
        before = item.assignee_id
        item.assignee_id = b.raw.get("assignee_id", b.get("assignee_id")) or None
        item.assigned_by = b.get("assigned_by") or actor
        if b.get("tenant_id"):
            item.tenant_id = b.get("tenant_id")
        service.touch(item, actor)
        service.log(
            session,
            item,
            "assigned",
            actor,
            message=f"{before or 'unassigned'} → {item.assignee_id or 'unassigned'}",
        )
        done.append(serialize.dashed(item.id))
    return ok("Items assigned", itemIds=done)


# ============================================================================== items: search


@router.post("/items/searches", tags=["search"], summary="Search item ids (paged)")
def search_items(session: ReadSessionDep, body: BodyDep, settings: SettingsDep) -> dict:
    queue = service.get_queue(session, body.get("queue_id"))
    conds = search.filters(body, queue_id=queue.id)
    total = session.scalar(select(func.count()).select_from(Item).where(*conds)) or 0
    size = min(settings.search_page_size, settings.max_page_size)
    page = max(int(body.get("page", default=1) or 1), 1)
    ids = session.scalars(
        select(Item.id)
        .where(*conds)
        .order_by(Item.created_at.asc(), Item.display_id.asc())
        .offset((page - 1) * size)
        .limit(size)
    ).all()
    dashed_ids = [serialize.dashed(i) for i in ids]
    return {
        "item_ids": dashed_ids,
        "itemIds": dashed_ids,
        "page": page,
        "pages": max(math.ceil(total / size), 1),
        "count": total,
    }


def _files_by_item(session: Session, items: list[Item]) -> dict[int, list[ItemFile]]:
    out: dict[int, list[ItemFile]] = {i.display_id: [] for i in items}
    pks = list(out.keys())
    for idx in range(0, len(pks), 1000):
        chunk = pks[idx : idx + 1000]
        for f in session.scalars(
            select(ItemFile).where(ItemFile.item_pk.in_(chunk)).order_by(ItemFile.created_at)
        ):
            out[f.item_pk].append(f)
    return out


def _page(body: Body, settings: Settings) -> tuple[int, int]:
    """startRow/endRow, at most ``max_page_size`` (1000) rows per page."""
    raw_start = body.get("start_row")
    start = max(int(raw_start) if raw_start not in (None, "") else 0, 0)
    raw_end = body.get("end_row")
    end = int(raw_end) if raw_end not in (None, "") else start + 100
    return start, min(max(end, start), start + settings.max_page_size)


V1_SEARCH_STATUSES = (
    models.QUEUED,
    models.IN_PROGRESS,
    models.AUTOMATED,
    models.REQUIRES_REVIEW,
    models.SYSTEM_EXCEPTION,
    models.DEFERRED,
)


@router.post(
    "/items/v2/searches",
    tags=["search"],
    summary="AG Grid search: rows, columns, metadata (max 1000 rows a page; export=true → CSV)",
)
def search_v2(session: ReadSessionDep, body: BodyDep, settings: SettingsDep) -> Response:
    queue_id = body.get("queue_id")
    q = service.get_queue(session, queue_id) if queue_id else None
    conds = search.filters(body, queue_id=q.id if q else None)
    start, end = _page(body, settings)
    export = bool(body.get("export", default=False))
    stmt = search.item_query(conds).order_by(*search.order_by(body))
    if not export:
        stmt = stmt.offset(start).limit(max(end - start, 0))
    items = session.scalars(stmt).all()
    files = _files_by_item(session, items)
    users = _user_names(session)
    rows = [serialize.v2_row(i, files[i.display_id], users) for i in items]
    if export:
        return PlainTextResponse(
            serialize.v2_csv(rows),
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="work_items.csv"'},
        )
    # v1 lists these statuses even when none of the items has them (others only when present).
    statuses: dict[str, int] = dict.fromkeys(V1_SEARCH_STATUSES, 0)
    statuses.update(
        session.execute(
            select(Item.status, func.count()).where(*conds).group_by(Item.status)
        ).all()
    )
    total = sum(statuses.values())
    return JSONResponse(
        {
            "rows": rows,
            "columns": serialize.V2_COLUMNS,
            "metadata": {"total_item_count": total, "statuses": statuses},
            "lastRow": total,
        }
    )


@router.post("/items/ui_get_items", tags=["search"], summary="Paged full items")
def ui_get_items(session: SessionDep, body: BodyDep, settings: SettingsDep) -> dict:
    queue_id = body.get("queue_id")
    q = service.get_queue(session, queue_id) if queue_id else None
    conds = search.filters(body, queue_id=q.id if q else None)
    size = max(min(int(body.get("items_count", default=75) or 75), settings.max_page_size), 1)
    page = max(int(body.get("page", default=1) or 1), 1)
    order = (
        Item.created_at.asc()
        if int(body.get("order", default=1) or 1) >= 0
        else Item.created_at.desc()
    )
    total = session.scalar(select(func.count()).select_from(Item).where(*conds)) or 0
    items = session.scalars(
        select(Item).where(*conds).order_by(order).offset((page - 1) * size).limit(size)
    ).all()
    return {
        "items": [serialize.item_info(i) for i in items],
        "total": total,
        "page": page,
        "pages": max(math.ceil(total / size), 1),
    }


@router.post("/docs/searches", tags=["search"], summary="Documents in a queue")
def docs_search(session: ReadSessionDep, body: BodyDep, settings: SettingsDep) -> dict:
    """With v1's body ``{queueId, project_id, tenant_id, startRow, endRow}`` (the v1 UI's
    Documents page): the project's documents with their review status and tree configuration
    (rows from the Annotations API). Otherwise: the queue's item files."""
    if body.has("project_id", "tenant_id"):
        return _v1_documents(session, body, settings)
    queue_id = body.get("queue_id")
    q = service.get_queue(session, queue_id) if queue_id else None
    item_conds = search.filters(body, queue_id=q.id if q else None)
    start, end = _page(body, settings)
    text = str(body.get("file_search", default="")).strip()
    stmt = (
        select(ItemFile, Item).join(Item, Item.display_id == ItemFile.item_pk).where(*item_conds)
    )
    if text:
        stmt = stmt.where(ItemFile.file_name.ilike(f"%{search.escape_like(text)}%", escape="\\"))
    total = session.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    sort = str(body.get("sort", default="") or "")
    desc = not sort.lstrip("-") or sort.startswith("-") or sort in ("undefined", "null")
    col = {"file_name": ItemFile.file_name, "status": Item.status}.get(
        sort.lstrip("-"), ItemFile.created_at
    )
    rows = session.execute(
        stmt.order_by(col.desc() if desc else col.asc(), ItemFile.id)
        .offset(start)
        .limit(max(end - start, 0))
    ).all()
    return {
        "rows": [
            serialize.file_out(f)
            | {
                "trace_id": serialize.dashed(i.id),
                "incremented_display_id": i.display_id,
                "display_roots_id": i.display_roots_id,
                "item_status": i.status,
                "queue_id": i.queue_id,
            }
            for f, i in rows
        ],
        "metadata": {"total_document_count": total},
        "lastRow": total,
    }


def _v1_documents(session: Session, body: Body, settings: Settings) -> dict:
    """The packages linked to the queue's work items (newest first), as v1 document rows."""
    q = service.get_queue(session, body.get("queue_id"))
    linked = session.execute(
        select(ItemFile.package_id, Item.id)
        .join(Item, Item.display_id == ItemFile.item_pk)
        .where(
            Item.queue_id == q.id,
            Item.deleted_at.is_(None),
            ItemFile.package_id.is_not(None),
        )
        .order_by(ItemFile.package_id.desc())
    ).all()
    traces: dict[int, str] = {}
    for package_id, trace in linked:
        traces.setdefault(int(package_id), trace)
    payload = {
        k: body.raw[k]
        for k in ("startRow", "endRow", "start_row", "end_row", "statuses", "search")
        if k in body.raw
    }
    payload |= {"package_ids": list(traces), "work_items": {str(k): v for k, v in traces.items()}}
    if not settings.annotations_api_url:
        raise HTTPException(503, "document search needs the Annotations API")
    headers = (
        {"Ocp-Apim-Subscription-Key": settings.annotations_api_key}
        if settings.annotations_api_key
        else {}
    )
    try:
        r = httpx.post(
            f"{settings.annotations_api_url.rstrip('/')}/docs/searches",
            json=payload,
            headers=headers,
            timeout=60,
        )
    except httpx.HTTPError as exc:
        raise HTTPException(503, f"Annotations API unavailable: {exc}") from exc
    if r.status_code != 200:
        raise HTTPException(502, f"Annotations API: {r.status_code} {r.text[:200]}")
    return r.json()


# ============================================================================== items: one item


@router.get("/items/{item_id}", tags=["items"], summary="Item details")
def get_item(item_id: str, session: SessionDep) -> dict:
    return serialize.item_info(service.get_item(session, item_id))


@router.put("/items/{item_id}", tags=["items"], summary="Update item columns")
def update_item(item_id: str, session: SessionDep, body: BodyDep, actor: ActorDep) -> dict:
    item = service.get_item(session, item_id)
    changes = []
    for attr, key in (
        ("priority", "priority"),
        ("display_roots_id", "display_roots_id"),
    ):
        v = body.get(key)
        if v not in (None, "", 0) or (key == "priority" and v == 0 and body.has("priority")):
            setattr(item, attr, v)
            changes.append(key)
    if body.has("assignee_id"):
        item.assignee_id = body.raw.get("assigneeId", body.get("assignee_id")) or None
        item.assigned_by = actor
        changes.append("assignee_id")
    service.touch(item, actor)
    service.log(session, item, "updated", actor, message=", ".join(changes) or None)
    return serialize.item_info(item)


@router.delete("/items/{item_id}/delete", tags=["items"], summary="Delete an item (soft)")
def delete_item(item_id: str, session: SessionDep, actor: ActorDep) -> dict:
    item = service.get_item(session, item_id)
    # Every attempt of the trace id goes, or the previous attempt would resurface.
    rows = session.scalars(
        select(Item)
        .where(Item.id == item.id, Item.deleted_at.is_(None))
        .order_by(Item.display_id)  # fixed lock order
    )
    for row in rows:
        row.deleted_at = models.now()
        row.locked = False
        service.touch(row, actor)
        service.log(session, row, "deleted", actor)
    return ok("Item deleted", itemId=serialize.dashed(item.id))


@router.post("/items/{item_id}/data", tags=["items"], summary="Overwrite item data")
def set_item_data(item_id: str, session: SessionDep, body: BodyDep, actor: ActorDep) -> dict:
    item = service.get_item(session, item_id)
    data = body.raw.get("data")
    if not isinstance(data, dict):
        raise HTTPException(422, "data must be a JSON object")
    item.data = data
    service.touch(item, actor)
    service.log(session, item, "data", actor)
    return ok("Item data updated")


@router.get("/items/{item_id}/item_status", tags=["items"], summary="Item status")
def get_item_status(item_id: str, session: SessionDep) -> dict:
    item = service.get_item(session, item_id)
    return {"status": item.status, "locked": item.locked, "previousStatus": item.previous_status}


@router.put("/items/{item_id}/status", tags=["items"], summary="Set item status")
def put_status(item_id: str, session: SessionDep, body: BodyDep, actor: ActorDep) -> dict:
    item = service.get_item(session, item_id, for_update=True)
    status = body.get("status")
    if not status:
        raise HTTPException(422, "status is required")
    service.set_status(
        session,
        item,
        status,
        actor=actor,
        exception_message=body.get("exception_message"),
        defer_until=parse_dt(body.get("defer_until"), "deferUntil"),
        auto_retry=bool(body.get("auto_retry", default=False)),
        user_assign=body.get("user_assign"),
        manual=bool(body.get("manual", default=False)),
    )
    return ok("Status updated", status=item.status, itemId=serialize.dashed(item.id))


@router.put("/items/{item_id}/lock", tags=["items"], summary="Lock an item")
def lock(item_id: str, session: SessionDep, actor: ActorDep) -> dict:
    item = service.get_item(session, item_id, for_update=True)
    return ok(service.lock_item(session, item, actor), status=item.status)


@router.put("/items/{item_id}/unlock", tags=["items"], summary="Unlock an item")
def unlock(
    item_id: str,
    session: SessionDep,
    actor: ActorDep,
    force: Annotated[bool, Query(description="Unlock even if another resource holds it")] = False,
) -> dict:
    item = service.get_item(session, item_id, for_update=True)
    return ok(service.unlock_item(session, item, actor, force=force), status=item.status)


@router.post(
    "/items/{item_id}/force_retry",
    tags=["items"],
    summary="system_exception → new queued item (new Item ID, same trace id)",
)
def force_retry(item_id: str, session: SessionDep, actor: ActorDep) -> dict:
    item = service.get_item(session, item_id, for_update=True)
    retry = service.force_retry(session, item, actor)
    return ok(
        "Item queued for retry",
        status=retry.status,
        itemId=serialize.dashed(retry.id),
        incrementedDisplayId=retry.display_id,
        previousIncrementedDisplayId=item.display_id,
    )


@router.get("/items/{item_id}/events", tags=["items"], summary="Audit trail (newest first)")
def item_events(item_id: str, session: SessionDep) -> list[dict]:
    item = service.get_item(session, item_id)
    # Every attempt of the trace id (force_retry adds rows), newest first.
    return [serialize.event_out(e) for e in service.events_for_trace(session, item)]


# ---------------------------------------------------------------- tags


@router.get("/items/{item_id}/tag", tags=["tags"], summary="Item tags (list of strings)")
def get_tags(item_id: str, session: SessionDep) -> list[str]:
    return service.get_item(session, item_id).tag_names


def _tag_list(body: Body) -> list[str]:
    tags = body.get("tags", default=[])
    if isinstance(tags, str):
        tags = [tags]
    return [str(t) for t in tags if str(t).strip()]


@router.put("/items/{item_id}/tag", tags=["tags"], summary="Add tags")
def add_tags(item_id: str, session: SessionDep, body: BodyDep, actor: ActorDep) -> dict:
    item = service.get_item(session, item_id)
    have = set(item.tag_names)
    added = [t for t in dict.fromkeys(_tag_list(body)) if t not in have]
    item.tags.extend(ItemTag(tag=t) for t in added)
    service.touch(item, actor)
    if added:
        service.log(session, item, "tags_added", actor, message=", ".join(added))
    return ok("Tags added", tags=item.tag_names)


@router.delete("/items/{item_id}/tag", tags=["tags"], summary="Delete tags")
def delete_tags(item_id: str, session: SessionDep, body: BodyDep, actor: ActorDep) -> dict:
    item = service.get_item(session, item_id)
    drop = set(_tag_list(body))
    item.tags = [t for t in item.tags if t.tag not in drop]
    service.touch(item, actor)
    service.log(session, item, "tags_deleted", actor, message=", ".join(sorted(drop)))
    return ok("Tags deleted", tags=item.tag_names)


# ------------------------------------------------ notes / form search text / reporting json


def _text_field(attr: str, key: str, label: str):
    def overwrite(item_id: str, session: SessionDep, body: BodyDep, actor: ActorDep) -> dict:
        item = service.get_item(session, item_id)
        setattr(item, attr, str(body.get(key, default="")))
        service.touch(item, actor)
        service.log(session, item, f"{attr}_set", actor)
        return ok(f"{label} overwritten", **{key: getattr(item, attr)})

    def append(item_id: str, session: SessionDep, body: BodyDep, actor: ActorDep) -> dict:
        item = service.get_item(session, item_id)
        add = str(body.get(key, default=""))
        current = getattr(item, attr) or ""
        setattr(item, attr, f"{current} {add}".strip() if current else add)
        service.touch(item, actor)
        service.log(session, item, f"{attr}_appended", actor, message=add)
        return ok(f"{label} updated", **{key: getattr(item, attr)})

    def clear(item_id: str, session: SessionDep, actor: ActorDep) -> dict:
        item = service.get_item(session, item_id)
        setattr(item, attr, "")
        service.touch(item, actor)
        service.log(session, item, f"{attr}_deleted", actor)
        return ok(f"{label} deleted")

    return overwrite, append, clear


for _attr, _key, _path, _label in (
    ("notes", "notes", "notes", "Notes"),
    ("form_search_text", "form_search_text", "form_search_text", "Form search text"),
):
    _ow, _ap, _cl = _text_field(_attr, _key, _label)
    router.add_api_route(
        f"/items/{{item_id}}/{_path}",
        _ow,
        methods=["POST"],
        tags=["items"],
        summary=f"Overwrite {_label.lower()}",
        name=f"overwrite_{_attr}",
    )
    router.add_api_route(
        f"/items/{{item_id}}/{_path}",
        _ap,
        methods=["PUT"],
        tags=["items"],
        summary=f"Append to {_label.lower()}",
        name=f"append_{_attr}",
    )
    router.add_api_route(
        f"/items/{{item_id}}/{_path}",
        _cl,
        methods=["DELETE"],
        tags=["items"],
        summary=f"Delete {_label.lower()}",
        name=f"delete_{_attr}",
    )


def _reporting_payload(body: Body) -> dict:
    rj = body.raw.get("reportingJson", body.raw.get("reporting_json"))
    if rj is None and "key" in body.raw:
        return {str(body.raw["key"]): body.raw.get("value")}
    if not isinstance(rj, dict):
        raise HTTPException(422, "reportingJson must be a JSON object")
    return rj


@router.post("/items/{item_id}/reporting_json", tags=["items"], summary="Overwrite reporting json")
def overwrite_reporting(item_id: str, session: SessionDep, body: BodyDep, actor: ActorDep) -> dict:
    item = service.get_item(session, item_id)
    item.reporting_json = _reporting_payload(body)
    service.touch(item, actor)
    service.log(session, item, "reporting_json_set", actor)
    return ok("Reporting json overwritten", reportingJson=item.reporting_json)


@router.put("/items/{item_id}/reporting_json", tags=["items"], summary="Merge into reporting json")
def merge_reporting(item_id: str, session: SessionDep, body: BodyDep, actor: ActorDep) -> dict:
    item = service.get_item(session, item_id)
    item.reporting_json = {**(item.reporting_json or {}), **_reporting_payload(body)}
    service.touch(item, actor)
    service.log(session, item, "reporting_json_updated", actor)
    return ok("Reporting json updated", reportingJson=item.reporting_json)


@router.delete("/items/{item_id}/reporting_json", tags=["items"], summary="Delete reporting json")
def delete_reporting(item_id: str, session: SessionDep, actor: ActorDep) -> dict:
    item = service.get_item(session, item_id)
    item.reporting_json = None
    service.touch(item, actor)
    service.log(session, item, "reporting_json_deleted", actor)
    return ok("Reporting json deleted")


# ---------------------------------------------------------------- files & documents


@router.post(
    "/items/{item_id}/file", tags=["files"], summary="Attach files (fileName, filePath, packageId)"
)
def insert_files(item_id: str, session: SessionDep, body: BodyDep, actor: ActorDep) -> dict:
    item = service.get_item(session, item_id)
    files = body.get("files", default=[])
    if not isinstance(files, list) or not files:
        raise HTTPException(422, "files must be a non-empty list")
    created = []
    for f in files:
        b = Body(f)
        name = b.get("file_name")
        path = b.get("file_path")
        if not name or path in (None, ""):
            raise HTTPException(422, "each file needs fileName and filePath")
        pkg = b.get("package_id")
        try:
            pkg = int(pkg) if pkg not in (None, "") else None
        except (TypeError, ValueError):
            pkg = None
        row = ItemFile(
            item_pk=item.display_id, file_name=str(name), file_path=str(path), package_id=pkg
        )
        session.add(row)
        created.append(row)
    session.flush()
    service.touch(item, actor)
    service.log(
        session, item, "files_added", actor, message=", ".join(f.file_name for f in created)
    )
    return ok("Files inserted", fileIds=[f.id for f in created])


@router.get("/items/{item_id}/file", tags=["files"], summary="Item files")
def get_files(item_id: str, session: SessionDep) -> list[dict]:
    item = service.get_item(session, item_id)
    files = session.scalars(
        select(ItemFile).where(ItemFile.item_pk == item.display_id).order_by(ItemFile.created_at)
    ).all()
    return [serialize.file_out(f) for f in files]


@router.put("/files/{file_id}/move", tags=["files"], summary="Move a file to another item")
def move_file(file_id: str, session: SessionDep, body: BodyDep, actor: ActorDep) -> dict:
    f = session.get(ItemFile, norm_id(file_id))
    if f is None:
        raise HTTPException(400, f"File {file_id} cannot be found")
    target = service.get_item(session, str(body.get("item_id", default="")))
    source = session.get(Item, f.item_pk)
    f.item_pk = target.display_id
    service.touch(target, actor)
    service.log(session, target, "file_moved_in", actor, message=f.file_name)
    if source is not None:
        service.log(session, source, "file_moved_out", actor, message=f.file_name)
    return ok("File moved", itemId=serialize.dashed(target.id))


@router.delete("/files/{file_id}/delete", tags=["files"], summary="Delete a file")
def delete_file(file_id: str, session: SessionDep, actor: ActorDep) -> dict:
    f = session.get(ItemFile, norm_id(file_id))
    if f is None:
        raise HTTPException(400, f"File {file_id} cannot be found")
    item = session.get(Item, f.item_pk)
    session.delete(f)
    if item is not None:
        service.log(session, item, "file_deleted", actor, message=f.file_name)
    return ok("File deleted")


@router.get(
    "/items/{item_id}/docs",
    tags=["files"],
    summary="Documents: packages linked in the annotations API plus files with a packageId",
)
def get_docs(item_id: str, session: SessionDep, settings: SettingsDep) -> list[dict]:
    item = service.get_item(session, item_id)
    docs: dict[int, dict] = {}
    try:
        headers = (
            {"Ocp-Apim-Subscription-Key": settings.annotations_api_key}
            if settings.annotations_api_key
            else {}
        )
        r = httpx.get(
            f"{settings.annotations_api_url.rstrip('/')}/{item.id}/documents",
            headers=headers,
            timeout=5,
        )
        if r.status_code == 200:
            for d in r.json():
                docs[int(d["package_id"])] = d | {"source": "annotations"}
    except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
        logger.warning("annotations API unavailable for item %s: %s", item.id, exc)
    for f in session.scalars(select(ItemFile).where(ItemFile.item_pk == item.display_id)):
        if f.package_id is not None and f.package_id not in docs:
            docs[f.package_id] = {
                "package_id": f.package_id,
                "file_name": f.file_name,
                "source": "file",
            }
    return list(docs.values())


# ---------------------------------------------------------------- form elements


@router.post("/form/{item_id}", tags=["form"], summary="Add form elements (encrypted or platform)")
def add_form_elements(item_id: str, session: SessionDep, body: BodyDep, actor: ActorDep) -> dict:
    item = service.get_item(session, item_id)
    form = body.get("form")
    if isinstance(form, dict):
        # RootsQueueClient.add_item_form_elements_platform: the item's whole form (label, value,
        # type, permission, ...). It replaces the previous one, so reruns don't duplicate rows.
        elements = form.get("form_elements", form.get("formElements")) or []
        if not isinstance(elements, list) or not all(isinstance(e, dict) for e in elements):
            raise HTTPException(422, "form.form_elements must be a list of objects")
        for old in session.scalars(
            select(FormElement).where(FormElement.item_pk == item.display_id)
        ):
            if serialize.platform_form_element(old) is not None:
                session.delete(old)
        ordered = sorted(
            enumerate(elements),
            key=lambda p: (50 if p[1].get("sort_order") is None else p[1]["sort_order"], p[0]),
        )
        rows = [
            FormElement(item_pk=item.display_id, encrypted_form_element=json.dumps(e))
            for _, e in ordered
        ]
        session.add_all(rows)
        session.flush()
        service.touch(item, actor)
        service.log(session, item, "form_updated", actor, message=str(len(rows)))
        return ok("Form elements added", formElementIds=[r.id for r in rows])
    elements = body.get("form_elements", default=[])
    if not isinstance(elements, list):
        raise HTTPException(422, "formElements must be a list of encrypted strings")
    rows = [FormElement(item_pk=item.display_id, encrypted_form_element=str(e)) for e in elements]
    session.add_all(rows)
    session.flush()
    service.touch(item, actor)
    service.log(session, item, "form_elements_added", actor, message=str(len(rows)))
    return ok("Form elements added", formElementIds=[r.id for r in rows])


@router.get("/items/{item_id}/form", tags=["form"], summary="The item's form (v1 shape)")
def get_item_form(item_id: str, session: SessionDep) -> dict:
    """What the work item details screen shows: one row per form element (label: value)."""
    item = service.get_item(session, item_id)
    rows = session.scalars(
        select(FormElement)
        .where(FormElement.item_pk == item.display_id)
        .order_by(FormElement.created_at, FormElement.id)
    ).all()
    elements = [e for e in (serialize.platform_form_element(r) for r in rows) if e is not None]
    return {"form": {"form_elements": elements}}


@router.get("/form/{item_id}", tags=["form"], summary="Encrypted form elements")
def get_form_elements(item_id: str, session: SessionDep) -> dict:
    item = service.get_item(session, item_id)
    rows = session.scalars(
        select(FormElement)
        .where(FormElement.item_pk == item.display_id)
        .order_by(FormElement.created_at)
    ).all()
    return {"formElements": [serialize.form_element_out(r) for r in rows]}


@router.put("/form_element/{element_id}", tags=["form"], summary="Replace one form element")
def update_form_element(element_id: str, session: SessionDep, body: BodyDep) -> dict:
    fe = session.get(FormElement, norm_id(element_id))
    if fe is None:
        raise HTTPException(400, f"Form element {element_id} cannot be found")
    value = body.get("form_element")
    if not value:
        raise HTTPException(422, "form_element is required")
    fe.encrypted_form_element = str(value)
    return ok("Form element updated", id=fe.id)


@router.delete("/form_element/{element_id}", tags=["form"], summary="Delete one form element")
def delete_form_element(element_id: str, session: SessionDep) -> dict:
    fe = session.get(FormElement, norm_id(element_id))
    if fe is None:
        raise HTTPException(400, f"Form element {element_id} cannot be found")
    session.delete(fe)
    return ok("Form element deleted")


# ============================================================================== retention


@router.post(
    "/admin/retire_items",
    tags=["admin"],
    summary="Delete finished items older than N days (default RETENTION_DAYS)",
)
def retire_items(body: BodyDep, settings: SettingsDep) -> dict:
    from app.retention import annotations_retirer
    from app.retention import retire_items as run

    raw_days = body.get("older_than_days")
    days = settings.retention_days if raw_days in (None, "") else int(raw_days)
    if days < 30:
        raise HTTPException(422, "older_than_days must be at least 30")
    dry_run = bool(body.get("dry_run", default=False))
    try:
        n = run(
            older_than_days=days,
            statuses=body.get("statuses") or settings.retire_statuses,
            batch_size=settings.retire_batch_size,
            dry_run=dry_run,
            retire_documents=None if dry_run else annotations_retirer(settings),
        )
    except (RuntimeError, httpx.HTTPError) as exc:
        # The batch whose documents could not be retired was kept; earlier batches are gone.
        raise HTTPException(502, f"Retention stopped: {exc}") from exc
    return ok("Dry run" if dry_run else "Items retired", count=n, olderThanDays=days)
