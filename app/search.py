"""Item search filters shared by POST /items/searches (v1), /items/v2/searches and the UI.

Accepts the key spellings callers actually send: camelCase or snake_case, the platform's
``created_at_start_datetime`` / ``updated_start_datetime`` forms, and AG Grid date filter models
(``"updated_at": {"dateFrom", "dateTo", "type": "inRange"}``).
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from sqlalchemy import exists, or_, select
from sqlalchemy.sql import Select

from app.models import Item, ItemTag
from app.util import Body, norm_id, norm_status, parse_dt


def escape_like(text: str) -> str:
    return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _statuses(values: Any) -> list[str]:
    if not values:
        return []
    if isinstance(values, str):
        values = [values]
    return [s for s in (norm_status(v) for v in values) if s]


def _range(body: Body, col, prefix: str) -> list:
    """created/updated date range from flat keys or an AG Grid filter model."""
    conds = []
    start = body.get(
        f"{prefix}_at_start_datetime", f"{prefix}_start_datetime", f"{prefix}_at_start"
    )
    end = body.get(f"{prefix}_at_end_datetime", f"{prefix}_end_datetime", f"{prefix}_at_end")
    model = body.get(f"{prefix}_at")
    if isinstance(model, dict):
        kind = model.get("type", "inRange")
        d_from, d_to = model.get("dateFrom"), model.get("dateTo")
        if kind in ("inRange", "greaterThan", "greaterThanOrEqual") and d_from:
            start = start or d_from
        if kind == "inRange" and d_to:
            end = end or d_to
        if kind in ("lessThan", "lessThanOrEqual") and d_from:
            end = end or d_from
        if kind == "equals" and d_from:
            day = parse_dt(d_from, f"{prefix}_at")
            if day is not None:
                day_start = day.replace(hour=0, minute=0, second=0, microsecond=0)
                conds.append(col >= day_start)
                conds.append(col < day_start + timedelta(days=1))
    if start:
        parsed_start = parse_dt(start, f"{prefix} start")
        if parsed_start is not None:
            conds.append(col >= parsed_start)
    if end:
        parsed_end = parse_dt(end, f"{prefix} end")
        if parsed_end is not None:
            conds.append(col <= parsed_end)
    return conds


def filters(body: Body, *, queue_id: str | None, with_status: bool = True) -> list:
    conds: list = [Item.deleted_at.is_(None)]
    if queue_id:
        conds.append(Item.queue_id == queue_id)
    statuses = _statuses(body.get("statuses", "status"))
    if with_status and statuses:
        conds.append(Item.status.in_(statuses))

    roots = body.get("roots_id", "display_roots_id", "client_id")
    if roots:
        conds.append(or_(Item.roots_id == str(roots), Item.display_roots_id == str(roots)))

    text = str(body.get("search", "search_text", "query", default="")).strip()
    if text:
        like = f"%{escape_like(text)}%"
        alts = [
            Item.display_roots_id.ilike(like, escape="\\"),
            Item.roots_id.ilike(like, escape="\\"),
        ]
        hexed = norm_id(text)
        if hexed and all(c in "0123456789abcdef" for c in hexed):
            alts.append(Item.id.like(f"{hexed}%"))
        if text.isascii() and text.isdigit():
            num = int(text)
            if 0 <= num <= 2_147_483_647:
                alts.append(Item.display_id == num)
        conds.append(or_(*alts))

    if body.has("assignee_ids"):
        ids = body.get("assignee_ids") or []
        if ids:
            wanted = [i for i in ids if i]
            parts = []
            if wanted:
                parts.append(Item.assignee_id.in_(wanted))
            if any(i is None or i == "" or i == "unassigned" for i in ids):
                parts.append(Item.assignee_id.is_(None))
            conds.append(or_(*parts))

    tags = body.get("tags") or []
    if isinstance(tags, str):
        tags = [tags]
    if tags:
        if body.get("all_tags_required", default=False):
            for t in tags:
                conds.append(exists().where(ItemTag.item_pk == Item.display_id, ItemTag.tag == t))
        else:
            conds.append(exists().where(ItemTag.item_pk == Item.display_id, ItemTag.tag.in_(tags)))

    locked = body.get("locked")
    if locked is not None:
        conds.append(Item.locked == bool(locked))

    conds += _range(body, Item.created_at, "created")
    conds += _range(body, Item.updated_at, "updated")
    return conds


SORT_COLUMNS = {
    "incremented_display_id": Item.display_id,
    "item_id": Item.display_id,
    "display_id": Item.display_id,
    "display_roots_id": Item.display_roots_id,
    "client_id": Item.display_roots_id,
    "trace_id": Item.id,
    "status": Item.status,
    "created_at": Item.created_at,
    "updated_at": Item.updated_at,
    "assignee_id": Item.assignee_id,
    "priority": Item.priority,
    "attempts": Item.attempts,
}


def order_by(body: Body) -> list:
    model = body.get("sort_model")
    keys: list[tuple[str, bool]] = []
    if isinstance(model, list):
        for m in model:
            if isinstance(m, dict) and m.get("colId"):
                keys.append((str(m["colId"]), str(m.get("sort", "asc")).lower() == "desc"))
    sort = body.get("sort")
    if isinstance(sort, str) and sort not in ("", "undefined", "null"):
        for part in sort.split(","):
            part = part.strip()
            if part:
                keys.append((part.lstrip("-"), part.startswith("-")))
    out = []
    for key, desc in keys:
        col = SORT_COLUMNS.get(key)
        if col is not None:
            out.append(col.desc() if desc else col.asc())
    if not out:
        out = [Item.created_at.desc()]
    out.append(Item.display_id.desc())
    return out


def item_query(conds: list) -> Select:
    return select(Item).where(*conds)
