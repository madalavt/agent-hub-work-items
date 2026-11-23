"""Response shapes. Some are strict, because RootsQueueClient builds dataclasses from them:

* ``POST /items/next`` → exactly the ``Item`` dataclass fields (an extra key is a TypeError).
* ``GET /queues/{id}`` → exactly the ``Queue`` dataclass fields.
"""

from __future__ import annotations

import csv
import io
import json
from typing import Any

from app.models import FormElement, Item, ItemEvent, ItemFile, Queue, Resource, User
from app.util import dashed, iso


def next_item(item: Item) -> dict[str, Any]:
    return {
        "itemId": dashed(item.id),
        "rootsId": item.roots_id,
        "data": item.data or {},
        "status": item.status,
        "attempts": item.attempts,
        "lockedStatus": item.locked,
        "previousStatus": item.previous_status,
    }


def reporting_str(value: Any) -> str | None:
    if value is None:
        return None
    return value if isinstance(value, str) else str(value)


def item_info(item: Item) -> dict[str, Any]:
    """GET /items/{id}/ (same keys as the platform's work item manager)."""
    return {
        "incrementedDisplayId": item.display_id,
        "createdAt": iso(item.created_at),
        "updatedAt": iso(item.updated_at),
        "deletedAt": iso(item.deleted_at),
        "assigneeId": item.assignee_id,
        "tenantId": item.tenant_id,
        "assignedBy": item.assigned_by,
        "priority": item.priority,
        "status": item.status,
        "locked": item.locked,
        "attempts": item.attempts,
        "deferCount": item.defer_count,
        "nextReviewDate": iso(item.next_review_date),
        "completedAt": iso(item.completed_at),
        "workTimeSeconds": round(item.work_time_seconds or 0.0, 6),
        "exceptionDate": iso(item.exception_date),
        "exceptionReason": item.exception_reason,
        "data": item.data or {},
        "lastLockedAt": iso(item.last_locked_at),
        # The platform returns reporting json as a string (Python dict repr); callers parse it.
        "reportingJson": reporting_str(item.reporting_json),
        "formSearchText": item.form_search_text or "",
        "notes": item.notes or "",
        "displayRootsId": item.display_roots_id,
        "rootsId": item.roots_id,
        "lastUpdatedBy": item.last_updated_by,
        "lastExceptionType": item.last_exception_type,
        "queue": item.queue_id,
        "lockingResource": item.locking_resource,
        "previousStatus": item.previous_status,
        "tags": item.tag_names,
        "id": dashed(item.id),
    }


def package_details(files: list[ItemFile]) -> list[dict[str, Any]]:
    return [
        {"package_id": f.package_id, "file_id": f.id, "file_name": f.file_name}
        for f in files
        if f.package_id is not None
    ]


def v2_row(item: Item, files: list[ItemFile], users: dict[str, str]) -> dict[str, Any]:
    """POST /items/v2/searches row (keys match the platform's AG Grid search)."""
    return {
        "display_roots_id": item.display_roots_id,
        "trace_id": dashed(item.id),
        "status": item.status,
        "created_at": iso(item.created_at),
        "updated_at": iso(item.updated_at),
        "assignee_id": item.assignee_id,
        "assignee_name": users.get(item.assignee_id or ""),
        "is_assignable": True,
        "incremented_display_id": item.display_id,
        "package_details": package_details(files),
        "exception_details": item.exception_reason,
        "exception_type": item.last_exception_type,
        "file_count": len(files),
        "locked": item.locked,
        "locking_resource": item.locking_resource,
        "priority": item.priority,
        "attempts": item.attempts,
        "tags": item.tag_names,
        "queue_id": item.queue_id,
    }


V2_COLUMNS = [
    {"id": 1, "field": "display_roots_id", "headerName": "Client ID"},
    {"id": 2, "field": "trace_id", "headerName": "Trace ID"},
    {"id": 3, "field": "status", "headerName": "Status"},
    {"id": 4, "field": "created_at", "headerName": "Created at (UTC)"},
    {"id": 5, "field": "updated_at", "headerName": "Updated at (UTC)"},
    {"id": 6, "field": "assignee_id", "headerName": "Assigned To"},
    {"id": 7, "field": "is_assignable", "headerName": "Is Assignable"},
    {"id": 8, "field": "incremented_display_id", "headerName": "Incremented Display ID"},
]


def _csv_safe(value: Any) -> Any:
    """Prevent spreadsheet formula injection (CWE-1236) on user/external string cells."""
    if isinstance(value, str) and value and value[0] in ("=", "+", "-", "@", "\t", "\r"):
        return "'" + value
    return value


def v2_csv(rows: list[dict[str, Any]]) -> str:
    """The export=true variant of the v2 search (same columns as the platform's export)."""
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(
        [
            "Client ID",
            "Item ID",
            "Trace ID",
            "Status",
            "Created at (UTC)",
            "Updated at (UTC)",
            "Assigned To",
            "Document Count",
            "Exception Type",
            "Exception Details",
        ]
    )
    for r in rows:
        w.writerow(
            [
                _csv_safe(r["display_roots_id"] or ""),
                r["incremented_display_id"],
                r["trace_id"],
                r["status"],
                r["created_at"],
                r["updated_at"],
                _csv_safe(r["assignee_name"] or r["assignee_id"] or ""),
                r["file_count"],
                _csv_safe(r["exception_type"] or ""),
                _csv_safe(r["exception_details"] or ""),
            ]
        )
    return buf.getvalue()


def queue_strict(q: Queue) -> dict[str, Any]:
    return {
        "id": q.id,
        "name": q.name,
        "maxAttempts": q.max_attempts,
        "rootsId": q.roots_id,
        "createdAt": iso(q.created_at),
        "updatedAt": iso(q.updated_at),
        "deletedAt": iso(q.deleted_at),
        "organization": q.organization,
    }


def queue_details(q: Queue, counts: dict[str, int] | None = None) -> dict[str, Any]:
    out = queue_strict(q) | {
        "description": q.description,
        "tenantId": q.tenant_id,
        "projectId": q.project_id,  # deprecated tree-configuration link (old data)
    }
    if counts is not None:
        out["counts"] = counts
    return out


def file_out(f: ItemFile, item: Item | None = None) -> dict[str, Any]:
    out = {
        "id": f.id,
        "fileName": f.file_name,
        "filePath": f.file_path,
        "packageId": f.package_id,
        "createdAt": iso(f.created_at),
    }
    if item is not None:
        out["itemId"] = dashed(item.id)
    return out


def platform_form_element(fe: FormElement) -> dict[str, Any] | None:
    """A platform form element (stored as JSON) in v1's shape; None for encrypted ones."""
    try:
        el = json.loads(fe.encrypted_form_element)
    except ValueError:
        return None
    if not isinstance(el, dict):
        return None
    validation = el.get("validation") or {}
    return {
        "id": fe.id,
        "label": el.get("label"),
        "value": el.get("value"),
        "type": el.get("type") or "string",
        "permission": el.get("permission") or "readonly",
        "options": el.get("options") or [],
        "info": el.get("info"),
        "validation": {
            "regex_rule": validation.get("regex_rule") or el.get("validation_regex") or None,
            "message": validation.get("message") or el.get("validation_message") or None,
        },
    }


def form_element_out(fe: FormElement) -> dict[str, Any]:
    return {
        "element_id": fe.id,
        "id": fe.id,
        "encrypted_form_element": fe.encrypted_form_element,
        "created_at": iso(fe.created_at),
        "updated_at": iso(fe.updated_at),
    }


def event_out(e: ItemEvent) -> dict[str, Any]:
    return {
        "id": e.id,
        "at": iso(e.at),
        "event": e.event,
        "fromStatus": e.from_status,
        "toStatus": e.to_status,
        "actor": e.actor,
        "message": e.message,
    }


def user_out(u: User) -> dict[str, Any]:
    return {"id": u.id, "name": u.name, "email": u.email, "createdAt": iso(u.created_at)}


def resource_out(r: Resource) -> dict[str, Any]:
    return {
        "id": r.id,
        "resourceName": r.name,
        "organizationId": r.organization_id,
        "createdAt": iso(r.created_at),
    }
