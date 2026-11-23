"""Request helpers: tolerant JSON bodies, camelCase/snake_case keys, ids, statuses, datetimes."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import Any

from fastapi import HTTPException, Request

from app import models

_CAMEL = re.compile(r"(?<!^)(?=[A-Z])")


def snake(key: str) -> str:
    return _CAMEL.sub("_", key).replace("-", "_").replace(" ", "_").lower()


async def json_body(request: Request) -> Any:
    """The JSON body, or {} when empty (RootsQueueClient sends no body on several PUT/POSTs)."""
    raw = await request.body()
    if not raw or not raw.strip():
        return {}
    try:
        body = json.loads(raw)
        # The v1 UI sends bodies double-encoded (a JSON string holding the JSON object).
        if isinstance(body, str) and body.strip()[:1] in ("{", "["):
            body = json.loads(body)
        return body
    except ValueError as exc:
        raise HTTPException(400, f"request body is not valid JSON: {exc}") from exc


class Body:
    """Read top-level keys by their snake_case name, whatever casing the caller used."""

    def __init__(self, raw: Any):
        if raw is None:
            raw = {}
        if not isinstance(raw, dict):
            raise HTTPException(422, "request body must be a JSON object")
        self.raw = raw
        self._by_snake = {snake(k): v for k, v in raw.items()}

    def get(self, *names: str, default: Any = None) -> Any:
        for n in names:
            if n in self._by_snake and self._by_snake[n] is not None:
                return self._by_snake[n]
        return default

    def has(self, *names: str) -> bool:
        return any(n in self._by_snake for n in names)


def norm_id(value: str) -> str:
    """Item / queue / file ids: accept dashed UUIDs and any case; store 32-char lowercase hex."""
    return value.replace("-", "").strip().lower()


def dashed(hex_id: str) -> str:
    h = hex_id.replace("-", "")
    if len(h) != 32:
        return hex_id
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"


_STATUS_ALIASES = {
    # Older names: a finished item is automated; "retry" hands it back to the queue.
    "complete": models.AUTOMATED,
    "completed": models.AUTOMATED,
    "retrying": models.QUEUED,
    "retry": models.QUEUED,
    "closed": models.MANUALLY_CLOSED,
    "manual_close": models.MANUALLY_CLOSED,
    "business_exception": models.REQUIRES_REVIEW,
    "review": models.REQUIRES_REVIEW,
    "exception": models.SYSTEM_EXCEPTION,
    "inprogress": models.IN_PROGRESS,
    "pending": models.QUEUED,
}


def norm_status(value: str | None, *, strict: bool = True) -> str | None:
    if value is None:
        return None
    s = re.sub(r"[\s\-]+", "_", str(value).strip().lower())
    s = _STATUS_ALIASES.get(s, s)
    if s not in models.STATUSES:
        if strict:
            raise HTTPException(
                422, f"unknown status {value!r}; expected one of {', '.join(models.STATUSES)}"
            )
        return None
    return s


def parse_dt(value: Any, field: str = "datetime") -> datetime | None:
    """ISO-8601 or 'YYYY-MM-DD HH:MM:SS' (what str(datetime) gives); naive means UTC."""
    if value in (None, "", "None", "null"):
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        s = str(value).strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)
        except ValueError as exc:
            raise HTTPException(422, f"{field}: invalid datetime {value!r}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")
