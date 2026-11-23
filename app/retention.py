"""Retire old work items so the live tables stay small (items are retired every 6 months).

Deletes items whose last update is older than ``older_than`` and whose status is one of
``statuses`` (finished work), plus soft-deleted items of the same age. Their tags, files, form
elements and events go with them (ON DELETE CASCADE). Deletes run in small batches, each in its
own transaction, so the API keeps serving while a retirement runs.

Before a batch is deleted, the Annotations API deletes the documents linked to those items
(``POST /admin/retire_work_items``: packages, page text, tables, uploaded files, review state).
If that call fails, the batch is kept and the run stops; the next run tries again. A trace id
that still has a live row (``force_retry`` keeps the old attempt) is not sent.

Run it on a schedule (e.g. a Kubernetes CronJob):  ``python -m app.retention --days 180``
or call ``POST /admin/retire_items``.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable
from datetime import timedelta

import httpx
from sqlalchemy import delete, or_, select

from app import db
from app.models import Item, now

logger = logging.getLogger(__name__)


def retire_items(
    *,
    older_than_days: int,
    statuses: list[str],
    batch_size: int = 2000,
    dry_run: bool = False,
    retire_documents: Callable[[list[str]], None] | None = None,
) -> int:
    cutoff = now() - timedelta(days=older_than_days)
    cond = (
        Item.updated_at < cutoff,
        Item.locked.is_(False),
        or_(Item.status.in_(statuses), Item.deleted_at.is_not(None)),
    )
    if dry_run:
        from sqlalchemy import func

        with db.session_scope() as session:
            return session.scalar(select(func.count()).select_from(Item).where(*cond)) or 0
    total = 0
    while True:
        with db.session_scope() as session:
            ids = list(
                session.scalars(
                    select(Item.display_id)
                    .where(*cond)
                    .order_by(Item.display_id)
                    .limit(batch_size)
                )
            )
            if not ids:
                break
            retire_traces: list[str] = []
            if retire_documents is not None:
                traces = set(session.scalars(select(Item.id).where(Item.display_id.in_(ids))))
                live = set(
                    session.scalars(
                        select(Item.id).where(Item.id.in_(traces), Item.display_id.not_in(ids))
                    )
                )
                retire_traces = sorted(traces - live)
        # Call Annotations API outside any DB transaction so connections never sit idle in tx.
        if retire_documents is not None and retire_traces:
            retire_documents(retire_traces)  # raises: keep the batch
        with db.session_scope() as session:
            res = session.execute(delete(Item).where(Item.display_id.in_(ids), *cond))
            deleted = res.rowcount if res.rowcount is not None and res.rowcount >= 0 else len(ids)
        total += deleted
        logger.info("Retired %d items (%d so far)", deleted, total)
        if deleted == 0:
            break
    return total


def annotations_retirer(settings) -> Callable[[list[str]], None] | None:
    """Calls the Annotations API to delete the documents of items being retired (None when no
    Annotations API is configured)."""
    if not settings.annotations_api_url:
        return None
    url = f"{settings.annotations_api_url.rstrip('/')}/admin/retire_work_items"
    headers = (
        {"Ocp-Apim-Subscription-Key": settings.annotations_api_key}
        if settings.annotations_api_key
        else {}
    )

    def retire(work_item_ids: list[str]) -> None:
        r = httpx.post(url, json={"work_item_ids": work_item_ids}, headers=headers, timeout=300)
        if r.status_code != 200:
            raise RuntimeError(
                "Annotations API could not retire documents"
                f" (HTTP {r.status_code}): {r.text[:300]}"
            )
        try:
            body = r.json()
        except ValueError:
            body = {}
        if isinstance(body, dict) and body.get("packages_skipped"):
            raise RuntimeError(
                f"Annotations API skipped {len(body['packages_skipped'])} processing package(s)"
            )

    return retire


def main() -> None:  # pragma: no cover - CLI
    from app.config import get_settings

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = get_settings()
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--days", type=int, default=settings.retention_days)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    db.init_db(settings.database_url, direct_url=settings.direct_database_url)
    n = retire_items(
        older_than_days=args.days,
        statuses=settings.retire_statuses,
        batch_size=settings.retire_batch_size,
        dry_run=args.dry_run,
        retire_documents=annotations_retirer(settings),
    )
    print(f"{'Would retire' if args.dry_run else 'Retired'} {n} items older than {args.days} days")


if __name__ == "__main__":  # pragma: no cover
    main()
