"""Seeds.

* ``seed_default_queues`` (every environment): the **Demo** organization's queues, linked to the
  Demo projects the decision-tree API seeds with fixed ids (services/api/seed/demo_*.json).
* ``seed_demo`` (SEED_DEMO_DATA=true, local only): two example queues, a few people and work
  items in every status. Applied once; names are invented.
"""

from __future__ import annotations

import logging
import random
from datetime import timedelta

from sqlalchemy import select

from app import models, service
from app.db import session_scope
from app.models import Item, ItemFile, Queue, User, now

logger = logging.getLogger(__name__)

USERS = [
    ("Avery Chen", "avery.chen@example.com"),
    ("Jordan Patel", "jordan.patel@example.com"),
    ("Morgan Ruiz", "morgan.ruiz@example.com"),
]

INSUREDS = [
    "Bluebird Bakery LLC",
    "Harbor Point Marine Inc",
    "Summit Ridge Builders",
    "Oak & Ember Restaurant Group",
    "Northwind Logistics Co",
    "Cedar Valley Dental PC",
    "Lumen Print Works",
    "Granite State Paving",
    "Riverside Animal Clinic",
    "Copperline Electric Inc",
    "Maple Street Florist",
    "Ironclad Fabrication LLC",
]

REVIEW_REASONS = [
    "Policy number not found in policy admin system",
    "Low confidence on claims table (3 rows below threshold)",
    "Loss run is older than 90 days",
]
SYSTEM_REASONS = [
    "Extraction timed out after 3 attempts",
    "Upstream API returned 503",
]


DEMO_ORGANIZATION = "Demo"
DEMO_TENANT_ID = "de300000-0000-4000-8000-000000000001"  # the Demo customer (services/api seed)
DEFAULT_QUEUES = [
    {
        "name": "Demo - Mortgage Change",
        "description": "Demo organization: submission packages classified and extracted by the "
        "Mortgage Change project (tables with page chunking).",
        "project_id": "de300000-0000-4000-8000-0000000000a1",
        "id": "de3000000000400080000000000000c1",  # the Demo project's queue (api seed)
    },
    {
        "name": "Demo - Loss Runs",
        "description": "Demo organization: loss runs extracted by the Loss Runs project "
        "(policy and claims tables, verifiers, business rules).",
        "project_id": "de300000-0000-4000-8000-0000000000a2",
        "id": "de3000000000400080000000000000c2",
    },
]


def seed_default_queues() -> None:
    """Create the Demo queues if missing; never overwrite settings someone changed."""
    with session_scope() as session:
        for spec in DEFAULT_QUEUES:
            q = session.scalar(
                select(Queue).where(Queue.name == spec["name"], Queue.deleted_at.is_(None))
            )
            if q is None:
                taken = session.get(Queue, spec["id"]) is not None
                session.add(
                    Queue(
                        **({} if taken else {"id": spec["id"]}),
                        name=spec["name"],
                        description=spec["description"],
                        project_id=spec["project_id"],
                        organization=DEMO_ORGANIZATION,
                        tenant_id=DEMO_TENANT_ID,
                        max_attempts=3,
                        roots_id="KEY",
                    )
                )
                logger.info("seeded queue %s", spec["name"])
            else:
                q.project_id = q.project_id or spec["project_id"]
                q.tenant_id = q.tenant_id or DEMO_TENANT_ID


# Demo queues and the decision-tree projects they feed (fixed ids in services/api/seed). A queue
# must be linked to a project for anyone but an admin to see it.
DEMO_QUEUE_PROJECTS = {
    "Selective - Loss Runs - Prod": "40402aad-7849-4425-b189-c0c96eefab2c",
    "Frontline - Mortgage Change": "0a25aade-a622-4224-874e-d71d440941e8",
}


def seed_demo() -> None:
    with session_scope() as session:
        existing = {
            q.name: q
            for q in session.scalars(select(Queue).where(Queue.name.in_(DEMO_QUEUE_PROJECTS)))
        }
        if existing:
            for name, q in existing.items():  # databases seeded before the link existed
                if not q.project_id:
                    q.project_id = DEMO_QUEUE_PROJECTS[name]
            return
        users = [User(name=n, email=e) for n, e in USERS]
        session.add_all(users)
        loss_runs = Queue(
            name="Selective - Loss Runs - Prod",
            description="Loss run PDFs from the intake mailbox; extraction + verification.",
            project_id=DEMO_QUEUE_PROJECTS["Selective - Loss Runs - Prod"],
            max_attempts=3,
            roots_id="KEY",
        )
        mortgage = Queue(
            name="Frontline - Mortgage Change",
            description="Mortgagee change requests classified and posted to the policy system.",
            project_id=DEMO_QUEUE_PROJECTS["Frontline - Mortgage Change"],
            max_attempts=3,
            roots_id="KEY",
        )
        session.add_all([loss_runs, mortgage])
        session.flush()

        rng = random.Random(7)
        base = now() - timedelta(days=4)
        latest = now() - timedelta(minutes=5)
        plan = (
            [models.AUTOMATED] * 22
            + [models.REQUIRES_REVIEW] * 5
            + [models.SYSTEM_EXCEPTION] * 2
            + [models.QUEUED] * 3
            + [models.DEFERRED, models.MANUALLY_CLOSED, models.QUEUED]
        )
        rng.shuffle(plan)
        for queue, prefix in ((loss_runs, "Files"), (mortgage, "MC")):
            for n, status in enumerate(plan if queue is loss_runs else plan[:14]):
                created = min(base + timedelta(minutes=rng.randint(0, 5500)), latest)
                insured = rng.choice(INSUREDS)
                stamp = created.strftime("%Y%m%d%H%M%S%f")[:17]
                key = f"{prefix}_{rng.randint(1, 9)}_{stamp}_{insured}"
                [item], _ = service.add_items(
                    session,
                    queue,
                    [{"KEY": key, "file_name": f"{insured}.pdf", "insured": insured}],
                    actor="demo-seed",
                    unique=None,
                )
                item.created_at = created
                item.updated_at = min(created + timedelta(minutes=rng.randint(2, 400)), latest)
                if item.updated_at < created:
                    item.created_at = created = item.updated_at - timedelta(minutes=3)
                item.attempts = 0 if status == models.QUEUED else 1
                if status != models.QUEUED:
                    item.previous_status = models.IN_PROGRESS
                    item.status = status
                    item.work_time_seconds = round(rng.uniform(20, 240), 2)
                if status == models.REQUIRES_REVIEW:
                    item.exception_reason = rng.choice(REVIEW_REASONS)
                    item.last_exception_type = "Business_Exception"
                    item.exception_date = item.updated_at
                    if n % 2 == 0:
                        item.assignee_id = users[n % len(users)].id
                        item.assigned_by = "demo-seed"
                elif status == models.SYSTEM_EXCEPTION:
                    item.exception_reason = rng.choice(SYSTEM_REASONS)
                    item.last_exception_type = "System_Exception"
                    item.exception_date = item.updated_at
                elif status == models.DEFERRED:
                    item.next_review_date = now() + timedelta(hours=6)
                    item.defer_count = 1
                elif status in models.COMPLETED_STATUSES:
                    item.completed_at = item.updated_at
                item.reporting_json = {"Insured": insured, "Document Type": "Loss Runs"}
                session.add(
                    ItemFile(
                        item_pk=item.display_id,
                        file_name=f"{insured}.pdf",
                        file_path=f"intake/{stamp}/{insured}.pdf",
                        created_at=created,
                    )
                )
        n_items = session.query(Item).count()
    logger.info("seeded demo work items: 2 queues, %d items", n_items)
