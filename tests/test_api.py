"""Work Item Manager API: Roots Queue compatibility and UI endpoints."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from tests.conftest import add


def bot_status(client, item, status, **extra):
    """What a bot does: lock the item, then report its status."""
    assert client.put(f"/items/{item}/lock").status_code == 200
    r = client.put(f"/items/{item}/status", json={"status": status, **extra})
    assert r.status_code == 200, r.text
    return r


NEXT_KEYS = {"itemId", "rootsId", "data", "status", "attempts", "lockedStatus", "previousStatus"}
QUEUE_KEYS = {
    "id",
    "name",
    "maxAttempts",
    "rootsId",
    "createdAt",
    "updatedAt",
    "deletedAt",
    "organization",
}


def test_queues_strict_shape_and_trailing_slashes(client, queue):
    assert {"id": queue, "name": "Test Queue"} in client.get("/queues/").json()
    for path in (f"/queues/{queue}", f"/queues/{queue}/"):
        body = client.get(path).json()
        assert set(body) == QUEUE_KEYS
        assert body["maxAttempts"] == 3
    r = client.put(f"/queues/{queue}/update", json={"maxAttempts": 5, "rootsId": "Claim Number"})
    assert r.json()["maxAttempts"] == 5 and r.json()["rootsId"] == "Claim Number"
    assert client.post("/queues", json={"queueName": "Test Queue"}).status_code == 400


def test_add_items_order_roots_id_and_uniqueness(client, queue):
    ids = add(client, queue, [{"KEY": "a.pdf", "x": 1}, {"KEY": "b.pdf"}, {"KEY": "a.pdf"}])
    assert len(ids) == 3 and all(len(i) == 36 for i in ids)  # dashed uuids
    info = client.get(f"/items/{ids[0]}/").json()
    assert info["displayRootsId"] == "a.pdf" and info["status"] == "queued"
    assert info["data"] == {"KEY": "a.pdf", "x": 1}

    unique = {"includePendingItems": True}
    r = client.post(
        "/items/",
        json={
            "queueId": queue,
            "items": [{"KEY": "a.pdf"}, {"KEY": "c.pdf"}, {"KEY": "c.pdf"}],
            "enforceUniqueRootsId": unique,
        },
    )
    assert r.status_code == 200
    assert len(r.json()["itemIds"]) == 1  # a.pdf pending already, c.pdf once
    assert r.json()["skippedRootsIds"] == ["a.pdf", "c.pdf"]


def test_get_next_item_contract(client, queue):
    assert client.post("/items/next", json={"queueId": queue}).status_code == 204
    low, high = add(client, queue, [{"KEY": "low"}], priority=90)[0], None
    high = add(client, queue, [{"KEY": "high"}], priority=10)[0]
    r = client.post("/items/next", json={"queueId": queue, "tagsInclude": [], "tagsExclude": []})
    assert r.status_code == 200
    body = r.json()
    assert set(body) == NEXT_KEYS  # an extra key breaks Item(**camel_to_snake(json))
    assert body["itemId"] == high and body["status"] == "in_progress"
    assert body["lockedStatus"] is True and body["previousStatus"] == "queued"
    assert body["attempts"] == 1
    # with itemId: only that item
    assert client.post("/items/next", json={"queueId": queue, "itemId": high}).status_code == 204
    r = client.post("/items/next", json={"queueId": queue, "itemId": low.replace("-", "")})
    assert r.json()["itemId"] == low


def test_tags_filter_next(client, queue):
    a = add(client, queue, [{"KEY": "a"}], tags=["blue"])[0]
    b = add(client, queue, [{"KEY": "b"}], tags=["red"])[0]
    r = client.post("/items/next", json={"queueId": queue, "tagsExclude": ["blue"]})
    assert r.json()["itemId"] == b
    r = client.post("/items/next", json={"queueId": queue, "tagsInclude": ["blue"]})
    assert r.json()["itemId"] == a


def test_lock_and_unlock_rules(client, queue):
    item = add(client, queue, [{"KEY": "k"}])[0]
    assert client.put(f"/items/{item}/lock").json()["status"] == "in_progress"
    assert "already locked" in client.put(f"/items/{item}/lock").json()["message"]
    other = client.put(f"/items/{item}/lock", headers={"Resource": "bot2"})
    assert other.status_code == 400 and "currently locked" in other.text
    # unlock is for stuck items: any caller may release it, and it always goes back to queued
    r = client.put(f"/items/{item}/unlock", headers={"Resource": "ops"})
    assert r.status_code == 200 and r.json()["status"] == "queued"
    info = client.get(f"/items/{item}").json()
    assert info["locked"] is False and info["lockingResource"] is None
    assert client.put(f"/items/{item}/unlock").status_code == 400  # not locked
    events = client.get(f"/items/{item}/events").json()
    assert events[0]["event"] == "unlocked" and "lock held by default" in events[0]["message"]


def test_only_queued_or_deferred_can_be_claimed(client, queue):
    item = add(client, queue, [{"KEY": "k"}])[0]
    bot_status(client, item, "requires_review", exceptionMessage="look")
    assert client.put(f"/items/{item}/lock").status_code == 400
    assert client.post("/items/next", json={"queueId": queue, "itemId": item}).status_code == 204
    assert client.post("/items/next", json={"queueId": queue}).status_code == 204
    d = add(client, queue, [{"KEY": "d"}])[0]
    bot_status(client, d, "deferred")  # due now
    r = client.post("/items/next", json={"queueId": queue})
    assert r.json()["itemId"] == d and r.json()["previousStatus"] == "deferred"


def test_status_needs_the_lock(client, queue):
    item = add(client, queue, [{"KEY": "k"}])[0]
    r = client.put(f"/items/{item}/status", json={"status": "automated"})
    assert r.status_code == 409 and "locked in in_progress" in r.text
    client.put(f"/items/{item}/lock")
    r = client.put(f"/items/{item}/status", json={"status": "in_progress"})
    assert r.status_code == 400
    r = client.put(f"/items/{item}/status", json={"status": "automated"})
    assert r.status_code == 200
    info = client.get(f"/items/{item}").json()
    assert info["status"] == "automated" and info["locked"] is False and info["completedAt"]
    assert client.put(f"/items/{item}/status", json={"status": "queued"}).status_code == 409


def test_force_retry_adds_a_new_item_with_the_same_trace_id(client, queue):
    item = add(client, queue, [{"KEY": "k"}], tags=["t"])[0]
    client.post(
        f"/items/{item}/file",
        json={"files": [{"fileName": "a.pdf", "filePath": "1", "packageId": 7}]},
    )
    first = client.get(f"/items/{item}").json()["incrementedDisplayId"]
    assert client.post(f"/items/{item}/force_retry").status_code == 400  # queued: not a failure
    bot_status(client, item, "system_exception", exceptionMessage="boom")
    info = client.get(f"/items/{item}").json()
    assert info["status"] == "system_exception" and info["lastExceptionType"] == "System_Exception"

    r = client.post(f"/items/{item}/force_retry").json()
    assert r["itemId"] == item and r["status"] == "queued"
    assert r["incrementedDisplayId"] > first and r["previousIncrementedDisplayId"] == first
    latest = client.get(f"/items/{item}").json()  # the trace id now means the retry
    assert latest["incrementedDisplayId"] == r["incrementedDisplayId"]
    assert latest["attempts"] == 1 and latest["tags"] == ["t"]
    assert client.get(f"/items/{item}/file").json()[0]["packageId"] == 7
    assert client.post(f"/items/{item}/force_retry").status_code == 400

    rows = client.post("/items/v2/searches", json={"queueId": queue}).json()["rows"]
    assert sorted((x["incremented_display_id"], x["status"]) for x in rows) == [
        (first, "system_exception"),
        (r["incrementedDisplayId"], "queued"),
    ]
    assert all(x["trace_id"] == item for x in rows)
    events = [e["event"] for e in client.get(f"/items/{item}/events").json()]
    assert events[0] == "created" and "force_retry" in events  # both attempts' history
    # the retry is claimable like any queued item
    assert client.post("/items/next", json={"queueId": queue}).json()["itemId"] == item


def test_status_variants_defer_and_auto_retry(client, queue):
    item = add(client, queue, [{"KEY": "k"}])[0]
    client.post("/items/next", json={"queueId": queue})
    until = (datetime.now(UTC) + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    r = client.put(f"/items/{item}/status", json={"status": "deferred", "deferUntil": until})
    assert r.json()["status"] == "deferred"
    info = client.get(f"/items/{item}").json()
    assert info["deferCount"] == 1 and info["nextReviewDate"]
    assert client.post("/items/next", json={"queueId": queue}).status_code == 204  # not due

    client.put(f"/items/{item}/lock")  # a deferred item may be locked by id
    r = client.put(
        f"/items/{item}/status", json={"status": "Requires Review", "exceptionMessage": "x"}
    )
    assert client.get(f"/items/{item}").json()["lastExceptionType"] == "Business_Exception"
    assert r.json()["status"] == "requires_review"
    assert client.put(f"/items/{item}/status", json={"status": "bogus"}).status_code == 422

    other = add(client, queue, [{"KEY": "o"}])[0]
    client.post("/items/next", json={"queueId": queue, "itemId": other})
    client.put(f"/items/{other}/status", json={"status": "system_exception", "auto_retry": True})
    retry = client.get(f"/items/{other}").json()
    assert retry["status"] == "queued" and retry["previousStatus"] == "system_exception"


def test_finished_item_has_no_stale_exception_message(client, queue):
    """Like a Digital Coworker: deferred while extraction runs, then automated."""
    item = add(client, queue, [{"KEY": "k"}])[0]
    bot_status(client, item, "deferred", exceptionMessage="Awaiting Platform extraction.")
    assert (
        client.get(f"/items/{item}").json()["exceptionReason"] == "Awaiting Platform extraction."
    )
    bot_status(client, item, "automated")
    info = client.get(f"/items/{item}").json()
    assert info["status"] == "automated" and info["exceptionReason"] is None
    events = client.get(f"/items/{item}/events").json()
    assert "Awaiting Platform extraction." in str(events)  # history is kept
    other = add(client, queue, [{"KEY": "o"}])[0]
    bot_status(client, other, "automated", exceptionMessage="done with notes")
    assert client.get(f"/items/{other}").json()["exceptionReason"] == "done with notes"


def test_manual_review_transitions(client, queue):
    item = add(client, queue, [{"KEY": "k"}])[0]
    bot_status(client, item, "requires_review", exceptionMessage="check")
    bad = client.put(f"/items/{item}/status", json={"status": "automated", "manual": True})
    assert bad.status_code == 409
    r = client.put(f"/items/{item}/status", json={"status": "queued", "manual": True})
    assert r.status_code == 200  # reviewed: back to the bots
    client.put(f"/items/{item}/lock")
    held = client.put(f"/items/{item}/status", json={"status": "manually_closed", "manual": True})
    assert held.status_code == 409  # a bot holds it
    client.put(f"/items/{item}/status", json={"status": "requires_review"})
    r = client.put(f"/items/{item}/status", json={"status": "manually_closed", "manual": True})
    assert r.json()["status"] == "manually_closed"


def _age_all_items(days: int) -> None:
    from app import db
    from app.models import Item

    with db.session_scope() as s:
        for row in s.query(Item):
            row.updated_at = datetime.now(UTC) - timedelta(days=days)


def test_retire_old_finished_items(client, queue, monkeypatch):
    done, open_ = add(client, queue, [{"KEY": "done"}, {"KEY": "open"}])
    bot_status(client, done, "automated")
    _age_all_items(200)
    retired_docs: list[list[str]] = []
    monkeypatch.setattr("app.retention.annotations_retirer", lambda settings: retired_docs.append)
    r = client.post("/admin/retire_items", json={"olderThanDays": 180, "dryRun": True})
    assert r.json()["count"] == 1 and retired_docs == []
    r = client.post("/admin/retire_items", json={"olderThanDays": 180})
    assert r.json()["count"] == 1
    # The item's documents were retired (Annotations API) before the item was deleted.
    assert retired_docs == [[done.replace("-", "")]]
    assert client.get(f"/items/{done}").status_code == 400
    assert client.get(f"/items/{open_}").json()["status"] == "queued"
    assert client.post("/admin/retire_items", json={"olderThanDays": 5}).status_code == 422


def test_retirement_keeps_items_when_documents_cannot_be_retired(client, queue):
    """ANNOTATIONS_API_URL points nowhere in tests: the batch is kept, nothing is lost."""
    done = add(client, queue, [{"KEY": "done"}])[0]
    bot_status(client, done, "automated")
    _age_all_items(200)
    r = client.post("/admin/retire_items", json={"olderThanDays": 180})
    assert r.status_code == 502 and "Retention stopped" in r.json()["detail"]
    assert client.get(f"/items/{done}").json()["status"] == "automated"


def test_retirement_skips_trace_ids_still_live(client, queue, monkeypatch):
    """force_retry keeps the failed attempt (same trace id): retiring the old row must not
    delete the documents the live retry still uses."""
    item = add(client, queue, [{"KEY": "k"}])[0]
    bot_status(client, item, "system_exception", exceptionMessage="boom")
    client.post(f"/items/{item}/force_retry")
    _age_all_items(200)
    retired_docs: list[list[str]] = []
    monkeypatch.setattr("app.retention.annotations_retirer", lambda settings: retired_docs.append)
    r = client.post(
        "/admin/retire_items", json={"olderThanDays": 180, "statuses": ["system_exception"]}
    )
    assert r.json()["count"] == 1 and retired_docs == []  # the retry (queued) still uses them
    assert client.get(f"/items/{item}").json()["status"] == "queued"


def test_missing_item_is_400_cannot_be_found(client):
    r = client.get("/items/0123456789abcdef0123456789abcdef/")
    assert r.status_code == 400 and "cannot be found" in r.text


def test_search_v1_status_casing_and_paging(client, queue):
    ids = add(client, queue, [{"KEY": f"k{i}"} for i in range(3)])
    bot_status(client, ids[0], "requires_review")
    r = client.post("/items/searches", json={"queueId": queue, "statuses": ["Requires Review"]})
    assert r.json()["item_ids"] == [ids[0]]
    r = client.post("/items/searches", json={"queueId": queue, "statuses": ["Queued"], "page": 1})
    assert set(r.json()["item_ids"]) == set(ids[1:])
    start = (datetime.now(UTC) - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
    r = client.post("/items/searches", json={"queueId": queue, "updatedAtStartDatetime": start})
    assert len(r.json()["item_ids"]) == 3


def test_search_v2_rows_filters_sort_export(client, queue):
    ids = add(client, queue, [{"KEY": f"Files_{i}_ACME"} for i in range(5)])
    u = client.post("/users/", json={"name": "Avery Chen", "email": "a@example.com"}).json()
    client.put(
        "/items/assign",
        json={"items": [{"work_item_id": ids[1], "assignee_id": u["id"], "assigned_by": "me"}]},
    )
    bot_status(client, ids[2], "requires_review", exceptionMessage="x")
    client.post(
        f"/items/{ids[1]}/file",
        json={"files": [{"fileName": "a.pdf", "filePath": "12", "packageId": 12}]},
    )

    r = client.post(
        "/items/v2/searches",
        json={
            "queueId": queue,
            "startRow": 0,
            "endRow": 2,
            "sortModel": [{"colId": "incremented_display_id", "sort": "asc"}],
        },
    ).json()
    assert r["metadata"]["total_item_count"] == 5 and len(r["rows"]) == 2
    assert r["columns"][0]["field"] == "display_roots_id"
    row = r["rows"][1]
    assert row["trace_id"] == ids[1] and row["assignee_name"] == "Avery Chen"
    assert row["file_count"] == 1 and row["package_details"][0]["package_id"] == 12

    def rows(**f):
        return client.post("/items/v2/searches", json={"queueId": queue, **f}).json()["rows"]

    assert [x["trace_id"] for x in rows(statuses=["requires_review"])] == [ids[2]]
    assert [x["trace_id"] for x in rows(assigneeIds=[u["id"]])] == [ids[1]]
    assert len(rows(assigneeIds=[None])) == 4
    assert [x["trace_id"] for x in rows(search=ids[3][:8])] == [ids[3]]
    assert len(rows(search="ACME")) == 5
    disp = rows()[0]["incremented_display_id"]
    assert rows(search=str(disp))[0]["incremented_display_id"] == disp
    today = datetime.now(UTC).date().isoformat()
    assert len(rows(updated_at={"dateFrom": today, "filterType": "date", "type": "inRange"})) == 5

    csv = client.post("/items/v2/searches", json={"queueId": queue, "export": True})
    assert csv.headers["content-type"].startswith("text/csv")
    assert csv.text.splitlines()[0].startswith("Client ID,Item ID,Trace ID,Status")
    assert len(csv.text.strip().splitlines()) == 6

    counts = client.post("/queues/count_by_status", json={"queueId": queue}).json()
    assert counts["queued"] == 4 and counts["requires_review"] == 1 and counts["total"] == 5


def test_tags_notes_reporting_form_files(client, queue):
    item = add(client, queue, [{"KEY": "k"}])[0]
    client.put(f"/items/{item}/tag", json={"tags": ["retrying", "x", "retrying"]})
    assert client.get(f"/items/{item}/tag").json() == ["retrying", "x"]
    client.request("DELETE", f"/items/{item}/tag", json={"tags": ["x"]})
    assert client.get(f"/items/{item}/tag").json() == ["retrying"]

    client.post(f"/items/{item}/notes", json={"notes": "first"})
    client.put(f"/items/{item}/notes", json={"notes": "second"})
    assert client.get(f"/items/{item}").json()["notes"] == "first second"
    client.delete(f"/items/{item}/notes")
    assert client.get(f"/items/{item}").json()["notes"] == ""

    client.post(f"/items/{item}/reporting_json", json={"reportingJson": {"a": 1}})
    client.put(f"/items/{item}/reporting_json", json={"key": "b", "value": 2})
    # returned as a string, like the platform (Python dict repr)
    assert client.get(f"/items/{item}").json()["reportingJson"] == "{'a': 1, 'b': 2}"

    r = client.post(f"/form/{item}/", json={"formElements": ["enc1", "enc2"]})
    fe_ids = r.json()["formElementIds"]
    client.put(f"/form_element/{fe_ids[0]}/", json={"form_element": "enc1b"})
    client.delete(f"/form_element/{fe_ids[1]}/")
    els = client.get(f"/form/{item}/").json()["formElements"]
    assert [e["encrypted_form_element"] for e in els] == ["enc1b"]

    other = add(client, queue, [{"KEY": "o"}])[0]
    fid = client.post(
        f"/items/{item}/file", json={"files": [{"filePath": "p", "fileName": "n"}]}
    ).json()["fileIds"][0]
    assert client.put(f"/files/{fid}/move", json={"itemId": other}).status_code == 200
    assert client.get(f"/items/{item}/file").json() == []
    assert client.get(f"/items/{other}/file").json()[0]["fileName"] == "n"
    docs = client.post("/docs/searches", json={"queueId": queue}).json()
    assert docs["metadata"]["total_document_count"] == 1
    client.delete(f"/files/{fid}/delete")
    assert client.get(f"/items/{other}/file").json() == []

    client.post(f"/items/{item}/data", json={"data": {"KEY": "k", "new": True}})
    client.put(f"/items/{item}/", json={"displayRootsId": "Display", "priority": 5})
    info = client.get(f"/items/{item}").json()
    assert info["data"]["new"] is True and info["displayRootsId"] == "Display"
    assert info["priority"] == 5

    assert client.delete(f"/items/{item}/delete").status_code == 200
    assert client.get(f"/items/{item}").status_code == 400


def test_clean_up(client, queue):
    a, b = add(client, queue, [{"KEY": "a"}, {"KEY": "b"}])
    client.post("/items/next", json={"queueId": queue, "itemId": a}, headers={"Resource": "bot"})
    assert client.put("/items/clean_up", headers={"Resource": "bot"}).json()["count"] == 1
    assert client.get(f"/items/{a}").json()["status"] == "system_exception"

    assert client.get(f"/items/{b}").json()["status"] == "queued"


def test_no_roots_cockpit(client, queue):
    """Roots Cockpit is not used: RootsQueueClient's Cockpit fields are accepted and ignored,
    nothing about Cockpit is stored or returned, and the Cockpit postback routes are gone."""
    r = client.post(
        "/items/",
        json={
            "queueId": queue,
            "items": [{"KEY": "c1"}],
            "sendToRootsCockpit": True,
            "processId": 7,
            "processSubtypeId": 8,
        },
    )
    assert r.status_code == 200, r.text
    item = r.json()["itemIds"][0]
    r = client.put(f"/items/{item}/", json={"cockpitId": 5, "processId": 9, "priority": 4})
    assert r.status_code == 200 and r.json()["priority"] == 4
    for body in (client.get(f"/items/{item}").json(), client.get(f"/queues/{queue}").json()):
        assert not [k for k in body if "cockpit" in k.lower() or "process" in k.lower()]
    for path in ("/cockpit/postback", "/cockpit/postback_priority"):
        assert client.post(path, content="<x/>").status_code == 404


def test_api_key(client, queue, monkeypatch):
    from app.config import get_settings

    monkeypatch.setenv("API_KEY", "s3cret")
    get_settings.cache_clear()
    assert client.get("/queues/").status_code == 401
    assert (
        client.get("/queues/", headers={"Ocp-Apim-Subscription-Key": "s3cret"}).status_code == 200
    )
    assert client.get("/queues/", headers={"X-API-Key": "s3cret"}).status_code == 200
    assert client.get("/health/live").status_code == 200


def test_health_degrades_without_annotations(client):
    body = client.get("/health").json()
    assert body["status"] == "degraded"
    assert body["checks"]["database"]["status"] == "ok"
    assert client.get("/health_check/").status_code == 200


def test_demo_queues_seeded_in_every_environment(client):
    queues = {q["name"]: q for q in client.get("/queues/?details=true").json()}
    mc, lr = queues["Demo - Mortgage Change"], queues["Demo - Loss Runs"]
    assert mc["organization"] == "Demo" and lr["organization"] == "Demo"
    assert mc["projectId"] == "de300000-0000-4000-8000-0000000000a1"
    assert lr["projectId"] == "de300000-0000-4000-8000-0000000000a2"


def test_search_pages_hold_at_most_1000_items(client, queue):
    add(client, queue, [{"KEY": f"k{i}"} for i in range(1005)])
    r = client.post(
        "/items/v2/searches", json={"queueId": queue, "startRow": 0, "endRow": 5000}
    ).json()
    assert len(r["rows"]) == 1000 and r["metadata"]["total_item_count"] == 1005
    r = client.post(
        "/items/v2/searches", json={"queueId": queue, "startRow": 1000, "endRow": 3000}
    ).json()
    assert len(r["rows"]) == 5
    r = client.post("/items/searches", json={"queueId": queue}).json()
    assert len(r["item_ids"]) == 1000 and r["pages"] == 2
    r = client.post("/items/ui_get_items", json={"queueId": queue, "itemsCount": 5000}).json()
    assert len(r["items"]) == 1000


def test_status_set_and_old_names(client, queue):
    item = add(client, queue, [{"KEY": "k"}])[0]
    client.put(f"/items/{item}/lock")
    r = client.put(f"/items/{item}/status", json={"status": "Completed"})
    assert r.json()["status"] == "automated"  # older name for a finished item
    other = add(client, queue, [{"KEY": "o"}])[0]
    client.put(f"/items/{other}/lock")
    assert (
        client.put(f"/items/{other}/status", json={"status": "Retrying"}).json()["status"]
        == "queued"
    )
    client.put(f"/items/{other}/lock")
    assert (
        client.put(f"/items/{other}/status", json={"status": "Manually Closed"}).json()["status"]
        == "manually_closed"
    )
    bad = client.put(f"/items/{item}/status", json={"status": "bogus"})
    assert bad.status_code == 422
    for s in (
        "queued",
        "in_progress",
        "deferred",
        "requires_review",
        "automated",
        "system_exception",
        "manually_closed",
    ):
        assert s in bad.text


def test_database_rejects_in_progress_without_lock(client, queue):
    import pytest
    from sqlalchemy.exc import IntegrityError

    from app import db
    from app.models import Item

    add(client, queue, [{"KEY": "k"}])
    with pytest.raises(IntegrityError):
        with db.session_scope() as s:
            row = s.query(Item).one()
            row.status = "in_progress"  # locked stays False: what v1 ended up with
    with db.session_scope() as s:
        assert s.query(Item).one().status == "queued"


def test_v1_double_encoded_body(client, queue):
    """The v1 UI sends bodies as a JSON string holding the JSON object."""
    add(client, queue, [{"KEY": "a"}])
    body = json.dumps(json.dumps({"search": "", "queueId": queue, "startRow": 0, "endRow": 10}))
    r = client.post(
        "/items/v2/searches", content=body, headers={"Content-Type": "application/json"}
    )
    assert r.status_code == 200, r.text
    assert r.json()["metadata"]["total_item_count"] == 1


def test_platform_form_like_v1(client, queue):
    """add_item_form_elements_platform stores the item's form; GET /items/{id}/form is what the
    work item details screen shows (label: value), in v1's shape."""
    item = add(client, queue, [{"KEY": "k"}])[0]
    form = {
        "form": {
            "form_elements": [
                {
                    "label": "Number Of Rows",
                    "value": "203",
                    "type": "STRING",
                    "permission": "ReadOnly",
                    "sort_order": 20,
                },
                {
                    "label": "Document Type",
                    "value": "Mortgage Change",
                    "type": "string",
                    "permission": "readonly",
                    "sort_order": 10,
                },
            ]
        }
    }
    assert client.post(f"/form/{item}/", json=form).status_code == 200
    els = client.get(f"/items/{item}/form").json()["form"]["form_elements"]
    assert [(e["label"], e["value"]) for e in els] == [
        ("Document Type", "Mortgage Change"),
        ("Number Of Rows", "203"),
    ]
    assert set(els[0]) == {
        "id",
        "label",
        "value",
        "type",
        "permission",
        "options",
        "info",
        "validation",
    }
    # A rerun replaces the form instead of adding the rows again.
    form["form"]["form_elements"][0]["value"] = "204"
    client.post(f"/form/{item}/", json=form)
    els = client.get(f"/items/{item}/form").json()["form"]["form_elements"]
    assert [e["value"] for e in els] == ["Mortgage Change", "204"]
    bad = client.post(f"/form/{item}/", json={"form": {"form_elements": ["x"]}})
    assert bad.status_code == 422


def test_queue_belongs_to_a_tenant_not_a_tree(client):
    tenant = "AAAAAAAA-0000-4000-8000-000000000001"  # the v1 UI sends uppercase ids
    r = client.post("/queues", json={"queueName": "Org Project", "tenantId": tenant})
    assert r.status_code == 200, r.text
    qid = r.json()["queueId"]
    assert r.json()["tenantId"] == tenant.lower() and r.json()["projectId"] is None
    client.post("/queues", json={"queueName": "Other Org Project", "tenantId": "bbbb"})

    mine = client.get("/queues", params={"tenant_id": tenant, "details": True}).json()
    assert [q["name"] for q in mine] == ["Org Project"]
    assert len(client.get("/queues").json()) >= 2  # unfiltered: every queue

    moved = client.put(f"/queues/{qid}/update", json={"tenantId": "bbbb"}).json()
    assert moved["tenantId"] == "bbbb"


def test_v1_docs_search_lists_the_queues_linked_packages(client, queue, monkeypatch):
    """v1 body {queueId, project_id, tenant_id, ...}: the packages linked to the queue's items go
    to the Annotations API, which returns the v1 rows."""
    import httpx

    first, second = add(client, queue, [{"KEY": "a"}, {"KEY": "b"}])
    for item, pkg in ((first, 21), (second, 22), (second, 21)):
        client.post(
            f"/items/{item}/file",
            json={"files": [{"fileName": f"{pkg}.pdf", "filePath": str(pkg), "packageId": pkg}]},
        )
    sent = {}

    def fake_post(url, json, headers, timeout):
        sent.update(url=url, body=json)
        return httpx.Response(200, json={"rows": [], "columns": [], "metadata": {}})

    monkeypatch.setattr(httpx, "post", fake_post)
    body = {"queueId": queue, "project_id": "p", "tenant_id": "t", "startRow": 0, "endRow": 5}
    r = client.post("/docs/searches", json=body)
    assert r.status_code == 200, r.text
    assert sent["url"].endswith("/docs/searches")
    assert sent["body"]["package_ids"] == [22, 21]  # newest first, once each
    assert sent["body"]["work_items"]["22"] == second.replace("-", "")
    assert sent["body"]["startRow"] == 0 and sent["body"]["endRow"] == 5


def test_v2_search_lists_every_v1_status(client, queue):
    add(client, queue, [{"KEY": "only-queued"}])
    st = client.post("/items/v2/searches", json={"queueId": queue}).json()["metadata"]["statuses"]
    assert set(st) >= {
        "queued",
        "in_progress",
        "automated",
        "requires_review",
        "system_exception",
        "deferred",
    }
    assert st["queued"] == 1 and st["deferred"] == 0
