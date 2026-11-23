"""Regression tests for bugs identified and fixed in Work Item Manager."""

from __future__ import annotations

from datetime import UTC, datetime

from tests.conftest import add


def test_v2_search_equals_date_and_null_strings_and_large_numeric_search(client, queue):
    add(client, queue, [{"KEY": "POL_1_3000000001"}])
    today = datetime.now(UTC).date().isoformat()

    # AG Grid equals date filter (must not crash on Postgres or SQLite)
    r = client.post(
        "/items/v2/searches",
        json={"queueId": queue, "created_at": {"type": "equals", "dateFrom": today}},
    )
    assert r.status_code == 200, r.text
    assert len(r.json()["rows"]) == 1

    # "null" date strings must be ignored instead of crashing or returning 0 rows
    r = client.post(
        "/items/v2/searches",
        json={"queueId": queue, "created_at_start_datetime": "null", "created_at": {"type": "equals", "dateFrom": "null"}},
    )
    assert r.status_code == 200, r.text
    assert len(r.json()["rows"]) == 1

    # Searching 32-bit+ numeric client IDs or Unicode digits must not 500
    r = client.post("/items/v2/searches", json={"queueId": queue, "search": "3000000001"})
    assert r.status_code == 200, r.text
    assert len(r.json()["rows"]) == 1
    r = client.post("/items/v2/searches", json={"queueId": queue, "search": "999999999999999999999"})
    assert r.status_code == 200, r.text
    r = client.post("/items/v2/searches", json={"queueId": queue, "search": "²"})
    assert r.status_code == 200, r.text


def test_like_wildcards_escaped_in_search(client, queue):
    add(client, queue, [{"KEY": "ABC_1"}, {"KEY": "ABCX1"}])
    r = client.post("/items/v2/searches", json={"queueId": queue, "search": "ABC_1"})
    assert r.status_code == 200
    assert [row["display_roots_id"] for row in r.json()["rows"]] == ["ABC_1"]


def test_csv_export_neutralizes_formula_injection(client, queue):
    add(client, queue, [{"KEY": "=CMD|' /C calc'!A0"}])
    r = client.post("/items/v2/searches", json={"queueId": queue, "export": True})
    assert r.status_code == 200
    assert "'=CMD|' /C calc'!A0" in r.text


def test_falsy_zero_pagination_and_retention_validation(client, queue):
    add(client, queue, [{"KEY": "K1"}, {"KEY": "K2"}])
    r = client.post("/items/v2/searches", json={"queueId": queue, "startRow": 0, "endRow": 0})
    assert r.status_code == 200
    assert r.json()["rows"] == []

    r = client.post("/admin/retire_items", json={"older_than_days": 0, "dry_run": True})
    assert r.status_code == 422


def test_queue_update_duplicate_name_and_form_null_sort_order(client, queue):
    r2 = client.post("/queues/", json={"queueName": "Other Queue"})
    q2 = r2.json()["queueId"]
    assert client.put(f"/queues/{q2}/update", json={"name": "Test Queue"}).status_code == 400

    iid = add(client, queue, [{"KEY": "FORM-1"}])[0]
    r = client.post(
        f"/form/{iid}",
        json={
            "form": {
                "form_elements": [
                    {"label": "A", "value": "1", "sort_order": None},
                    {"label": "B", "value": "2", "sort_order": 1},
                ]
            }
        },
    )
    assert r.status_code == 200, r.text
