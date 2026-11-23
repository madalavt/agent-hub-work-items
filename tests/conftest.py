from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{tmp_path}/wi.db")
    monkeypatch.setenv("SEED_DEMO_DATA", "false")
    monkeypatch.setenv("ANNOTATIONS_API_URL", "http://127.0.0.1:9/annotations")
    monkeypatch.delenv("API_KEY", raising=False)
    from app import db
    from app.config import get_settings

    get_settings.cache_clear()
    db._engine = None
    from app.main import app

    with TestClient(app) as c:
        c.headers.update({"Resource": "default"})
        yield c
    get_settings.cache_clear()


@pytest.fixture()
def queue(client) -> str:
    r = client.post("/queues/", json={"queueName": "Test Queue"})
    assert r.status_code == 200, r.text
    return r.json()["queueId"]


def add(client, queue, items, **extra) -> list[str]:
    r = client.post("/items/", json={"queueId": queue, "items": items, **extra})
    assert r.status_code == 200, r.text
    return r.json()["itemIds"]
